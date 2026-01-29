package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	pb "torv.io/worker-agent/proto"
)

type server struct {
	pb.UnimplementedWorkerServiceServer
	dockerClient *client.Client
	nodeImage    string
	networkName  string
}

func errorResponse(err error) *pb.ExecuteStageRunResponse {
	return &pb.ExecuteStageRunResponse{
		Status: pb.Status_FAILED,
		Error:  err.Error(),
	}
}

func (s *server) ExecuteStageRun(ctx context.Context, req *pb.ExecuteStageRunRequest) (*pb.ExecuteStageRunResponse, error) {
	log.Printf("Received stage run request: stage_id=%s, stage_run_id=%s", req.StageId, req.StageRunId)

	// Prepare input JSON for pipe-node-worker-agent
	// The bootstrap.sh expects: { code, context, stageConfig }
	inputJSON := map[string]interface{}{
		"code":        req.Code,
		"context":     req.Context,
		"stageConfig": req.StageConfig,
	}

	inputBytes, err := json.Marshal(inputJSON)
	if err != nil {
		log.Printf("Error marshaling input JSON: %v", err)
		return errorResponse(err), nil
	}

	containerConfig := &container.Config{
		Image: s.nodeImage,
		Env: []string{}, // WORK_DIR and CONTEXT will be set by bootstrap.sh from stdin
		AttachStdin:  true,
		AttachStdout: true,
		AttachStderr: true,
		OpenStdin:    true,
		StdinOnce:    true,
	}

	hostConfig := &container.HostConfig{
		AutoRemove: true,
		NetworkMode: s.networkName,
	}

	createResp, err := s.dockerClient.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, "")
	if err != nil {
		log.Printf("Error creating container: %v", err)
		return errorResponse(err), nil
	}

	containerID := createResp.ID
	log.Printf("Created container: %s", containerID)

	// Attach to container before starting to capture stdout/stderr
	attachResp, err := s.dockerClient.ContainerAttach(ctx, containerID, container.AttachOptions{
		Stream: true,
		Stdin:  true,
		Stdout: true,
		Stderr: true,
	})
	if err != nil {
		log.Printf("Error attaching to container: %v", err)
		return errorResponse(err), nil
	}
	defer attachResp.Close()

	if err := s.dockerClient.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		log.Printf("Error starting container: %v", err)
		return errorResponse(err), nil
	}

	// Send input JSON to container stdin (with newline for jq to parse)
	if _, err := attachResp.Conn.Write(append(inputBytes, '\n')); err != nil {
		log.Printf("Error writing to container stdin: %v", err)
		return errorResponse(err), nil
	}
	attachResp.Conn.Close()

	// Read output from container (stdout only, stderr is mixed)
	output, err := io.ReadAll(attachResp.Reader)
	if err != nil && err != io.EOF {
		log.Printf("Error reading container output: %v", err)
		return errorResponse(err), nil
	}

	// Wait for container to finish
	waitResp, errCh := s.dockerClient.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			log.Printf("Error waiting for container: %v", err)
			return errorResponse(err), nil
		}
	case <-waitResp:
	}

	// Parse output JSON
	var result map[string]interface{}
	if err := json.Unmarshal(output, &result); err != nil {
		log.Printf("Error parsing container output: %v, output: %s", err, string(output))
		return &pb.ExecuteStageRunResponse{
			Status: pb.Status_FAILED,
			Error:  "Failed to parse container output: " + err.Error(),
		}, nil
	}

	success, _ := result["success"].(bool)
	status := pb.Status_EXECUTED
	if !success {
		status = pb.Status_FAILED
	}

	errorMsg, _ := result["error"].(string)
	log.Printf("Container execution completed: success=%v, error=%s", success, errorMsg)

	// Convert outputs to map[string]string, marshaling non-string values to JSON
	outputs := make(map[string]string)
	if outputsMap, ok := result["outputs"].(map[string]interface{}); ok {
		for k, v := range outputsMap {
			if vStr, ok := v.(string); ok {
				outputs[k] = vStr
			} else if vBytes, err := json.Marshal(v); err == nil {
				outputs[k] = string(vBytes)
			}
		}
	}

	return &pb.ExecuteStageRunResponse{
		Status:  status,
		Error:   errorMsg,
		Outputs: outputs,
	}, nil
}

func main() {
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("Failed to create Docker client: %v", err)
	}

	// Get node worker agent image from environment or use default
	nodeImage := os.Getenv("NODE_WORKER_AGENT_IMAGE")
	if nodeImage == "" {
		nodeImage = "ghcr.io/torv-io/pipe-node-worker-agent:latest"
		log.Printf("Warning: NODE_WORKER_AGENT_IMAGE not set, using default: %s", nodeImage)
	}

	// Get network name from environment or detect it
	networkName := os.Getenv("WORKER_NETWORK_NAME")
	if networkName == "" {
		// Try to detect the network this container is running on
		ctx := context.Background()
		// Try multiple methods to find container name
		containerNames := []string{
			os.Getenv("HOSTNAME"),
			os.Getenv("CONTAINER_NAME"),
		}
		if hostname, _ := os.Hostname(); hostname != "" {
			containerNames = append(containerNames, hostname)
		}
		containerNames = append(containerNames, "pipe-worker-agent") // Fallback to known container name
		
		for _, containerName := range containerNames {
			if containerName == "" {
				continue
			}
			containerInfo, err := dockerClient.ContainerInspect(ctx, containerName)
			if err == nil && len(containerInfo.NetworkSettings.Networks) > 0 {
				// Use the first network found
				for netName := range containerInfo.NetworkSettings.Networks {
					networkName = netName
					log.Printf("Auto-detected network: %s (from container: %s)", networkName, containerName)
					break
				}
				if networkName != "" {
					break
				}
			}
		}
		if networkName == "" {
			networkName = "torv_pipe_worker_network"
			log.Printf("Warning: WORKER_NETWORK_NAME not set and could not auto-detect, using default: %s", networkName)
		}
	}

	// Pull the image to ensure it's available
	log.Printf("Pulling image: %s", nodeImage)
	if reader, err := dockerClient.ImagePull(context.Background(), nodeImage, types.ImagePullOptions{}); err != nil {
		log.Printf("Warning: Failed to pull image (may already exist): %v", err)
	} else {
		io.Copy(io.Discard, reader)
		reader.Close()
		log.Printf("Successfully pulled image: %s", nodeImage)
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterWorkerServiceServer(s, &server{
		dockerClient: dockerClient,
		nodeImage:    nodeImage,
		networkName:  networkName,
	})
	reflection.Register(s)

	log.Printf("gRPC server listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
