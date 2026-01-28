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
}

func (s *server) ExecuteStageRun(ctx context.Context, req *pb.ExecuteStageRunRequest) (*pb.ExecuteStageRunResponse, error) {
	log.Printf("Received stage run request: stage_id=%s, stage_run_id=%s", req.StageId, req.StageRunId)

	// For now, assume it needs pipe-node-worker-agent
	// TODO: Determine runner type based on stage configuration

	// Prepare input JSON for pipe-node-worker-agent
	// The bootstrap.sh expects: { code, context, stageConfig }
	inputJSON := map[string]interface{}{
		"code":        req.Code,        // Stage code blob
		"context":     req.Context,     // Stage context JSON
		"stageConfig": req.StageConfig, // Stage configuration (nodeVersion, dependencies, etc.)
	}

	inputBytes, err := json.Marshal(inputJSON)
	if err != nil {
		log.Printf("Error marshaling input JSON: %v", err)
		return &pb.ExecuteStageRunResponse{
			Status: pb.Status_FAILED,
			Error:  err.Error(),
		}, nil
	}

	// Create container configuration
	containerConfig := &container.Config{
		Image: s.nodeImage,
		Env: []string{
			// WORK_DIR and CONTEXT will be set by bootstrap.sh from stdin
		},
		AttachStdin:  true,
		AttachStdout: true,
		AttachStderr: true,
		OpenStdin:    true,
		StdinOnce:    true,
	}

	hostConfig := &container.HostConfig{
		AutoRemove: true,
		NetworkMode: "torv_pipe_worker_network",
	}

	// Create container
	createResp, err := s.dockerClient.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, "")
	if err != nil {
		log.Printf("Error creating container: %v", err)
		return &pb.ExecuteStageRunResponse{
			Status: pb.Status_FAILED,
			Error:  err.Error(),
		}, nil
	}

	containerID := createResp.ID
	log.Printf("Created container: %s", containerID)

	// Attach to container before starting to capture stdout/stderr
	attachResp, err := s.dockerClient.ContainerAttach(ctx, containerID, types.ContainerAttachOptions{
		Stream: true,
		Stdin:  true,
		Stdout: true,
		Stderr: true,
	})
	if err != nil {
		log.Printf("Error attaching to container: %v", err)
		return &pb.ExecuteStageRunResponse{
			Status: pb.Status_FAILED,
			Error:  err.Error(),
		}, nil
	}
	defer attachResp.Close()

	// Start container
	if err := s.dockerClient.ContainerStart(ctx, containerID, types.ContainerStartOptions{}); err != nil {
		log.Printf("Error starting container: %v", err)
		return &pb.ExecuteStageRunResponse{
			Status: pb.Status_FAILED,
			Error:  err.Error(),
		}, nil
	}

	// Send input JSON to container stdin (with newline for jq to parse)
	inputWithNewline := append(inputBytes, '\n')
	if _, err := attachResp.Conn.Write(inputWithNewline); err != nil {
		log.Printf("Error writing to container stdin: %v", err)
		return &pb.ExecuteStageRunResponse{
			Status: pb.Status_FAILED,
			Error:  err.Error(),
		}, nil
	}
	attachResp.Conn.Close()

	// Read output from container (stdout only, stderr is mixed)
	output, err := io.ReadAll(attachResp.Reader)
	if err != nil && err != io.EOF {
		log.Printf("Error reading container output: %v", err)
		return &pb.ExecuteStageRunResponse{
			Status: pb.Status_FAILED,
			Error:  err.Error(),
		}, nil
	}

	// Wait for container to finish
	waitResp, errCh := s.dockerClient.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			log.Printf("Error waiting for container: %v", err)
			return &pb.ExecuteStageRunResponse{
				Status: pb.Status_FAILED,
				Error:  err.Error(),
			}, nil
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

	// Check if execution was successful
	success, ok := result["success"].(bool)
	if !ok {
		success = false
	}

	status := pb.Status_EXECUTED
	if !success {
		status = pb.Status_FAILED
	}

	errorMsg := ""
	if errStr, ok := result["error"].(string); ok && errStr != "" {
		errorMsg = errStr
	}

	log.Printf("Container execution completed: success=%v, error=%s", success, errorMsg)

	return &pb.ExecuteStageRunResponse{
		Status: status,
		Error:  errorMsg,
		Outputs: func() map[string]string {
			if outputs, ok := result["outputs"].(map[string]interface{}); ok {
				outputMap := make(map[string]string)
				for k, v := range outputs {
					if vStr, ok := v.(string); ok {
						outputMap[k] = vStr
					} else {
						// Convert to JSON string if not already a string
						if vBytes, err := json.Marshal(v); err == nil {
							outputMap[k] = string(vBytes)
						}
					}
				}
				return outputMap
			}
			return nil
		}(),
	}, nil
}

func main() {
	// Initialize Docker client
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("Failed to create Docker client: %v", err)
	}

	// Get node worker agent image from environment or use default
	nodeImage := os.Getenv("NODE_WORKER_AGENT_IMAGE")
	if nodeImage == "" {
		// Default to GHCR image
		nodeImage = "ghcr.io/torv-io/pipe-node-worker-agent:latest"
		log.Printf("Warning: NODE_WORKER_AGENT_IMAGE not set, using default: %s", nodeImage)
	}

	// Pull the image to ensure it's available
	log.Printf("Pulling image: %s", nodeImage)
	reader, err := dockerClient.ImagePull(context.Background(), nodeImage, types.ImagePullOptions{})
	if err != nil {
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
	})
	reflection.Register(s)

	log.Printf("gRPC server listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
