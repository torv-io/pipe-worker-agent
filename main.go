package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	pb "torv.io/worker-agent/proto"
)

type workerServer struct {
	pb.UnimplementedWorkerServiceServer
	dockerClient *client.Client
	nodeImage    string
}

func (s *workerServer) ExecuteStageRun(ctx context.Context, req *pb.ExecuteStageRunRequest) (*pb.ExecuteStageRunResponse, error) {
	log.Printf("[Worker] Executing stage run: %s", req.StageRunId)

	input, _ := json.Marshal(map[string]interface{}{
		"code":        req.Code,
		"context":     req.Context,
		"stageConfig": req.StageConfig,
	})

	config := &container.Config{
		Image: s.nodeImage,
		AttachStdin: true, AttachStdout: true, AttachStderr: true,
		OpenStdin: true, StdinOnce: true,
	}
	hostConfig := &container.HostConfig{AutoRemove: true, NetworkMode: "torv_pipe_worker_network"}

	resp, err := s.dockerClient.ContainerCreate(ctx, config, hostConfig, nil, nil, "")
	if err != nil {
		return &pb.ExecuteStageRunResponse{Status: pb.Status_FAILED, Error: err.Error()}, nil
	}

	attach, err := s.dockerClient.ContainerAttach(ctx, resp.ID, container.AttachOptions{Stream: true, Stdin: true, Stdout: true, Stderr: true})
	if err != nil {
		return &pb.ExecuteStageRunResponse{Status: pb.Status_FAILED, Error: err.Error()}, nil
	}
	defer attach.Close()

	s.dockerClient.ContainerStart(ctx, resp.ID, container.StartOptions{})
	attach.Conn.Write(append(input, '\n'))
	attach.Conn.Close()

	output, _ := io.ReadAll(attach.Reader)
	s.dockerClient.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)

	var res map[string]interface{}
	if err := json.Unmarshal(output, &res); err != nil {
		return &pb.ExecuteStageRunResponse{Status: pb.Status_FAILED, Error: "Invalid output"}, nil
	}

	outputs := make(map[string]string)
	if om, ok := res["outputs"].(map[string]interface{}); ok {
		for k, v := range om {
			if s, ok := v.(string); ok {
				outputs[k] = s
			} else {
				b, _ := json.Marshal(v)
				outputs[k] = string(b)
			}
		}
	}

	status := pb.Status_EXECUTED
	if success, _ := res["success"].(bool); !success {
		status = pb.Status_FAILED
	}

	errStr, _ := res["error"].(string)
	return &pb.ExecuteStageRunResponse{Status: status, Error: errStr, Outputs: outputs}, nil
}

func startHeartbeat(ctx context.Context, orchestratorURL, workerID string) {
	conn, err := grpc.NewClient(orchestratorURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[Heartbeat] Failed to connect: %v", err)
		return
	}
	defer conn.Close()

	client := pb.NewAgentServiceClient(conn)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	log.Printf("[Heartbeat] Starting for worker: %s", workerID)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{
				WorkerId: workerID,
				Status:   "online",
			})
			if err != nil {
				log.Printf("[Heartbeat] Error: %v", err)
			}
		}
	}
}

func ensureWorkerID(orchestratorURL string) string {
	workerID := os.Getenv("WORKER_ID")
	if workerID != "" {
		return workerID
	}

	// Try loading from persistent file
	idFile := "/data/worker_id"
	data, err := os.ReadFile(idFile)
	if err == nil && len(data) > 0 {
		id := string(data)
		log.Printf("[Worker] Loaded ID from storage: %s", id)
		return id
	}

	// Register with orchestrator using Peer Identification (IP/Port)
	log.Println("[Worker] No ID found. Registering with orchestrator...")
	conn, err := grpc.NewClient(orchestratorURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[Worker] Failed to connect to orchestrator for registration: %v", err)
	}
	defer conn.Close()

	client := pb.NewAgentServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Register(ctx, &pb.RegisterRequest{})
	if err != nil {
		log.Fatalf("[Worker] Registration RPC failed: %v", err)
	}
	if !resp.Success {
		log.Fatalf("[Worker] Registration rejected by orchestrator: %s", resp.Error)
	}

	workerID = resp.WorkerId
	log.Printf("[Worker] Assigned identity: %s", workerID)

	// Save for future boots
	if err := os.WriteFile(idFile, []byte(workerID), 0644); err != nil {
		log.Printf("[Worker] Warning: Failed to persist ID to %s: %v", idFile, err)
	}

	return workerID
}

func main() {
	orchestratorURL := os.Getenv("ORCHESTRATOR_URL")
	if orchestratorURL == "" {
		orchestratorURL = "torv.io:50052"
	}

	// Bootstrap identity
	workerID := ensureWorkerID(orchestratorURL)

	cli, _ := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	img := os.Getenv("NODE_WORKER_AGENT_IMAGE")
	if img == "" {
		img = "ghcr.io/torv-io/pipe-node-worker-agent:latest"
	}

	log.Printf("[Worker] Starting server on :50051 (ID: %s)", workerID)
	
	// Start heartbeat in background
	go startHeartbeat(context.Background(), orchestratorURL, workerID)

	lis, _ := net.Listen("tcp", ":50051")
	s := grpc.NewServer()
	pb.RegisterWorkerServiceServer(s, &workerServer{dockerClient: cli, nodeImage: img})
	reflection.Register(s)
	s.Serve(lis)
}
