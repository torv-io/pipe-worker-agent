package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	pb "torv.io/worker-agent/proto"
)

const (
	defaultOrchestratorURL = "torv.io:50052"
	defaultNodeImage       = "ghcr.io/torv-io/pipe-node-worker-agent:latest"
	grpcPort               = ":50051"
	heartbeatInterval      = 10 * time.Second
)

// workerServer implements the gRPC WorkerService and runs stage code in Docker containers.
type workerServer struct {
	pb.UnimplementedWorkerServiceServer
	dockerClient     *client.Client
	nodeImage        string
	orchestratorURL  string
	workerID         string
}

func (s *workerServer) ExecuteStageRun(ctx context.Context, req *pb.ExecuteStageRunRequest) (*pb.ExecuteStageRunResponse, error) {
	log.Printf("[Worker] Executing stage run: %s", req.StageRunId)

	input, _ := json.Marshal(map[string]interface{}{
		"code": req.Code, "context": req.Context, "stageConfig": req.StageConfig,
	})

	// Run code in ephemeral container attached to worker network
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

	// Stream stdout line-by-line: log lines are forwarded to orchestrator; result line is the final response
	res := s.streamContainerOutput(ctx, attach.Reader, req.StageRunId)
	s.dockerClient.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)

	if res == nil {
		return &pb.ExecuteStageRunResponse{Status: pb.Status_FAILED, Error: "Invalid output"}, nil
	}

	outputs := extractOutputs(res)
	status := pb.Status_EXECUTED
	if success, _ := res["success"].(bool); !success {
		status = pb.Status_FAILED
	}
	errStr, _ := res["error"].(string)

	return &pb.ExecuteStageRunResponse{Status: status, Error: errStr, Outputs: outputs}, nil
}

// streamContainerOutput reads JSON lines from container stdout. Log lines are forwarded to the orchestrator
// via gRPC Message; the final result line is returned.
func (s *workerServer) streamContainerOutput(ctx context.Context, r io.Reader, stageRunID string) map[string]interface{} {
	conn, err := grpc.NewClient(s.orchestratorURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[Worker] Failed to connect to orchestrator for logs: %v", err)
		return readResultFromReader(r)
	}
	defer conn.Close()
	agentClient := pb.NewAgentServiceClient(conn)

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var raw map[string]interface{}
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			continue
		}
		typ, _ := raw["type"].(string)
		switch typ {
		case "log":
			msg, _ := raw["message"].(string)
			level, _ := raw["level"].(string)
			if msg != "" {
				_, _ = agentClient.Message(ctx, &pb.MessageRequest{
					WorkerId:   s.workerID,
					StageRunId: stageRunID,
					Message:    msg,
					Level:      level,
				})
			}
		case "result":
			return raw
		default:
			// Backward compat: line without type but with success/outputs is the result
			if _, ok := raw["success"]; ok {
				return raw
			}
		}
	}
	return nil
}

// readResultFromReader parses JSON lines (or single JSON) when gRPC to orchestrator is unavailable.
func readResultFromReader(r io.Reader) map[string]interface{} {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil
	}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var raw map[string]interface{}
		if json.Unmarshal([]byte(line), &raw) != nil {
			continue
		}
		if typ, _ := raw["type"].(string); typ == "result" {
			return raw
		}
		if _, ok := raw["success"]; ok {
			return raw
		}
	}
	var single map[string]interface{}
	if json.Unmarshal(data, &single) == nil && (single["success"] != nil || single["outputs"] != nil) {
		return single
	}
	return nil
}

// extractOutputs flattens the outputs map from JSON response to string values.
func extractOutputs(res map[string]interface{}) map[string]string {
	outputs := make(map[string]string)
	om, ok := res["outputs"].(map[string]interface{})
	if !ok {
		return outputs
	}
	for k, v := range om {
		if s, ok := v.(string); ok {
			outputs[k] = s
		} else {
			b, _ := json.Marshal(v)
			outputs[k] = string(b)
		}
	}
	return outputs
}

// startHeartbeat periodically notifies the orchestrator that this worker is alive.
func startHeartbeat(ctx context.Context, orchestratorURL, workerID string) {
	conn, err := grpc.NewClient(orchestratorURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[Heartbeat] Failed to connect: %v", err)
		return
	}
	defer conn.Close()

	agentClient := pb.NewAgentServiceClient(conn)
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	log.Printf("[Heartbeat] Starting for worker: %s", workerID)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := agentClient.Heartbeat(ctx, &pb.HeartbeatRequest{WorkerId: workerID, Status: "online"}); err != nil {
				log.Printf("[Heartbeat] Error: %v", err)
			}
		}
	}
}

// ensureWorkerID registers with the orchestrator and returns the assigned worker ID.
func ensureWorkerID(orchestratorURL string) string {
	log.Println("[Worker] Registering with orchestrator...")
	conn, err := grpc.NewClient(orchestratorURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[Worker] Failed to connect for registration: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	address := getEnv("WORKER_ADDRESS", "")
	if address == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "localhost"
		}
		port := strings.TrimPrefix(grpcPort, ":")
		if port == "" {
			port = "50051"
		}
		address = host + ":" + port
	}

	resp, err := pb.NewAgentServiceClient(conn).Register(ctx, &pb.RegisterRequest{
		Address: address,
	})
	if err != nil {
		log.Fatalf("[Worker] Registration RPC failed: %v", err)
	}
	if !resp.Success {
		log.Fatalf("[Worker] Registration rejected: %s", resp.Error)
	}

	log.Printf("[Worker] Assigned identity: %s", resp.WorkerId)
	return resp.WorkerId
}

func main() {
	orchestratorURL := getEnv("ORCHESTRATOR_URL", defaultOrchestratorURL)
	nodeImage := getEnv("NODE_WORKER_AGENT_IMAGE", defaultNodeImage)

	workerID := ensureWorkerID(orchestratorURL)

	cli, _ := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())

	log.Printf("[Worker] Starting server on %s (ID: %s)", grpcPort, workerID)
	go startHeartbeat(context.Background(), orchestratorURL, workerID)

	lis, _ := net.Listen("tcp", grpcPort)
	srv := grpc.NewServer()
	pb.RegisterWorkerServiceServer(srv, &workerServer{
		dockerClient:    cli,
		nodeImage:       nodeImage,
		orchestratorURL: orchestratorURL,
		workerID:        workerID,
	})
	reflection.Register(srv)
	srv.Serve(lis)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
