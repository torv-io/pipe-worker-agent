package main

import (
	"context"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "torv.io/worker-agent/proto"
)

func main() {
	orch := getEnv("ORCHESTRATOR_URL", "torv.io:50052")
	secret := getEnv("WORKER_SECRET", "")
	addr := getEnv("WORKER_ADDRESS", "pipe-worker-agent:50051")

	conn, err := grpc.NewClient(orch, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewAgentServiceClient(conn)
	ctx := context.Background()

	r, err := client.Message(ctx, &pb.AgentRequest{
		Type: pb.RequestType_REQUEST_TYPE_REGISTER,
		Body: &pb.AgentRequest_Register{Register: &pb.RegisterBody{Secret: secret, Address: addr}},
	})
	if err != nil {
		log.Fatalf("register: %v", err)
	}
	if !r.GetRegister().GetSuccess() {
		log.Fatalf("register: %s", r.GetRegister().GetError())
	}
	workerID := r.GetRegister().GetWorkerId()
	log.Printf("registered: %s", workerID)

	stream, err := client.Subscribe(ctx)
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	if err := stream.Send(&pb.AgentRequest{
		Type: pb.RequestType_REQUEST_TYPE_SUBSCRIBE,
		Body: &pb.AgentRequest_Subscribe{Subscribe: &pb.SubscribeBody{WorkerId: workerID}},
	}); err != nil {
		log.Fatalf("subscribe send: %v", err)
	}

	go func() {
		tick := time.NewTicker(10 * time.Second)
		for range tick.C {
			hb := &pb.HeartbeatBody{WorkerId: workerID, Status: "idle"}
			if err := stream.Send(&pb.AgentRequest{
				Type: pb.RequestType_REQUEST_TYPE_HEARTBEAT,
				Body: &pb.AgentRequest_Heartbeat{Heartbeat: hb},
			}); err != nil {
				log.Printf("heartbeat: %v", err)
				return
			}
		}
	}()

	for {
		resp, err := stream.Recv()
		if err != nil {
			log.Fatalf("recv: %v", err)
		}
		if w := resp.GetWorkItem(); w != nil {
			log.Printf("work_item: stage_id=%s stage_run_id=%s", w.StageId, w.StageRunId)
		} else {
			log.Printf("message: %v", resp)
		}
	}
}

func getEnv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

