package main

import (
	"context"
	"log"
	"net"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	pb "torv.io/worker-agent/proto"
)

type server struct {
	pb.UnimplementedWorkerServiceServer
}

func (s *server) ExecuteStageRun(ctx context.Context, req *pb.ExecuteStageRunRequest) (*pb.ExecuteStageRunResponse, error) {
	log.Printf("Received stage run request: stage_id=%s, stage_run_id=%s", req.StageId, req.StageRunId)
	return &pb.ExecuteStageRunResponse{
		Status: pb.Status_EXECUTED,
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterWorkerServiceServer(s, &server{})
	reflection.Register(s)

	log.Printf("gRPC server listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
