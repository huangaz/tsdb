package main

import (
	"log"
	"net"
	"time"

	pb "github.com/huangaz/tsdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50051"
)

func main() {
	ts := &pb.TsdbService{}
	err := ts.Start()
	if err != nil {
		log.Fatal("tsdb start error!")
	}
	time.Sleep(time.Second)

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterTsdbServer(s, ts)
	reflection.Register(s)
	log.Printf("tsdb server start!")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to server: %v", err)
	}
}
