package tsdbgrpc

import (
	"log"
	"net"
	"time"

	tsdb "github.com/huangaz/tsdb"
	pb "github.com/huangaz/tsdb/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	context "golang.org/x/net/context"
)

const (
	port = ":50051"
)

type grpcServer struct {
	ts *tsdb.TsdbService
}

func StartGrpc() {
	g := new(grpcServer)
	g.ts = new(tsdb.TsdbService)
	err := g.ts.Start()
	if err != nil {
		log.Fatalf("tsdb start error: %v", err)
	}

	time.Sleep(time.Second)

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterTsdbServer(s, g)
	reflection.Register(s)
	log.Printf("tsdb server start!")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to server: %v", err)
	}
}

func (t *grpcServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	res, err := t.ts.Put(req)
	return res, err
}

func (t *grpcServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	res, err := t.ts.Get(req)
	return res, err
}
