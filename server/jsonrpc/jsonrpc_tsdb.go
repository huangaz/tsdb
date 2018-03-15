package tsdbjsonrpc

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

	tsdb "github.com/huangaz/tsdb"
	pb "github.com/huangaz/tsdb/protobuf"
)

const (
	port = ":8435"
)

type JsonRpcServer struct {
	ts *tsdb.TsdbService
}

var count int64

func StartJsonRpc() {
	go countWorker()

	r := new(JsonRpcServer)
	r.ts = new(tsdb.TsdbService)
	if err := r.ts.Start(); err != nil {
		log.Fatalf("tsdb start error: %v", err)
	}
	time.Sleep(time.Second)

	rpc.Register(r)
	tcpAddr, err := net.ResolveTCPAddr("tcp", port)
	if err != nil {
		log.Fatalf("net.ResolveTCPAddr failed: %v", err)
	}

	lis, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Printf("tsdb server start! (jsonrpc)")

	for {
		conn, err := lis.Accept()
		if err != nil {
			continue
		}
		jsonrpc.ServeConn(conn)
	}
}

func (t *JsonRpcServer) Put(req *pb.PutRequest, res *pb.PutResponse) error {
	res, err := t.ts.Put(req)
	count += int64(res.N)
	return err
}

func (t *JsonRpcServer) Get(req *pb.GetRequest, res *pb.GetResponse) error {
	res, err := t.ts.Get(req)
	return err
}

func countWorker() {
	ticker := time.NewTicker(time.Second).C

	for {
		select {
		case <-ticker:
			fmt.Printf("QPS: %d\n", count)
			count = 0
		}
	}
}
