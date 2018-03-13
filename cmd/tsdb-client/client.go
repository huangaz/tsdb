package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	tsdb "github.com/huangaz/tsdb"
	pb "github.com/huangaz/tsdb/protobuf"
	"google.golang.org/grpc"
)

const (
	address = "localhost:50051"
)

type putOpts struct {
	put bool
	num int
}

type getOpts struct {
	get   bool
	key   string
	begin int64
	end   int64
}

var (
	pOpts putOpts
	gOpts getOpts
)

func init() {
	flag.BoolVar(&pOpts.put, "put", false, "put data points")
	flag.IntVar(&pOpts.num, "n", 1, "the num of data points genetate")

	flag.BoolVar(&gOpts.get, "get", false, "get data points")
	flag.StringVar(&gOpts.key, "k", "", "key to get")
	flag.Int64Var(&gOpts.begin, "b", 0, "begin timestamp")
	flag.Int64Var(&gOpts.end, "e", 0, "end timestamp")
}

func main() {
	flag.Parse()
	if !pOpts.put && !gOpts.get {
		os.Exit(0)
	}

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewTsdbClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if pOpts.put {
		putReq := tsdb.DataGenerator(1, pOpts.num)
		fmt.Println(putReq.PrintForDebug())
		putRes, err := c.Put(ctx, putReq)
		if err != nil {
			log.Fatalf("Put failed: %v", err)
		}
		log.Printf("Put %d data points", putRes.N)
	}

	if gOpts.get {
		getKey := pb.Key{
			Key:     []byte(gOpts.key),
			ShardId: 1,
		}
		getReq := &pb.GetRequest{
			Keys:  []*pb.Key{&getKey},
			Begin: gOpts.begin,
			End:   gOpts.end,
		}

		getRes, err := c.Get(ctx, getReq)
		if err != nil {
			log.Fatalf("Get failed: %v", err)
		}
		log.Println(getRes.PrintForDebug())
	}

}
