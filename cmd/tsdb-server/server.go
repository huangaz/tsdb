package main

import (
	"flag"
	"log"
	"os"

	tsdbgrpc "github.com/huangaz/tsdb/server/grpc"
	tsdbjsonrpc "github.com/huangaz/tsdb/server/jsonrpc"
	tsdbrpc "github.com/huangaz/tsdb/server/rpc"
)

type Cmd struct {
	rpctype string
	help    bool
}

var cmd Cmd

func init() {
	flag.BoolVar(&cmd.help, "h", false, "Print usage")
	flag.StringVar(&cmd.rpctype, "r", "grpc", "RPC type: rpc/grpc/jsonrpc")
}

func main() {
	flag.Parse()

	if cmd.help {
		flag.Usage()
		os.Exit(1)
	}

	switch cmd.rpctype {
	case "rpc":
		tsdbrpc.StartRpc()
	case "grpc":
		tsdbgrpc.StartGrpc()
	case "jsonrpc":
		tsdbjsonrpc.StartJsonRpc()
	default:
		log.Fatalf("Invalid rpc type!")
	}
}
