package main

import (
	tsdbgrpc "github.com/huangaz/tsdb/server/grpc"
)

func main() {
	tsdbgrpc.StartGrpc()
}
