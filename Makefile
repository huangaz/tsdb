.PHONY: all test start clean 

OUT_PATH=dist/bin
SERVER_BINARY=tsdb_server
CLIENT_BINARY=tsdb_client
LOG_FILE=dist/log/stdout.log
DATA_FILE=/tmp/path_test

DEPENDS=dist

all: $(OUT_PATH)/$(SERVER_BINARY) $(OUT_PATH)/$(CLIENT_BINARY)

test:
	go test -v -args -v 4 -logtostderr true

$(OUT_PATH)/$(SERVER_BINARY): $(DEPENDS) ./server/server.go
	go build -o $@ -v ./server/server.go 

$(OUT_PATH)/$(CLIENT_BINARY): $(DEPENDS) ./cmd/tsdb-client/*.go
	go build -o $@ -v ./cmd/tsdb-client

dist:
	mkdir -p $(OUT_PATH) dist/log

clean:
	go clean
	rm -rf ./dist
	rm -rf $(DATA_FILE)

start:
	$(OUT_PATH)/$(SERVER_BINARY) >> $(LOG_FILE) 2>&1 &
