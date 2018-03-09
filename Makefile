.PHONY: all test start clean 

OUT_PATH=dist/bin
SERVER_BINARY_NAME=tsdb_server
CLIENT_BINARY_NAME=tsdb_client
OBJECT=$(OUT_PATH)/$(BINARY_NAME)
LOG_FILE=dist/log/stdout.log

DEPENDS=dist

all: $(OUT_PATH)/$(SERVER_BINARY_NAME) $(OUT_PATH)/$(CLIENT_BINARY_NAME)

test:
	go test -v -args -v 4 -logtostderr true

$(OUT_PATH)/$(SERVER_BINARY_NAME): $(DEPENDS) ./cmd/tsdb-server/*.go
	go build -o $@ -v ./cmd/tsdb-server 

$(OUT_PATH)/$(CLIENT_BINARY_NAME): $(DEPENDS) ./cmd/tsdb-client/*.go
	go build -o $@ -v ./cmd/tsdb-client

dist:
	mkdir -p $(OUT_PATH) dist/log

clean:
	go clean
	rm -rf ./dist

start:
	$(OBJECT) start >> $(LOG_FILE) 2>&1 &
