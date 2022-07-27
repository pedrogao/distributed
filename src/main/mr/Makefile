all:
	@go build -race -buildmode=plugin ../../mrapps/wc/wc.go
	@rm mr-out* || true
	@go run -race ./coordinator/mrcoordinator.go ../pg-*.txt &
	@for i in {1..3}; do go run -race ./worker/mrworker.go ./wc.so; done
clean:
	@rm mr-* wc.so || true
	@rm ./coordinator/mrcoordinator || true
	@rm ./sequential/mrsequential || true
	@rm ./worker/mrworker || true