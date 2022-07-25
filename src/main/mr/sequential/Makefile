all:
	@go build -race -buildmode=plugin ../../../mrapps/wc/wc.go
	@rm mr-out* || true
	@go run -race ./mrsequential.go wc.so ../../pg*.txt
	@head -n 10 mr-out-0
clean:
	@rm mr-out* wc.so