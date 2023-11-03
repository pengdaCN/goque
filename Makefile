fmt:
	goimports -l -w .
	gofmt -s -w .

lint: fmt
	go vet ./...