.PHONY: test clean

sqat: *.go go.* cmd/sqat/*.go
	cd cmd/sqat && go build -o ../../sqat .

test:
	go test ./...

clean:
	rm -rf sqat dist/
