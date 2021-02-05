.PHONY: test clean

bootstrap: ../*.go ../go.* ../cmd/sqat/*.go
	GOARCH=amd64 GOOS=linux go build -o bootstrap ../cmd/sqat

deploy: bootstrap
	lambroll deploy
