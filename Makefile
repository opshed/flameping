.PHONY: all build test race web browser-smoke clean

all: build

build:
	CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags "-s -w" -o flameping ./cmd/flameping

test:
	go test ./...

race:
	go test -race ./...

web:
	cd web && npm ci && npm run build

browser-smoke:
	cd web && npm run build && npm run browser-smoke

clean:
	rm -f flameping
