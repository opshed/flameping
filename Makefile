.PHONY: all build test race web browser-smoke clean

VERSION ?= dev
COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null || printf unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
BUILDINFO = github.com/opshed/flameping/internal/buildinfo

all: build

build:
	CGO_ENABLED=0 go build -buildvcs=false -trimpath -ldflags "-s -w -X $(BUILDINFO).Version=$(VERSION) -X $(BUILDINFO).Commit=$(COMMIT) -X $(BUILDINFO).Date=$(BUILD_DATE)" -o flameping ./cmd/flameping

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
