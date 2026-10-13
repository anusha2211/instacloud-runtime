GO      ?= go
BIN     := bin
CMDS    := instacloud-controlplane instacloud-agent icctl demoapp

.PHONY: all build test race vet fmt lint demo clean

all: vet test build

build:
	@mkdir -p $(BIN)
	@for c in $(CMDS); do $(GO) build -o $(BIN)/$$c ./cmd/$$c || exit 1; done
	@echo "built: $(addprefix $(BIN)/,$(CMDS))"

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

fmt:
	gofmt -s -w .

lint: vet
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

demo: build
	./scripts/demo.sh

clean:
	rm -rf $(BIN) .data
