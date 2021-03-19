DOCKER_TAG=$(shell [[ -z "$(shell git tag --points-at HEAD)" ]] && git rev-parse --short HEAD || git tag --points-at HEAD)

.PHONY: run
run: build ## Run miniqueue docker image built from HEAD
	docker run \
		-v $(shell pwd)/testdata:/etc/miniqueue/certs \
		-p 8080:8080 \
		tomarrell/miniqueue:$(DOCKER_TAG) \
		-cert /etc/miniqueue/certs/localhost.pem \
		-key /etc/miniqueue/certs/localhost-key.pem \
		-db /var/lib/miniqueue \
		-human

.PHONY: build
build: ## Build a docker image with the git tag pointing to current HEAD or the current commit hash.
	docker build . -t tomarrell/miniqueue:$(DOCKER_TAG)

.PHONY: bench
bench: ## Run Go benchmarks
	go test -bench=. -run=$$^


## Help display.
## Pulls comments from beside commands and prints a nicely formatted
## display with the commands and their usage information.

.DEFAULT_GOAL := help

help: ## Prints this help
	@grep -h -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
