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
