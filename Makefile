IMAGE ?= pod-migration:latest

.PHONY: all
all: build

.PHONY: build
build:
	go build -o bin/manager main.go

.PHONY: test
test:
	go test ./...

.PHONY: docker-build
docker-build:
	docker build -t $(IMAGE) .

.PHONY: docker-push
docker-push:
	docker push $(IMAGE)
