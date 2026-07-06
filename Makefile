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

.PHONY: deploy
deploy:
	kubectl apply -f deploy/crds/ || true
	kubectl apply -f deploy/rbac.yaml -f deploy/service.yaml -f deploy/webhook.yaml -f deploy/cert-manager.yaml
	cat deploy/deployment.yaml | sed 's|image: pod-migration:latest|image: $(IMAGE)|g' | kubectl apply -f -

