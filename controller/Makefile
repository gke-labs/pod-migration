# Trimmed Makefile mirroring what `kubebuilder init` produces.
# The real Makefile is ~200 lines; this captures the targets you'll touch.

IMG ?= controller:latest
CONTROLLER_GEN ?= $(shell go env GOPATH)/bin/controller-gen
.PHONY: help generate manifests fmt vet build run test install uninstall deploy undeploy docker-build docker-push

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

generate: ## Run controller-gen to (re)generate deepcopy methods (zz_generated.deepcopy.go).
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

manifests: ## Generate CRDs, RBAC, and webhook YAML from +kubebuilder:* markers.
	$(CONTROLLER_GEN) rbac:roleName=pod-migration-controller-role crd webhook \
		paths="./..." output:crd:artifacts:config=config/crd/bases
	python3 scripts/assemble_deploy.py

fmt: ; go fmt ./...
vet: ; go vet ./...

build: generate fmt vet ## Compile the manager binary.
	go build -o bin/manager cmd/main.go

run: generate fmt vet manifests ## Run reconcilers against the current kubeconfig.
	# NB: webhooks need to run in-cluster with a serving cert; `make run` is for reconciler iteration.
	go run ./cmd/main.go

test: generate fmt vet manifests ## Run unit + envtest suite.
	KUBEBUILDER_ASSETS="$(shell setup-envtest use -p path)" \
		go test ./... -coverprofile cover.out

install: manifests ## Install CRDs into the K8s cluster and wait for them to be ready.
	kubectl apply -f config/crd/bases/
	kubectl wait --for=condition=established --timeout=60s crd/podmigrations.podmigration.gke.io crd/podmigrationjobs.podmigration.gke.io

uninstall: manifests ## Uninstall CRDs from the K8s cluster.
	kubectl delete --ignore-not-found=true -f config/crd/bases/

deploy: install ## Deploy controller (Deployment + RBAC + webhooks) to the cluster.
	sed 's|<YOUR_CONTROLLER_IMAGE>|$(IMG)|g' deploy.yaml | kubectl apply -f -

undeploy: ## Undeploy controller from the cluster.
	kubectl delete --ignore-not-found=true -f deploy.yaml
	$(MAKE) uninstall

docker-build: ## Build docker image for the controller.
	docker build -t $(IMG) .

docker-push: ## Push docker image for the controller.
	docker push $(IMG)
