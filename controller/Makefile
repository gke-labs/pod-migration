# Trimmed Makefile mirroring what `kubebuilder init` produces.
# The real Makefile is ~200 lines; this captures the targets you'll touch.

IMG ?= controller:latest
CONTROLLER_GEN ?= $(shell go env GOPATH)/bin/controller-gen
KUSTOMIZE ?= $(shell go env GOPATH)/bin/kustomize

.PHONY: help generate manifests fmt vet build run test install uninstall deploy undeploy docker-build

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

generate: ## Run controller-gen to (re)generate deepcopy methods (zz_generated.deepcopy.go).
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

manifests: ## Generate CRDs, RBAC, and webhook YAML from +kubebuilder:* markers.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook \
		paths="./..." output:crd:artifacts:config=config/crd/bases

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

install: manifests ## kubectl apply the generated CRDs into the current cluster.
	$(KUSTOMIZE) build config/crd | kubectl apply -f -

uninstall: manifests
	$(KUSTOMIZE) build config/crd | kubectl delete --ignore-not-found=true -f -

deploy: manifests ## Deploy controller (Deployment + RBAC + webhooks) to the cluster.
	cd config/manager && $(KUSTOMIZE) edit set image controller=$(IMG)
	$(KUSTOMIZE) build config/default | kubectl apply -f -

undeploy:
	$(KUSTOMIZE) build config/default | kubectl delete --ignore-not-found=true -f -

docker-build:
	docker build -t $(IMG) .
