# Trimmed Makefile mirroring what `kubebuilder init` produces.
# The real Makefile is ~200 lines; this captures the targets you'll touch.

IMG ?= controller:latest
CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.21.0
ENVTEST_K8S_VERSION ?= 1.36.2
SETUP_ENVTEST ?= $(shell which setup-envtest 2>/dev/null || echo $(shell go env GOPATH)/bin/setup-envtest)
.PHONY: help generate verify-generate manifests fmt vet build run envtest test install uninstall deploy undeploy docker-build docker-push

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

generate: ## Run controller-gen to (re)generate deepcopy methods (zz_generated.deepcopy.go).
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

verify-generate: generate ## Verify generated deepcopy code is up-to-date.
	@if [ -n "$$(git status --porcelain api/v1alpha1/zz_generated.deepcopy.go)" ]; then \
		echo "Error: zz_generated.deepcopy.go is out of date. Run 'make generate' and commit the changes."; \
		git diff api/v1alpha1/zz_generated.deepcopy.go; \
		exit 1; \
	fi

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

envtest: ## Download setup-envtest locally if necessary.
	@if [ ! -x "$$(command -v $(SETUP_ENVTEST))" ] && [ ! -x "$(SETUP_ENVTEST)" ]; then \
		echo "Installing setup-envtest..."; \
		go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest; \
	fi

test: verify-generate fmt vet manifests envtest ## Run unit + envtest suite.
	KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) -p path 2>/dev/null || $(SETUP_ENVTEST) use -p path 2>/dev/null)" \
		go test ./... -coverprofile cover.out

install: manifests ## Install CRDs into the K8s cluster and wait for them to be ready.
	kubectl apply -f config/crd/bases/
	kubectl wait --for=condition=established --timeout=60s crd/podmigrations.podmigration.gke.io crd/podmigrationjobs.podmigration.gke.io

uninstall: manifests ## Uninstall CRDs from the K8s cluster.
	kubectl delete --ignore-not-found=true -f config/crd/bases/

deploy: manifests ## Deploy controller to the K8s cluster.
	kubectl apply -f deploy.yaml

undeploy: ## Undeploy controller from the cluster.
	$(MAKE) uninstall

docker-build: ## Build docker image for the controller.
	docker build -t $(IMG) .

docker-push: ## Push docker image for the controller.
	docker push $(IMG)
