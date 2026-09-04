# Trimmed Makefile mirroring what `kubebuilder init` produces.
# The real Makefile is ~200 lines; this captures the targets you'll touch.

IMG ?= controller:latest
VERSION ?= $(shell git describe --tags --match 'v*' --dirty 2>/dev/null || echo "v0.1.0-dev")
GIT_COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE ?= $(shell git log -1 --format=%cI 2>/dev/null || echo "unknown")

VERSION_PKG := github.com/gke-labs/pod-migration/controller/internal/version
LDFLAGS := -X '$(VERSION_PKG).Version=$(VERSION)' \
	-X '$(VERSION_PKG).GitCommit=$(GIT_COMMIT)' \
	-X '$(VERSION_PKG).BuildDate=$(BUILD_DATE)'
DOCKER_LDFLAGS := -w $(LDFLAGS)

CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.21.0
ENVTEST_K8S_VERSION ?= 1.36.2
SETUP_ENVTEST_VERSION ?= v0.25.0
SETUP_ENVTEST ?= $(shell which setup-envtest 2>/dev/null || echo $(shell go env GOPATH)/bin/setup-envtest)
.PHONY: help generate verify-generate manifests fmt vet build run envtest test install uninstall deploy undeploy docker-build docker-push

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

generate: ## Run controller-gen to (re)generate deepcopy methods (zz_generated.deepcopy.go).
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

verify-generate: ## Verify generated files are up to date.
	@TMP_DIR=$$(mktemp -d); \
	trap 'rm -rf "$$TMP_DIR"' EXIT; \
	mkdir -p "$$TMP_DIR/api/v1alpha1" "$$TMP_DIR/config/crd" "$$TMP_DIR/config/rbac"; \
	cp api/v1alpha1/zz_generated.deepcopy.go "$$TMP_DIR/api/v1alpha1/"; \
	cp -r config/crd/bases "$$TMP_DIR/config/crd/"; \
	cp config/rbac/role.yaml "$$TMP_DIR/config/rbac/"; \
	cp deploy.yaml "$$TMP_DIR/"; \
	$(MAKE) generate manifests; \
	DIFF_FAILED=0; \
	for path in api/v1alpha1/zz_generated.deepcopy.go config/crd/bases config/rbac/role.yaml deploy.yaml; do \
		diff -ru "$$TMP_DIR/$$path" "$$path" || DIFF_FAILED=1; \
	done; \
	if [ $$DIFF_FAILED -ne 0 ]; then \
		echo "Error: Generated files (deepcopy, CRDs, role.yaml, or deploy.yaml) are out of date. Run 'make generate manifests' and commit the changes."; \
		git status --porcelain api/v1alpha1/zz_generated.deepcopy.go config/crd/bases config/rbac/role.yaml deploy.yaml; \
		exit 1; \
	fi

manifests: ## Generate CRDs, RBAC, and webhook YAML from +kubebuilder:* markers.
	$(CONTROLLER_GEN) rbac:roleName=pod-migration-controller-role crd webhook \
		paths="./..." output:crd:artifacts:config=config/crd/bases
	python3 scripts/assemble_deploy.py

fmt: ; go fmt ./...
vet: ; go vet ./...

build: generate fmt vet ## Compile the manager binary.
	go build -ldflags "$(LDFLAGS)" -o bin/manager cmd/main.go

run: generate fmt vet manifests ## Run reconcilers against the current kubeconfig.
	# NB: webhooks need to run in-cluster with a serving cert; `make run` is for reconciler iteration.
	go run -ldflags "$(LDFLAGS)" ./cmd/main.go

envtest: ## Download setup-envtest locally if necessary.
	@if [ ! -x "$$(command -v $(SETUP_ENVTEST))" ] && [ ! -x "$(SETUP_ENVTEST)" ]; then \
		echo "Installing setup-envtest..."; \
		go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(SETUP_ENVTEST_VERSION); \
	fi

test: generate fmt vet manifests envtest ## Run unit + envtest suite.
	KUBEBUILDER_ASSETS="$$($(SETUP_ENVTEST) use $(ENVTEST_K8S_VERSION) -p path || $(SETUP_ENVTEST) use -p path)" \
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
	docker build --build-arg LDFLAGS="$(DOCKER_LDFLAGS)" -t $(IMG) .

docker-push: ## Push docker image for the controller.
	docker push $(IMG)
