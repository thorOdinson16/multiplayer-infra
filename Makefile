.PHONY: verify dev-up dev-down seed-players fault-inject test build helm-lint

verify:
	./scripts/verify-deps.sh

dev-up:
	./scripts/dev-up.sh

dev-down:
	./scripts/dev-down.sh

seed-players:
	./scripts/seed-players.sh

fault-inject:
	./scripts/fault-inject.sh

test:
	for service in services/*; do if [ -f "$$service/go.mod" ]; then (cd "$$service" && go test ./...); fi; done

build:
	for service in services/*; do if [ -f "$$service/go.mod" ]; then (cd "$$service" && go build -o /dev/null ./cmd/main.go); fi; done

helm-lint:
	for chart in helm/*; do if [ -f "$$chart/Chart.yaml" ]; then helm lint "$$chart"; fi; done
