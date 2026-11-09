#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
dc() { docker compose -f compose.yaml -f compose.sharding.yaml "$@"; }
mkdir -p reports
dc up -d --build mysql redis broker proxy mq-topic
dc stop app relay worker
trap 'dc up -d --build app relay worker' EXIT
dc run --rm -e ROLE=all toolbox sh -c 'go test -count=1 ./... && go test -race -count=1 -v ./... && go vet ./... && go test -race -count=1 github.com/apache/rocketmq-client-go/v2/consumer -run TestSubscriptionVersionCopyOnWrite && go run ./cmd/whitelist'
