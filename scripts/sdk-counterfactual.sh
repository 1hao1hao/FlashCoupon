#!/bin/sh
# Test the original write in an isolated temporary copy; never mutate the module cache/workspace.
set -eu
cd /src
mkdir -p reports
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
cp -R third_party/rocketmq-client-go "$scratch/sdk"
sed -i 's/data := v.(\*internal.SubscriptionData).Clone()/data := v.(*internal.SubscriptionData)/' "$scratch/sdk/consumer/push_consumer.go"
sed -i '/data.SubVersion = newVersion/{n;/pc.subscriptionDataTable.Store(topic, data)/d;}' "$scratch/sdk/consumer/push_consumer.go"
cp go.mod "$scratch/before.mod"
cp go.sum "$scratch/before.sum"
go mod edit -modfile="$scratch/before.mod" -replace="github.com/apache/rocketmq-client-go/v2=$scratch/sdk"
set +e
go test -modfile="$scratch/before.mod" -race -count=1 github.com/apache/rocketmq-client-go/v2/consumer -run TestSubscriptionVersionCopyOnWrite > reports/sdk-counterfactual.log 2>&1
status=$?
set -e
test "$status" -ne 0
grep -q 'published subscription object was mutated' reports/sdk-counterfactual.log
echo 'Original mutable subscription fails the regression as expected.'
