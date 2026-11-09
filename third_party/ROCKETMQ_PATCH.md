# RocketMQ client patch provenance

Source: Apache `github.com/apache/rocketmq-client-go/v2` **v2.1.2**, copied unmodified from the verified Go module cache except the files listed below. Its original Apache-2.0 LICENSE and NOTICE are retained. This is the same client and wire protocol, not a new middleware or client architecture.

- `consumer/push_consumer.go`: `messageQueueChanged` clones the existing subscription, updates the private copy, then atomically publishes it in the existing sync.Map. The original SDK writes `SubVersion` on a pointer concurrently read by `SubscriptionData.Clone` in the routing/heartbeat thread. A real `go test -race` integration run detected this race.
- `consumer/subscription_race_regression_test.go`: concurrent version changes and subscription snapshot reads, plus an immutable-original assertion. Before the patch it fails and reports a race.

Reproduce: `go test -race -count=1 github.com/apache/rocketmq-client-go/v2/consumer -run TestSubscriptionVersionCopyOnWrite` followed by the real integration suite. Do not edit the external Go module cache, suppress race reports or change MQ acknowledgement semantics.

Upstream reference: <https://github.com/apache/rocketmq-client-go/blob/v2.1.2/consumer/push_consumer.go>. The module is pinned through the root go.mod replace, so both CI and Docker build use the reviewed local source.
