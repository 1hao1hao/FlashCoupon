package consumer

import (
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"sync"
	"testing"
)

type heartbeatOnlyClient struct{ internal.RMQClient }

func (heartbeatOnlyClient) SendHeartbeatToAllBrokerWithLock() {}
func TestSubscriptionVersionCopyOnWrite(t *testing.T) {
	rlog.SetLogLevel("error")
	original := &internal.SubscriptionData{Topic: "regression", SubVersion: 1}
	pc := &pushConsumer{defaultConsumer: &defaultConsumer{client: heartbeatOnlyClient{}}}
	pc.subscriptionDataTable.Store("regression", original)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			pc.messageQueueChanged("regression", nil, nil)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			pc.SubscriptionDataList()
		}
	}()
	wg.Wait()
	if original.SubVersion != 1 {
		t.Fatal("published subscription object was mutated")
	}
	current, _ := pc.subscriptionDataTable.Load("regression")
	if current == original {
		t.Fatal("replacement was not published")
	}
}
