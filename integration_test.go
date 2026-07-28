/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-05-07 19:02:30
 * @FilePath: \go-casbin-nats-adapter\integration_test.go
 * @Description: NATS 适配器集成测试
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package natsadapter

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kamalyes/go-casbin/policy"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDistributedPolicySync 测试分布式策略同步场景
func TestDistributedPolicySync(t *testing.T) {
	conn := setupNATSConnection(t)
	defer conn.Close()

	// 模拟3个节点
	nodeCount := 3
	nodes := make([]*NATSNotifier, nodeCount)
	eventCounters := make([]int, nodeCount)
	var mu sync.Mutex

	// 创建节点
	for i := 0; i < nodeCount; i++ {
		node, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject+".distributed"),
			policy.WithSource("node-"+string(rune('A'+i))),
		)
		require.NoError(t, err)
		nodes[i] = node
		defer node.Close()

		// 每个节点订阅事件
		nodeIndex := i
		err = node.Subscribe(context.Background(), func(event *ChangeEvent) {
			mu.Lock()
			eventCounters[nodeIndex]++
			mu.Unlock()
		})
		require.NoError(t, err)
	}

	time.Sleep(200 * time.Millisecond)

	// 节点0发布事件
	ctx := context.Background()
	err := nodes[0].PublishPolicyAdded(ctx, "p", []string{"alice", "data1", "read"})
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// 验证：节点0不应该收到自己的事件，其他节点应该收到
	mu.Lock()
	assert.Equal(t, 0, eventCounters[0], "Node 0 should not receive its own event")
	assert.Equal(t, 1, eventCounters[1], "Node 1 should receive the event")
	assert.Equal(t, 1, eventCounters[2], "Node 2 should receive the event")
	mu.Unlock()

	// 节点1发布事件
	err = nodes[1].PublishPolicyRemoved(ctx, "p", []string{"bob", "data2", "write"})
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// 验证：节点1不应该收到自己的事件，其他节点应该收到
	mu.Lock()
	assert.Equal(t, 1, eventCounters[0], "Node 0 should receive node 1's event")
	assert.Equal(t, 1, eventCounters[1], "Node 1 should not receive its own event")
	assert.Equal(t, 2, eventCounters[2], "Node 2 should receive both events")
	mu.Unlock()
}

// TestPolicyEventSequence 测试策略事件序列
func TestPolicyEventSequence(t *testing.T) {
	conn := setupNATSConnection(t)
	defer conn.Close()

	publisher, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".sequence"),
		policy.WithSource("publisher"),
	)
	require.NoError(t, err)
	defer publisher.Close()

	subscriber, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".sequence"),
		policy.WithSource("subscriber"),
	)
	require.NoError(t, err)
	defer subscriber.Close()

	var receivedEvents []*ChangeEvent
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(4)

	err = subscriber.Subscribe(context.Background(), func(event *ChangeEvent) {
		evt := *event
		mu.Lock()
		receivedEvents = append(receivedEvents, &evt)
		mu.Unlock()
		wg.Done()
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()

	// 发布一系列事件
	err = publisher.PublishPolicyAdded(ctx, "p", []string{"alice", "data1", "read"})
	require.NoError(t, err)

	err = publisher.PublishPolicyAdded(ctx, "p", []string{"bob", "data2", "write"})
	require.NoError(t, err)

	err = publisher.PublishPolicyUpdated(ctx, "p",
		[]string{"alice", "data1", "read"},
		[]string{"alice", "data1", "write"},
	)
	require.NoError(t, err)

	err = publisher.PublishPolicyRemoved(ctx, "p", []string{"bob", "data2", "write"})
	require.NoError(t, err)

	// 等待所有事件
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		mu.Lock()
		assert.Equal(t, 4, len(receivedEvents), "Should receive all 4 events")

		// 验证事件顺序和内容
		assert.Equal(t, policy.EventTypePolicyAdded, receivedEvents[0].Type)
		assert.Equal(t, []string{"alice", "data1", "read"}, receivedEvents[0].NewPolicy)

		assert.Equal(t, policy.EventTypePolicyAdded, receivedEvents[1].Type)
		assert.Equal(t, []string{"bob", "data2", "write"}, receivedEvents[1].NewPolicy)

		assert.Equal(t, policy.EventTypePolicyUpdated, receivedEvents[2].Type)
		assert.Equal(t, []string{"alice", "data1", "read"}, receivedEvents[2].OldPolicy)
		assert.Equal(t, []string{"alice", "data1", "write"}, receivedEvents[2].NewPolicy)

		assert.Equal(t, policy.EventTypePolicyRemoved, receivedEvents[3].Type)
		assert.Equal(t, []string{"bob", "data2", "write"}, receivedEvents[3].OldPolicy)
		mu.Unlock()
	case <-time.After(testTimeout):
		t.Fatal("Timeout waiting for event sequence")
	}
}

// TestReconnection 测试重连场景
func TestReconnection(t *testing.T) {
	conn := setupNATSConnection(t)
	defer conn.Close()

	notifier, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".reconnect"),
		policy.WithSource("test-node"),
	)
	require.NoError(t, err)
	defer notifier.Close()

	// 验证连接状态
	assert.True(t, conn.IsConnected(), "Connection should be active")
	assert.NotNil(t, notifier.conn, "Notifier should have connection")

	// 发布事件验证连接正常
	ctx := context.Background()
	err = notifier.PublishPolicyAdded(ctx, "p", []string{"alice", "data1", "read"})
	assert.NoError(t, err, "Should publish successfully")
}

// TestHighThroughput 测试高吞吐量场景
func TestHighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping high throughput test in short mode")
	}

	conn := setupNATSConnection(t)
	defer conn.Close()

	publisher, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".throughput"),
		policy.WithSource("publisher"),
	)
	require.NoError(t, err)
	defer publisher.Close()

	subscriber, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".throughput"),
		policy.WithSource("subscriber"),
	)
	require.NoError(t, err)
	defer subscriber.Close()

	messageCount := 1000
	var wg sync.WaitGroup
	wg.Add(messageCount)

	receivedCount := 0
	var mu sync.Mutex

	err = subscriber.Subscribe(context.Background(), func(event *ChangeEvent) {
		mu.Lock()
		receivedCount++
		mu.Unlock()
		wg.Done()
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// 发布大量消息
	ctx := context.Background()
	startTime := time.Now()

	for i := 0; i < messageCount; i++ {
		policy := []string{"user", "data" + string(rune('0'+i%10)), "read"}
		err := publisher.PublishPolicyAdded(ctx, "p", policy)
		require.NoError(t, err)
	}

	// 等待所有消息被接收
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		duration := time.Since(startTime)
		mu.Lock()
		assert.Equal(t, messageCount, receivedCount, "Should receive all messages")
		mu.Unlock()

		throughput := float64(messageCount) / duration.Seconds()
		t.Logf("Throughput: %.2f messages/second", throughput)
		t.Logf("Total time: %v", duration)
		t.Logf("Average latency: %v", duration/time.Duration(messageCount))
	case <-time.After(30 * time.Second):
		mu.Lock()
		t.Fatalf("Timeout: only received %d/%d messages", receivedCount, messageCount)
		mu.Unlock()
	}
}

// TestMultipleChannels 测试多个频道
func TestMultipleChannels(t *testing.T) {
	conn := setupNATSConnection(t)
	defer conn.Close()

	channel1 := testSubject + ".channel1"
	channel2 := testSubject + ".channel2"

	// 创建两个发布者，使用不同的频道
	publisher1, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(channel1),
		policy.WithSource("publisher1"),
	)
	require.NoError(t, err)
	defer publisher1.Close()

	publisher2, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(channel2),
		policy.WithSource("publisher2"),
	)
	require.NoError(t, err)
	defer publisher2.Close()

	// 创建两个订阅者，分别订阅不同的频道
	subscriber1, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(channel1),
		policy.WithSource("subscriber1"),
	)
	require.NoError(t, err)
	defer subscriber1.Close()

	subscriber2, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(channel2),
		policy.WithSource("subscriber2"),
	)
	require.NoError(t, err)
	defer subscriber2.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	count1 := 0
	count2 := 0
	var mu sync.Mutex

	err = subscriber1.Subscribe(context.Background(), func(event *ChangeEvent) {
		mu.Lock()
		count1++
		mu.Unlock()
		wg.Done()
	})
	require.NoError(t, err)

	err = subscriber2.Subscribe(context.Background(), func(event *ChangeEvent) {
		mu.Lock()
		count2++
		mu.Unlock()
		wg.Done()
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// 在不同频道发布事件
	ctx := context.Background()
	err = publisher1.PublishPolicyAdded(ctx, "p", []string{"alice", "data1", "read"})
	require.NoError(t, err)

	err = publisher2.PublishPolicyAdded(ctx, "p", []string{"bob", "data2", "write"})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		mu.Lock()
		assert.Equal(t, 1, count1, "Subscriber 1 should receive 1 event from channel 1")
		assert.Equal(t, 1, count2, "Subscriber 2 should receive 1 event from channel 2")
		mu.Unlock()
	case <-time.After(testWaitMessage):
		t.Fatal("Timeout waiting for events")
	}
}

// TestErrorHandling 测试错误处理
func TestErrorHandling(t *testing.T) {
	conn := setupNATSConnection(t)
	defer conn.Close()

	t.Run("PublishWithClosedConnection", func(t *testing.T) {
		// 创建一个新连接用于测试
		testConn, err := nats.Connect(testNATSURL)
		require.NoError(t, err)

		notifier, err := NewNATSNotifier(testConn, nil,
			policy.WithChannel(testSubject+".error"),
			policy.WithSource("test-node"),
		)
		require.NoError(t, err)

		// 关闭连接
		testConn.Close()

		// 尝试发布事件
		ctx := context.Background()
		err = notifier.PublishPolicyAdded(ctx, "p", []string{"alice", "data1", "read"})
		assert.Error(t, err, "Should return error when publishing with closed connection")
	})

	t.Run("SubscribeMultipleTimes", func(t *testing.T) {
		notifier, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject+".multi-sub"),
			policy.WithSource("test-node"),
		)
		require.NoError(t, err)
		defer notifier.Close()

		handler := func(event *ChangeEvent) {}

		// 第一次订阅
		err = notifier.Subscribe(context.Background(), handler)
		require.NoError(t, err)

		// 第二次订阅应该成功（幂等）
		err = notifier.Subscribe(context.Background(), handler)
		assert.NoError(t, err, "Multiple subscribe calls should be idempotent")
	})

	t.Run("UnsubscribeWithoutSubscribe", func(t *testing.T) {
		notifier, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject+".no-sub"),
			policy.WithSource("test-node"),
		)
		require.NoError(t, err)
		defer notifier.Close()

		// 未订阅就取消订阅
		err = notifier.Unsubscribe()
		assert.NoError(t, err, "Unsubscribe without subscribe should not error")
	})
}

// TestEventMetadata 测试事件元数据
func TestEventMetadata(t *testing.T) {
	conn := setupNATSConnection(t)
	defer conn.Close()

	publisher, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".metadata"),
		policy.WithSource("test-publisher"),
	)
	require.NoError(t, err)
	defer publisher.Close()

	subscriber, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".metadata"),
		policy.WithSource("test-subscriber"),
	)
	require.NoError(t, err)
	defer subscriber.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	var receivedEvent *ChangeEvent
	err = subscriber.Subscribe(context.Background(), func(event *ChangeEvent) {
		evt := *event
		receivedEvent = &evt
		wg.Done()
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	err = publisher.PublishPolicyAdded(ctx, "p", []string{"alice", "data1", "read"})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		require.NotNil(t, receivedEvent)

		// 验证元数据
		assert.NotEmpty(t, receivedEvent.ID, "Event ID should not be empty")
		assert.Equal(t, "test-publisher", receivedEvent.Source, "Source should match publisher")
		assert.False(t, receivedEvent.Timestamp.IsZero(), "Timestamp should be set")
		assert.Equal(t, policy.EventTypePolicyAdded, receivedEvent.Type, "Event type should match")
		assert.Equal(t, "p", receivedEvent.PType, "PType should match")

		// 验证时间戳在合理范围内（最近1秒内）
		timeDiff := time.Since(receivedEvent.Timestamp)
		assert.Less(t, timeDiff, 2*time.Second, "Timestamp should be recent")
	case <-time.After(testWaitMessage):
		t.Fatal("Timeout waiting for event")
	}
}
