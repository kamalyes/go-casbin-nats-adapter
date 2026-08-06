/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2026-05-07 19:06:07
 * @FilePath: \go-casbin-nats-adapter\notifier_test.go
 * @Description: NATS 通知器测试
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package natsadapter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kamalyes/go-casbin/policy"
	"github.com/kamalyes/go-logger"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testNATSURL     = "nats://192.168.1.10:4222"
	testSubject     = "casbin.policy.test"
	testTimeout     = 5 * time.Second
	testWaitMessage = 2 * time.Second
)

// setupNATSConnection 创建测试用的 NATS 连接
func setupNATSConnection(t *testing.T) *nats.Conn {
	conn, err := nats.Connect(testNATSURL,
		nats.ReconnectWait(natsReconnectWait),
		nats.MaxReconnects(natsMaxReconnects),
		nats.Timeout(testTimeout),
	)
	require.NoError(t, err, "Failed to connect to NATS server")
	require.NotNil(t, conn, "NATS connection should not be nil")
	assert.True(t, conn.IsConnected(), "NATS connection should be connected")
	return conn
}

// TestNewNATSNotifier 测试创建 NATS 通知器
func TestNewNATSNotifier(t *testing.T) {
	conn := setupNATSConnection(t)
	defer conn.Close()

	t.Run("CreateWithValidConnection", func(t *testing.T) {
		notifier, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject),
			policy.WithSource("test-node-1"),
		)
		require.NoError(t, err, "Should create notifier successfully")
		require.NotNil(t, notifier, "Notifier should not be nil")
		assert.Equal(t, testSubject, notifier.config.Channel)
		assert.Equal(t, "test-node-1", notifier.config.Source)

		err = notifier.Close()
		assert.NoError(t, err, "Should close notifier without error")
	})

	t.Run("CreateWithNilConnection", func(t *testing.T) {
		notifier, err := NewNATSNotifier(nil, nil)
		assert.Error(t, err, "Should return error for nil connection")
		assert.Nil(t, notifier, "Notifier should be nil")
	})

	t.Run("CreateWithDefaultSource", func(t *testing.T) {
		notifier, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject),
		)
		require.NoError(t, err, "Should create notifier successfully")
		require.NotNil(t, notifier, "Notifier should not be nil")
		assert.NotEmpty(t, notifier.config.Source, "Source should be auto-generated")
		assert.Contains(t, notifier.config.Source, "node-", "Source should have node prefix")

		err = notifier.Close()
		assert.NoError(t, err)
	})

	t.Run("CreateWithLogger", func(t *testing.T) {
		testLogger := logger.NewEmptyLogger()
		notifier, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject),
			policy.WithSource("test-node-logger"),
		)
		require.NoError(t, err)
		require.NotNil(t, notifier)

		WithNotifierLogger(testLogger)(notifier)
		assert.NotNil(t, notifier.logger, "Logger should be set")

		err = notifier.Close()
		assert.NoError(t, err)
	})
}

// TestPublishAndSubscribe 测试发布和订阅功能
func TestPublishAndSubscribe(t *testing.T) {
	conn := setupNATSConnection(t)
	defer conn.Close()

	t.Run("PublishPolicyAdded", func(t *testing.T) {
		publisher, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject+".added"),
			policy.WithSource("publisher"),
		)
		require.NoError(t, err)
		defer publisher.Close()

		subscriber, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject+".added"),
			policy.WithSource("subscriber"),
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
		require.NoError(t, err, "Should subscribe successfully")

		// 等待订阅生效
		time.Sleep(100 * time.Millisecond)

		// 发布事件
		ctx := context.Background()
		testPolicy := []string{"alice", "data1", "read"}
		err = publisher.PublishPolicyAdded(ctx, "p", testPolicy)
		require.NoError(t, err, "Should publish event successfully")

		// 等待接收事件
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			require.NotNil(t, receivedEvent, "Should receive event")
			assert.Equal(t, policy.EventTypePolicyAdded, receivedEvent.Type)
			assert.Equal(t, "p", receivedEvent.PType)
			assert.Equal(t, testPolicy, receivedEvent.NewPolicy)
			assert.Equal(t, "publisher", receivedEvent.Source)
		case <-time.After(testWaitMessage):
			t.Fatal("Timeout waiting for event")
		}
	})

	t.Run("PublishPolicyRemoved", func(t *testing.T) {
		publisher, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject+".removed"),
			policy.WithSource("publisher"),
		)
		require.NoError(t, err)
		defer publisher.Close()

		subscriber, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject+".removed"),
			policy.WithSource("subscriber"),
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
		oldPolicy := []string{"bob", "data2", "write"}
		err = publisher.PublishPolicyRemoved(ctx, "p", oldPolicy)
		require.NoError(t, err)

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			require.NotNil(t, receivedEvent)
			assert.Equal(t, policy.EventTypePolicyRemoved, receivedEvent.Type)
			assert.Equal(t, "p", receivedEvent.PType)
			assert.Equal(t, oldPolicy, receivedEvent.OldPolicy)
		case <-time.After(testWaitMessage):
			t.Fatal("Timeout waiting for event")
		}
	})

	t.Run("PublishPolicyUpdated", func(t *testing.T) {
		publisher, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject+".updated"),
			policy.WithSource("publisher"),
		)
		require.NoError(t, err)
		defer publisher.Close()

		subscriber, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject+".updated"),
			policy.WithSource("subscriber"),
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
		oldPolicy := []string{"charlie", "data3", "read"}
		newPolicy := []string{"charlie", "data3", "write"}
		err = publisher.PublishPolicyUpdated(ctx, "p", oldPolicy, newPolicy)
		require.NoError(t, err)

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			require.NotNil(t, receivedEvent)
			assert.Equal(t, policy.EventTypePolicyUpdated, receivedEvent.Type)
			assert.Equal(t, "p", receivedEvent.PType)
			assert.Equal(t, oldPolicy, receivedEvent.OldPolicy)
			assert.Equal(t, newPolicy, receivedEvent.NewPolicy)
		case <-time.After(testWaitMessage):
			t.Fatal("Timeout waiting for event")
		}
	})

	t.Run("PublishPolicyReload", func(t *testing.T) {
		publisher, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject+".reload"),
			policy.WithSource("publisher"),
		)
		require.NoError(t, err)
		defer publisher.Close()

		subscriber, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject+".reload"),
			policy.WithSource("subscriber"),
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
		err = publisher.PublishPolicyReload(ctx)
		require.NoError(t, err)

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			require.NotNil(t, receivedEvent)
			assert.Equal(t, policy.EventTypePolicyReload, receivedEvent.Type)
			assert.Empty(t, receivedEvent.PType)
		case <-time.After(testWaitMessage):
			t.Fatal("Timeout waiting for event")
		}
	})
}

// TestSelfPublishedEventFiltering 测试自发布事件过滤
func TestSelfPublishedEventFiltering(t *testing.T) {
	conn := setupNATSConnection(t)
	defer conn.Close()

	notifier, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".self"),
		policy.WithSource("same-node"),
	)
	require.NoError(t, err)
	defer notifier.Close()

	eventReceived := false
	err = notifier.Subscribe(context.Background(), func(event *ChangeEvent) {
		eventReceived = true
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// 发布自己的事件
	ctx := context.Background()
	err = notifier.PublishPolicyAdded(ctx, "p", []string{"alice", "data1", "read"})
	require.NoError(t, err)

	// 等待一段时间，确保不会收到自己的事件
	time.Sleep(500 * time.Millisecond)

	assert.False(t, eventReceived, "Should not receive self-published event")
}

// TestMultipleSubscribers 测试多个订阅者
func TestMultipleSubscribers(t *testing.T) {
	conn := setupNATSConnection(t)
	defer conn.Close()

	publisher, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".multi"),
		policy.WithSource("publisher"),
	)
	require.NoError(t, err)
	defer publisher.Close()

	// 创建3个订阅者
	subscriberCount := 3
	var wg sync.WaitGroup
	wg.Add(subscriberCount)

	receivedCount := 0
	var mu sync.Mutex

	for i := 0; i < subscriberCount; i++ {
		subscriber, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject+".multi"),
			policy.WithSource("subscriber-"+string(rune('A'+i))),
		)
		require.NoError(t, err)
		defer subscriber.Close()

		err = subscriber.Subscribe(context.Background(), func(event *ChangeEvent) {
			mu.Lock()
			receivedCount++
			mu.Unlock()
			wg.Done()
		})
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	// 发布一个事件
	ctx := context.Background()
	err = publisher.PublishPolicyAdded(ctx, "p", []string{"alice", "data1", "read"})
	require.NoError(t, err)

	// 等待所有订阅者接收
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		mu.Lock()
		assert.Equal(t, subscriberCount, receivedCount, "All subscribers should receive the event")
		mu.Unlock()
	case <-time.After(testWaitMessage):
		t.Fatal("Timeout waiting for all subscribers")
	}
}

// TestUnsubscribe 测试取消订阅
func TestUnsubscribe(t *testing.T) {
	conn := setupNATSConnection(t)
	defer conn.Close()

	publisher, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".unsub"),
		policy.WithSource("publisher"),
	)
	require.NoError(t, err)
	defer publisher.Close()

	subscriber, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".unsub"),
		policy.WithSource("subscriber"),
	)
	require.NoError(t, err)
	defer subscriber.Close()

	eventCount := 0
	var mu sync.Mutex

	err = subscriber.Subscribe(context.Background(), func(event *ChangeEvent) {
		mu.Lock()
		eventCount++
		mu.Unlock()
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// 发布第一个事件
	ctx := context.Background()
	err = publisher.PublishPolicyAdded(ctx, "p", []string{"alice", "data1", "read"})
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	firstCount := eventCount
	mu.Unlock()
	assert.Equal(t, 1, firstCount, "Should receive first event")

	// 取消订阅
	err = subscriber.Unsubscribe()
	require.NoError(t, err)

	// 发布第二个事件
	err = publisher.PublishPolicyAdded(ctx, "p", []string{"bob", "data2", "write"})
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	finalCount := eventCount
	mu.Unlock()
	assert.Equal(t, 1, finalCount, "Should not receive event after unsubscribe")
}

// TestConcurrentPublish 测试并发发布
func TestConcurrentPublish(t *testing.T) {
	conn := setupNATSConnection(t)
	defer conn.Close()

	publisher, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".concurrent"),
		policy.WithSource("publisher"),
	)
	require.NoError(t, err)
	defer publisher.Close()

	subscriber, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".concurrent"),
		policy.WithSource("subscriber"),
	)
	require.NoError(t, err)
	defer subscriber.Close()

	publishCount := 10
	var wg sync.WaitGroup
	wg.Add(publishCount)

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

	// 并发发布多个事件
	ctx := context.Background()
	for i := 0; i < publishCount; i++ {
		go func(index int) {
			policy := []string{"user" + string(rune('0'+index)), "data", "read"}
			err := publisher.PublishPolicyAdded(ctx, "p", policy)
			assert.NoError(t, err)
		}(i)
	}

	// 等待所有事件被接收
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		mu.Lock()
		assert.Equal(t, publishCount, receivedCount, "Should receive all published events")
		mu.Unlock()
	case <-time.After(testTimeout):
		t.Fatal("Timeout waiting for concurrent events")
	}
}

// TestEventTimestamp 测试事件时间戳
func TestEventTimestamp(t *testing.T) {
	conn := setupNATSConnection(t)
	defer conn.Close()

	publisher, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".timestamp"),
		policy.WithSource("publisher"),
	)
	require.NoError(t, err)
	defer publisher.Close()

	subscriber, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".timestamp"),
		policy.WithSource("subscriber"),
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

	beforePublish := time.Now()
	ctx := context.Background()
	err = publisher.PublishPolicyAdded(ctx, "p", []string{"alice", "data1", "read"})
	require.NoError(t, err)
	afterPublish := time.Now()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		require.NotNil(t, receivedEvent)
		assert.False(t, receivedEvent.Timestamp.IsZero(), "Timestamp should not be zero")
		assert.True(t, receivedEvent.Timestamp.After(beforePublish) || receivedEvent.Timestamp.Equal(beforePublish))
		assert.True(t, receivedEvent.Timestamp.Before(afterPublish) || receivedEvent.Timestamp.Equal(afterPublish))
		assert.NotEmpty(t, receivedEvent.ID, "Event ID should not be empty")
	case <-time.After(testWaitMessage):
		t.Fatal("Timeout waiting for event")
	}
}

// mockJetStreamContext 用于测试的 JetStreamContext 桩实现
// 嵌入 nil 接口满足编译，仅覆盖测试需要的方法
type mockJetStreamContext struct {
	nats.JetStreamContext
	publishFn      func(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error)
	addStreamFn    func(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error)
	updateStreamFn func(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error)
}

func (m *mockJetStreamContext) Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	if m.publishFn != nil {
		return m.publishFn(subj, data, opts...)
	}
	return &nats.PubAck{}, nil
}

func (m *mockJetStreamContext) AddStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	if m.addStreamFn != nil {
		return m.addStreamFn(cfg, opts...)
	}
	return &nats.StreamInfo{}, nil
}

func (m *mockJetStreamContext) UpdateStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	if m.updateStreamFn != nil {
		return m.updateStreamFn(cfg, opts...)
	}
	return &nats.StreamInfo{}, nil
}

// TestEnsureStream 测试 EnsureStream 的各路径
func TestEnsureStream(t *testing.T) {
	t.Run("NilJetStream", func(t *testing.T) {
		err := EnsureStream(nil, "test-stream", []string{"test.>"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "JetStream context is nil")
	})

	t.Run("AddStreamSuccess", func(t *testing.T) {
		conn := setupNATSConnection(t)
		defer conn.Close()
		js, err := conn.JetStream()
		require.NoError(t, err)

		streamName := "TEST_ENSURE_ADD_" + fmt.Sprintf("%d", time.Now().UnixNano())
		defer js.DeleteStream(streamName)

		err = EnsureStream(js, streamName, []string{testSubject + ".ensure.add.>"})
		require.NoError(t, err)
	})

	t.Run("UpdateStreamSuccess", func(t *testing.T) {
		conn := setupNATSConnection(t)
		defer conn.Close()
		js, err := conn.JetStream()
		require.NoError(t, err)

		streamName := "TEST_ENSURE_UPD_" + fmt.Sprintf("%d", time.Now().UnixNano())
		defer js.DeleteStream(streamName)

		// 第一次调用：AddStream 成功
		err = EnsureStream(js, streamName, []string{testSubject + ".ensure.upd.>"})
		require.NoError(t, err)
		// 第二次调用：AddStream 失败（已存在），UpdateStream 成功
		err = EnsureStream(js, streamName, []string{testSubject + ".ensure.upd.>"})
		require.NoError(t, err)
	})

	t.Run("BothFail", func(t *testing.T) {
		mockJS := &mockJetStreamContext{
			addStreamFn: func(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
				return nil, errors.New("add stream failed")
			},
			updateStreamFn: func(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
				return nil, errors.New("update stream failed")
			},
		}
		err := EnsureStream(mockJS, "fail-stream", []string{"fail.>"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ensure stream fail-stream failed")
	})
}

// TestPublishEdgeCases 测试 Publish 的边缘路径
func TestPublishEdgeCases(t *testing.T) {
	t.Run("ZeroTimestamp", func(t *testing.T) {
		conn := setupNATSConnection(t)
		defer conn.Close()

		notifier, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject+".zero-ts"),
			policy.WithSource("test-node"),
		)
		require.NoError(t, err)
		defer notifier.Close()

		// 手动构造零时间戳事件，覆盖 Publish 中 IsZero 分支
		event := &policy.ChangeEvent{
			Type:      policy.EventTypePolicyAdded,
			PType:     "p",
			NewPolicy: []string{"alice", "data1", "read"},
		}
		err = notifier.Publish(context.Background(), event)
		require.NoError(t, err)
		assert.False(t, event.Timestamp.IsZero(), "Publish 应填充时间戳")
	})

	t.Run("MarshalError", func(t *testing.T) {
		conn := setupNATSConnection(t)
		defer conn.Close()

		notifier, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(testSubject+".marshal-err"),
			policy.WithSource("test-node"),
		)
		require.NoError(t, err)
		defer notifier.Close()

		orig := marshalEvent
		marshalEvent = func(e *policy.ChangeEvent) ([]byte, error) {
			return nil, errors.New("injected marshal error")
		}
		defer func() { marshalEvent = orig }()

		err = notifier.PublishPolicyAdded(context.Background(), "p", []string{"a"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to marshal event")
	})

	t.Run("RetrySuccess", func(t *testing.T) {
		conn := setupNATSConnection(t)
		defer conn.Close()

		var attempts int32
		mockJS := &mockJetStreamContext{
			publishFn: func(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
				if atomic.AddInt32(&attempts, 1) == 1 {
					return nil, errors.New("injected transient failure")
				}
				return &nats.PubAck{Stream: "test", Sequence: 1}, nil
			},
		}

		notifier, err := NewNATSNotifier(conn, mockJS,
			policy.WithChannel(testSubject+".retry"),
			policy.WithSource("test-node"),
			policy.WithRetry(time.Millisecond, 3),
		)
		require.NoError(t, err)
		defer notifier.Close()

		err = notifier.PublishPolicyAdded(context.Background(), "p", []string{"a"})
		require.NoError(t, err)
		assert.Equal(t, int32(2), atomic.LoadInt32(&attempts), "第一次失败后重试成功")
	})

	t.Run("RetryExhausted", func(t *testing.T) {
		conn := setupNATSConnection(t)
		defer conn.Close()

		var attempts int32
		mockJS := &mockJetStreamContext{
			publishFn: func(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
				atomic.AddInt32(&attempts, 1)
				return nil, errors.New("permanent failure")
			},
		}

		notifier, err := NewNATSNotifier(conn, mockJS,
			policy.WithChannel(testSubject+".retry-exhaust"),
			policy.WithSource("test-node"),
			policy.WithRetry(time.Millisecond, 2),
		)
		require.NoError(t, err)
		defer notifier.Close()

		err = notifier.PublishPolicyAdded(context.Background(), "p", []string{"a"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to publish event to NATS")
	})

	t.Run("JetStreamPublishOnce", func(t *testing.T) {
		conn := setupNATSConnection(t)
		defer conn.Close()
		js, err := conn.JetStream()
		require.NoError(t, err)

		streamName := "TEST_PUB_JS_" + fmt.Sprintf("%d", time.Now().UnixNano())
		subject := testSubject + ".js.pub"
		require.NoError(t, EnsureStream(js, streamName, []string{subject}))
		defer js.DeleteStream(streamName)

		notifier, err := NewNATSNotifier(conn, js,
			policy.WithChannel(subject),
			policy.WithSource("js-publisher"),
		)
		require.NoError(t, err)
		defer notifier.Close()

		err = notifier.PublishPolicyAdded(context.Background(), "p", []string{"alice", "data1", "read"})
		require.NoError(t, err)
	})
}

// TestSubscribeEdgeCases 测试 Subscribe 的边缘路径
func TestSubscribeEdgeCases(t *testing.T) {
	t.Run("JetStreamSubscribe", func(t *testing.T) {
		conn := setupNATSConnection(t)
		defer conn.Close()
		js, err := conn.JetStream()
		require.NoError(t, err)

		streamName := "TEST_SUB_JS_" + fmt.Sprintf("%d", time.Now().UnixNano())
		subject := testSubject + ".js.sub"
		require.NoError(t, EnsureStream(js, streamName, []string{subject}))
		defer js.DeleteStream(streamName)

		publisher, err := NewNATSNotifier(conn, js,
			policy.WithChannel(subject),
			policy.WithSource("js-pub"),
		)
		require.NoError(t, err)
		defer publisher.Close()

		subscriber, err := NewNATSNotifier(conn, js,
			policy.WithChannel(subject),
			policy.WithSource("js-sub-"+fmt.Sprintf("%d", time.Now().UnixNano())),
		)
		require.NoError(t, err)
		defer subscriber.Close()

		var wg sync.WaitGroup
		wg.Add(1)
		var received *ChangeEvent
		err = subscriber.Subscribe(context.Background(), func(event *ChangeEvent) {
			evt := *event
			received = &evt
			wg.Done()
		})
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)

		err = publisher.PublishPolicyAdded(context.Background(), "p", []string{"alice", "data1", "read"})
		require.NoError(t, err)

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			require.NotNil(t, received)
			assert.Equal(t, policy.EventTypePolicyAdded, received.Type)
		case <-time.After(testWaitMessage):
			t.Fatal("Timeout waiting for JetStream event")
		}
	})

	t.Run("SubscribeWithClosedConnection", func(t *testing.T) {
		testConn, err := nats.Connect(testNATSURL)
		require.NoError(t, err)
		testConn.Close()

		notifier, err := NewNATSNotifier(testConn, nil,
			policy.WithChannel(testSubject+".closed-sub"),
			policy.WithSource("test-node"),
		)
		require.NoError(t, err)
		defer notifier.Close()

		err = notifier.Subscribe(context.Background(), func(event *ChangeEvent) {})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to subscribe NATS subject")
	})
}

// TestHandleMessageEdgeCases 测试 handleMessage 的边缘路径
func TestHandleMessageEdgeCases(t *testing.T) {
	t.Run("UnmarshalError", func(t *testing.T) {
		conn := setupNATSConnection(t)
		defer conn.Close()

		subject := testSubject + ".bad-data"
		notifier, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(subject),
			policy.WithSource("bad-data-sub"),
		)
		require.NoError(t, err)
		defer notifier.Close()

		// 订阅不应该收到任何有效事件
		err = notifier.Subscribe(context.Background(), func(event *ChangeEvent) {
			t.Error("Should not receive event for invalid data")
		})
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		// 直接发布无效二进制数据到 subject，触发 handleMessage 中的 UnmarshalEvent 错误
		err = conn.Publish(subject, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x01})
		require.NoError(t, err)

		// 等待确保消息被处理但不触发 handler
		time.Sleep(300 * time.Millisecond)
	})

	t.Run("ChannelFullDrop", func(t *testing.T) {
		conn := setupNATSConnection(t)
		defer conn.Close()

		subject := testSubject + ".channel-full"
		publisher, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(subject),
			policy.WithSource("full-pub"),
		)
		require.NoError(t, err)
		defer publisher.Close()

		subscriber, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(subject),
			policy.WithSource("full-sub"),
			policy.WithBufferSize(1),
		)
		require.NoError(t, err)
		defer subscriber.Close()

		// 用阻塞 handler 填满 channel 后触发丢弃
		blockCh := make(chan struct{})
		receivedCount := int32(0)
		err = subscriber.Subscribe(context.Background(), func(event *ChangeEvent) {
			atomic.AddInt32(&receivedCount, 1)
			<-blockCh // 阻塞直到测试释放
		})
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		// 发布多个事件：第一个被 handler 处理（阻塞），第二个填满 channel，后续被丢弃
		for i := 0; i < 5; i++ {
			err := publisher.PublishPolicyAdded(context.Background(), "p", []string{"u", "d", "r"})
			require.NoError(t, err)
		}

		// 等待消息投递完成
		time.Sleep(500 * time.Millisecond)

		// 释放阻塞的 handler，让 EventLoop 处理 channel 中剩余的 1 个事件
		close(blockCh)
		time.Sleep(300 * time.Millisecond)

		// handler 最多收到 2 个事件（1 个正在处理 + 1 个在 channel 缓冲中），其余被丢弃
		count := atomic.LoadInt32(&receivedCount)
		assert.LessOrEqual(t, count, int32(2), "Should drop events when channel is full, got %d", count)
	})

	t.Run("HandlerPanicRecovery", func(t *testing.T) {
		conn := setupNATSConnection(t)
		defer conn.Close()

		subject := testSubject + ".panic"
		publisher, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(subject),
			policy.WithSource("panic-pub"),
		)
		require.NoError(t, err)
		defer publisher.Close()

		panicTriggered := make(chan struct{})
		subscriber, err := NewNATSNotifier(conn, nil,
			policy.WithChannel(subject),
			policy.WithSource("panic-sub"),
		)
		require.NoError(t, err)
		defer subscriber.Close()

		err = subscriber.Subscribe(context.Background(), func(event *ChangeEvent) {
			close(panicTriggered)
			panic("test panic in handler")
		})
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		err = publisher.PublishPolicyAdded(context.Background(), "p", []string{"alice", "data1", "read"})
		require.NoError(t, err)

		// 等待 handler 被触发
		select {
		case <-panicTriggered:
		case <-time.After(testWaitMessage):
			t.Fatal("Timeout waiting for handler to be triggered")
		}

		// 等待 panic 恢复
		time.Sleep(200 * time.Millisecond)

		// 验证 notifier 仍可正常取消订阅（EventLoop 未崩溃）
		err = subscriber.Unsubscribe()
		assert.NoError(t, err, "EventLoop should recover from panic and remain functional")
	})
}

// BenchmarkPublish 基准测试：发布性能
func BenchmarkPublish(b *testing.B) {
	conn, err := nats.Connect(testNATSURL)
	require.NoError(b, err)
	defer conn.Close()

	notifier, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".bench"),
		policy.WithSource("benchmark"),
	)
	require.NoError(b, err)
	defer notifier.Close()

	ctx := context.Background()
	testPolicy := []string{"alice", "data1", "read"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = notifier.PublishPolicyAdded(ctx, "p", testPolicy)
	}
}

// BenchmarkSubscribe 基准测试：订阅性能
func BenchmarkSubscribe(b *testing.B) {
	conn, err := nats.Connect(testNATSURL)
	require.NoError(b, err)
	defer conn.Close()

	publisher, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".bench.sub"),
		policy.WithSource("publisher"),
	)
	require.NoError(b, err)
	defer publisher.Close()

	subscriber, err := NewNATSNotifier(conn, nil,
		policy.WithChannel(testSubject+".bench.sub"),
		policy.WithSource("subscriber"),
	)
	require.NoError(b, err)
	defer subscriber.Close()

	var wg sync.WaitGroup
	wg.Add(b.N)

	err = subscriber.Subscribe(context.Background(), func(event *ChangeEvent) {
		wg.Done()
	})
	require.NoError(b, err)

	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	testPolicy := []string{"alice", "data1", "read"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = publisher.PublishPolicyAdded(ctx, "p", testPolicy)
	}
	wg.Wait()
}
