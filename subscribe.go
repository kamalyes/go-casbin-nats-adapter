/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-03-28 00:00:00
 * @FilePath: \go-casbin\go-casbin-nats-adapter\subscribe.go
 * @Description: NATS 订阅策略变更事件
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package natsadapter

import (
	"context"
	"encoding/json"

	"github.com/kamalyes/go-casbin/errors"
	"github.com/kamalyes/go-casbin/policy"
	"github.com/nats-io/nats.go"
)

// Subscribe 订阅策略变更事件
func (nn *NATSNotifier) Subscribe(ctx context.Context, handler policy.ChangeEventHandler) error {
	nn.mu.Lock()
	defer nn.mu.Unlock()

	if nn.running {
		return nil
	}

	nn.handler = handler
	nn.running = true

	var sub *nats.Subscription
	var err error

	if nn.js != nil {
		sub, err = nn.js.Subscribe(nn.config.Channel, nn.handleMessage, nats.Durable(nn.config.Source))
	} else {
		sub, err = nn.conn.Subscribe(nn.config.Channel, nn.handleMessage)
	}

	if err != nil {
		nn.running = false
		return errors.NewPolicyWatchFailedError("failed to subscribe NATS subject: " + err.Error())
	}

	nn.sub = sub

	nn.logger.InfoKV("NATS notifier subscribed",
		"subject", nn.config.Channel,
		"source", nn.config.Source,
		"jetstream", nn.js != nil,
	)

	return nil
}

// Unsubscribe 取消订阅
func (nn *NATSNotifier) Unsubscribe() error {
	nn.mu.Lock()
	defer nn.mu.Unlock()

	if !nn.running {
		return nil
	}

	nn.running = false
	if nn.sub != nil {
		_ = nn.sub.Unsubscribe()
	}

	nn.logger.InfoKV("NATS notifier unsubscribed", "subject", nn.config.Channel)
	return nil
}

// handleMessage 处理 NATS 消息
func (nn *NATSNotifier) handleMessage(msg *nats.Msg) {
	var event ChangeEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		nn.logger.WarnKV("Failed to unmarshal NATS event", "error", err.Error())
		return
	}

	if event.Source == nn.config.Source {
		nn.logger.DebugKV("Skipping self-published event",
			"event_type", string(event.Type),
			"source", event.Source,
		)
		return
	}

	nn.mu.RLock()
	handler := nn.handler
	nn.mu.RUnlock()

	if handler != nil {
		handler(&event)
	}
}
