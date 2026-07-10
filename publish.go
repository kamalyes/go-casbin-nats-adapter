/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-03-28 00:00:00
 * @FilePath: \go-casbin\go-casbin-nats-adapter\publish.go
 * @Description: NATS 发布策略变更事件
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package natsadapter

import (
	"context"
	"time"

	"github.com/kamalyes/go-casbin/errors"
	"github.com/kamalyes/go-casbin/policy"
)

// Publish 发布策略变更事件到 NATS Subject
// 成功路径直接发送，失败才进入 retry 循环，避免每次都走 retry.Do 的加锁+caller 查找开销
func (nn *NATSNotifier) Publish(ctx context.Context, event *policy.ChangeEvent) error {
	event.ID = nn.idgen.GenerateRequestID()
	event.Source = nn.config.Source
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// 二进制编码（零反射，buffer 池复用）
	data, err := MarshalEvent(event)
	if err != nil {
		return errors.NewPolicyWatchFailedError("failed to marshal event: " + err.Error())
	}
	// NATS Publish 会拷贝 data，归还 buffer 到池
	defer releaseBuffer(data)

	// 成功路径：直接发送，跳过 retry.Do 的加锁 + runtime.Caller 开销
	if nn.publishOnce(data) == nil {
		nn.logger.DebugKV("Policy change event published to NATS",
			"subject", nn.config.Channel,
			"event_type", string(event.Type),
			"source", event.Source,
		)
		return nil
	}

	// 失败路径：进入 retry 循环
	retryErr := nn.retry.Do(func() error {
		return nn.publishOnce(data)
	})
	if retryErr != nil {
		return errors.NewPolicyWatchFailedError("failed to publish event to NATS: " + retryErr.Error())
	}

	nn.logger.DebugKV("Policy change event published to NATS (after retry)",
		"subject", nn.config.Channel,
		"event_type", string(event.Type),
		"source", event.Source,
	)
	return nil
}

// publishOnce 执行一次 NATS 发布
// 非 JetStream 模式用 conn.Publish（写入 socket buffer 即返回）
// JetStream 模式用 js.Publish（同步等待 server ack）
func (nn *NATSNotifier) publishOnce(data []byte) error {
	if nn.js != nil {
		_, err := nn.js.Publish(nn.config.Channel, data)
		return err
	}
	return nn.conn.Publish(nn.config.Channel, data)
}

// PublishPolicyAdded 发布策略添加事件
func (nn *NATSNotifier) PublishPolicyAdded(ctx context.Context, ptype string, p []string) error {
	event := policy.NewChangeEvent(policy.EventTypePolicyAdded, ptype, nn.config.Source)
	event.NewPolicy = p
	return nn.Publish(ctx, event)
}

// PublishPolicyRemoved 发布策略删除事件
func (nn *NATSNotifier) PublishPolicyRemoved(ctx context.Context, ptype string, oldPolicy []string) error {
	event := policy.NewChangeEvent(policy.EventTypePolicyRemoved, ptype, nn.config.Source)
	event.OldPolicy = oldPolicy
	return nn.Publish(ctx, event)
}

// PublishPolicyUpdated 发布策略更新事件
func (nn *NATSNotifier) PublishPolicyUpdated(ctx context.Context, ptype string, oldPolicy, newPolicy []string) error {
	event := policy.NewChangeEvent(policy.EventTypePolicyUpdated, ptype, nn.config.Source)
	event.OldPolicy = oldPolicy
	event.NewPolicy = newPolicy
	return nn.Publish(ctx, event)
}

// PublishPolicyReload 发布策略全量重载事件
func (nn *NATSNotifier) PublishPolicyReload(ctx context.Context) error {
	event := policy.NewChangeEvent(policy.EventTypePolicyReload, "", nn.config.Source)
	return nn.Publish(ctx, event)
}
