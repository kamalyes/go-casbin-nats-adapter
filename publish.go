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
	"encoding/json"
	"time"

	"github.com/kamalyes/go-casbin/errors"
	"github.com/kamalyes/go-casbin/policy"
)

// Publish 发布策略变更事件到 NATS Subject
func (nn *NATSNotifier) Publish(ctx context.Context, event *ChangeEvent) error {
	event.ID = nn.idgen.GenerateRequestID()
	event.Source = nn.config.Source
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	data, err := json.Marshal(event)
	if err != nil {
		return errors.NewPolicyWatchFailedError("failed to marshal event: " + err.Error())
	}

	var publishErr error
	retryErr := nn.retry.Do(func() error {
		if nn.js != nil {
			_, publishErr = nn.js.Publish(nn.config.Channel, data)
		} else {
			publishErr = nn.conn.Publish(nn.config.Channel, data)
		}
		return publishErr
	})

	if retryErr != nil {
		return errors.NewPolicyWatchFailedError("failed to publish event to NATS: " + retryErr.Error())
	}

	nn.logger.DebugKV("Policy change event published to NATS",
		"subject", nn.config.Channel,
		"event_type", string(event.Type),
		"source", event.Source,
	)

	return nil
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
