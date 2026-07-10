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
	"fmt"

	"github.com/kamalyes/go-casbin/errors"
	"github.com/kamalyes/go-casbin/policy"
	"github.com/kamalyes/go-toolbox/pkg/syncx"
	"github.com/nats-io/nats.go"
)

// Subscribe 订阅策略变更事件
// 使用 syncx.EventLoop 启动事件循环，异步处理事件避免 handler 阻塞 NATS 消息投递
func (nn *NATSNotifier) Subscribe(ctx context.Context, handler policy.ChangeEventHandler) error {
	nn.mu.Lock()
	defer nn.mu.Unlock()

	if nn.running {
		return nil
	}

	nn.handler = handler
	nn.running = true

	// 使用 syncx.EventLoop 启动事件处理循环（内置 panic 恢复和优雅关闭）
	nn.startEventLoop()

	var sub *nats.Subscription
	var err error

	if nn.js != nil {
		sub, err = nn.js.Subscribe(nn.config.Channel, nn.handleMessage, nats.Durable(nn.config.Source))
	} else {
		sub, err = nn.conn.Subscribe(nn.config.Channel, nn.handleMessage)
	}

	if err != nil {
		nn.running = false
		// 通知 EventLoop 退出
		nn.workerCancel()
		nn.wg.Wait()
		return errors.NewPolicyWatchFailedError("failed to subscribe NATS subject: " + err.Error())
	}

	nn.sub = sub

	nn.logger.InfoKV("NATS notifier subscribed",
		"subject", nn.config.Channel,
		"source", nn.config.Source,
		"jetstream", nn.js != nil,
		"buffer_size", cap(nn.eventCh),
	)

	return nil
}

// startEventLoop 使用 syncx.EventLoop 启动事件处理循环
// 内置 panic 恢复、context 取消优雅关闭、OnShutdown 回调
// 替代手写 select 循环 + recover，复用 go-toolbox 通用能力
// 消费完事件后调用 releaseEvent 归还到对象池，减少 GC 压力
func (nn *NATSNotifier) startEventLoop() {
	nn.wg.Add(1)
	go func() {
		defer nn.wg.Done()
		syncx.NewEventLoop(nn.workerCtx).
			OnChannel(nn.eventCh, func(event *ChangeEvent) {
				defer releaseEvent(event)
				nn.mu.RLock()
				handler := nn.handler
				nn.mu.RUnlock()
				if handler != nil {
					handler(event)
				}
			}).
			OnPanic(func(r interface{}) {
				nn.logger.ErrorKV("Event handler panic recovered",
					"panic", fmt.Sprintf("%v", r),
				)
			}).
			OnShutdown(func() {
				nn.logger.InfoKV("NATS event loop shutdown")
			}).
			Run()
	}()
}

// Unsubscribe 取消订阅
// 通过 workerCancel 通知 EventLoop 退出，等待剩余事件处理完成
func (nn *NATSNotifier) Unsubscribe() error {
	nn.mu.Lock()
	if !nn.running {
		nn.mu.Unlock()
		return nil
	}

	nn.running = false
	if nn.sub != nil {
		_ = nn.sub.Unsubscribe()
	}
	nn.mu.Unlock()

	// 通过 context 取消通知 EventLoop 退出并等待处理完剩余事件
	nn.workerCancel()
	nn.wg.Wait()

	// 重置 context 和 eventCh，支持后续重新 Subscribe
	nn.workerCtx, nn.workerCancel = context.WithCancel(context.Background())
	nn.eventCh = make(chan *ChangeEvent, nn.config.BufferSize)

	nn.logger.InfoKV("NATS notifier unsubscribed", "subject", nn.config.Channel)
	return nil
}

// handleMessage 处理 NATS 消息
// 二进制反序列化 + 来源过滤后投递到 eventCh，由 EventLoop 异步处理
// 非阻塞投递：channel 满时丢弃事件并告警（避免阻塞 NATS 消息投递 goroutine）
// 丢弃事件后，下一个全量事件（PolicyReload）会修复一致性
// 投递成功后由 EventLoop 消费时 releaseEvent 归还到对象池
func (nn *NATSNotifier) handleMessage(msg *nats.Msg) {
	event, err := UnmarshalEvent(msg.Data)
	if err != nil {
		nn.logger.WarnKV("Failed to unmarshal NATS event", "error", err.Error())
		return
	}

	if event.Source == nn.config.Source {
		nn.logger.DebugKV("Skipping self-published event",
			"event_type", string(event.Type),
			"source", event.Source,
		)
		releaseEvent(event)
		return
	}

	// 非阻塞投递到 channel，避免阻塞 NATS 消息投递
	select {
	case nn.eventCh <- event:
	default:
		// channel 满：丢弃事件，归还到池，等待下一个全量事件修复一致性
		releaseEvent(event)
		nn.logger.WarnKV("Event buffer full, dropping event",
			"event_type", string(event.Type),
			"source", event.Source,
			"buffer_size", cap(nn.eventCh),
		)
	}
}
