/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-03-28 00:00:00
 * @FilePath: \go-casbin\go-casbin-nats-adapter\notifier.go
 * @Description: NATS 通知器核心结构体与构造函数
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package natsadapter

import (
	"fmt"
	"sync"

	"github.com/kamalyes/go-casbin/errors"
	"github.com/kamalyes/go-casbin/policy"
	"github.com/kamalyes/go-logger"
	"github.com/kamalyes/go-toolbox/pkg/idgen"
	"github.com/kamalyes/go-toolbox/pkg/retry"
	"github.com/nats-io/nats.go"
)

// 编译时接口断言
var _ policy.PolicyNotifier = (*NATSNotifier)(nil)

// NATSNotifier 基于 NATS 的策略变更通知器
// 使用 NATS Subject 作为发布/订阅频道，实现分布式策略同步
// 适用于轻量级、低延迟的分布式部署场景
//
// 支持特性：
//   - 超低延迟：NATS 是高性能消息系统，延迟在微秒级
//   - 自动重连：NATS 连接断开后自动重试
//   - 消息去重：基于事件 ID 和来源节点过滤重复/自身事件
//   - 发布重试：发布失败时自动重试
//   - JetStream 可选：支持持久化消息（需要 NATS Server 启用 JetStream）
type NATSNotifier struct {
	conn   *nats.Conn             // NATS 连接
	js     nats.JetStreamContext  // JetStream 上下文（可选，用于持久化）
	sub    *nats.Subscription     // NATS 订阅对象
	config *policy.NotifierConfig // 通知器配置
	logger logger.ILogger         // 日志记录器
	idgen  idgen.IDGenerator      // ID 生成器
	retry  *retry.Retry           // 发布重试器

	mu      sync.RWMutex              // 保护以下字段
	running bool                      // 是否正在运行
	handler policy.ChangeEventHandler // 事件处理函数
}

// NewNATSNotifier 创建 NATS 通知器
func NewNATSNotifier(natsConfig *NATSConfig, opts ...policy.NotifierOption) (*NATSNotifier, error) {
	config := policy.DefaultNotifierConfig()
	for _, opt := range opts {
		opt(config)
	}

	if config.Source == "unknown" {
		config.Source = fmt.Sprintf("node-%s", idgen.NewIDGenerator(idgen.GeneratorTypeUUID).GenerateRequestID())
	}

	natsOpts := []nats.Option{
		nats.Name(config.Source),
		nats.ReconnectWait(2 * natsReconnectWait),
		nats.MaxReconnects(natsMaxReconnects),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {}),
		nats.ReconnectHandler(func(nc *nats.Conn) {}),
	}

	if natsConfig.Timeout > 0 {
		natsOpts = append(natsOpts, nats.Timeout(natsConfig.Timeout))
	}

	url := natsConfig.URL
	if url == "" {
		url = nats.DefaultURL
	}

	conn, err := nats.Connect(url, natsOpts...)
	if err != nil {
		return nil, errors.NewPolicyAdapterFailedError("failed to connect to NATS: " + err.Error())
	}

	var js nats.JetStreamContext
	if natsConfig.JetStream {
		js, err = conn.JetStream()
		if err != nil {
			conn.Close()
			return nil, errors.NewPolicyAdapterFailedError("failed to create JetStream context: " + err.Error())
		}

		_, err = js.StreamInfo(jetStreamName)
		if err != nil {
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     jetStreamName,
				Subjects: []string{config.Channel},
				Replicas: 1,
			})
			if err != nil {
				conn.Close()
				return nil, errors.NewPolicyAdapterFailedError("failed to create JetStream stream: " + err.Error())
			}
		}
	}

	return &NATSNotifier{
		conn:   conn,
		js:     js,
		config: config,
		logger: logger.NewEmptyLogger(),
		idgen:  idgen.NewIDGenerator(idgen.GeneratorTypeUUID),
		retry: retry.NewRetry().
			SetAttemptCount(config.RetryCount).
			SetInterval(config.RetryInterval),
	}, nil
}

// Close 关闭通知器
func (nn *NATSNotifier) Close() error {
	_ = nn.Unsubscribe()
	if nn.conn != nil {
		nn.conn.Close()
	}
	return nil
}
