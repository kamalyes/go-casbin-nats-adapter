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
//   - 复用连接：使用外部传入的 NATS 连接，避免重复创建
//   - 消息去重：基于事件 ID 和来源节点过滤重复/自身事件
//   - 发布重试：发布失败时自动重试
//   - JetStream 可选：支持持久化消息（需要 NATS Server 启用 JetStream）
type NATSNotifier struct {
	conn   *nats.Conn             // NATS 连接（外部传入，不由本通知器管理生命周期）
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

// NewNATSNotifier 使用已有的 NATS 连接创建通知器
// 适用于复用全局 NATS 连接池的场景，避免重复创建连接
//
// 参数:
//   - conn: NATS 连接实例（必须，由调用方管理生命周期）
//   - js: JetStream 上下文（可选，传 nil 则不使用 JetStream）
//   - opts: 通知器配置选项（Channel、Source、BufferSize 等）
//
// 返回: NATSNotifier 实例或错误
func NewNATSNotifier(conn *nats.Conn, js nats.JetStreamContext, opts ...policy.NotifierOption) (*NATSNotifier, error) {
	if conn == nil {
		return nil, errors.NewPolicyAdapterFailedError("NATS connection is nil")
	}

	config := policy.DefaultNotifierConfig()
	for _, opt := range opts {
		opt(config)
	}

	if config.Source == "unknown" {
		config.Source = fmt.Sprintf("node-%s", idgen.NewIDGenerator(idgen.GeneratorTypeUUID).GenerateRequestID())
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
// 注意：不会关闭 NATS 连接本身，因为连接由外部管理
func (nn *NATSNotifier) Close() error {
	_ = nn.Unsubscribe()
	// 不关闭 conn，因为它是外部传入的，由外部管理生命周期
	return nil
}


