# go-casbin-nats-adapter

基于 NATS 的策略变更通知适配器，用于分布式环境下的低延迟策略同步

## 功能特性

- ⚡ **超低延迟**：NATS 延迟在微秒级，适合高并发场景
- 🔄 **分布式策略同步**：A 节点修改策略后，通过 NATS 广播变更事件，B/C/D 节点自动重载
- 💾 **JetStream 可选**：启用 JetStream 后支持消息持久化
- 🔁 **自动重连**：NATS 连接断开后自动重试
- 🚫 **消息去重**：基于事件 ID 和来源节点过滤重复/自身事件
- 🔁 **发布重试**：发布失败时自动重试

## 安装

```bash
go get github.com/kamalyes/go-casbin-nats-adapter
```

## 基本使用

```go
package main

import (
    "github.com/kamalyes/go-casbin-nats-adapter"
    "github.com/kamalyes/go-casbin/enforcer"
    "github.com/kamalyes/go-casbin/policy"
    "github.com/kamalyes/go-logger"
)

func main() {
    log := logger.NewLogger().WithLevel(logger.INFO)

    // 创建 NATS 通知器
    notifier, _ := natsadapter.NewNATSNotifier(
        &natsadapter.NATSConfig{
            URL:       "nats://localhost:4222",
            JetStream: true,
        },
        policy.WithChannel("casbin.policy.changes"),
        policy.WithSource("node-1"),
    )

    // 创建执行器并集成 NATS 通知器
    e, _ := enforcer.NewEnforcer(
        enforcer.WithModelPath("resources/rbac_model.conf"),
        enforcer.WithPolicyPath("resources/rbac_policy.csv"),
        enforcer.WithNotifier(notifier),
        enforcer.WithLogger(log),
    )
    defer e.Close()

    // 修改策略后自动通知其他节点
    _ = e.AddPolicy("alice", "data3", "read")
}
```

## 多租户使用

每个租户使用独立的 NATS Subject，实现策略变更通知隔离：

```go
// 租户1：独立 Subject
notifier1, _ := natsadapter.NewNATSNotifier(
    &natsadapter.NATSConfig{
        URL:       "nats://localhost:4222",
        JetStream: true,
    },
    policy.WithChannel("casbin.tenant1.policy.changes"),
    policy.WithSource("tenant1-node-1"),
)

e1, _ := enforcer.NewEnforcer(
    enforcer.WithModelPath("resources/rbac_with_domains_model.conf"),
    enforcer.WithPolicyPath("resources/rbac_with_domains_policy.csv"),
    enforcer.WithNotifier(notifier1),
    enforcer.WithLogger(log),
)

// 租户2：独立 Subject + ABAC 规则策略
notifier2, _ := natsadapter.NewNATSNotifier(
    &natsadapter.NATSConfig{
        URL:       "nats://localhost:4222",
        JetStream: true,
    },
    policy.WithChannel("casbin.tenant2.policy.changes"),
    policy.WithSource("tenant2-node-1"),
)

e2, _ := enforcer.NewEnforcer(
    enforcer.WithModelPath("resources/abac_rule_model.conf"),
    enforcer.WithPolicyPath("resources/abac_rule_policy.csv"),
    enforcer.WithNotifier(notifier2),
    enforcer.WithLogger(log),
)
```

## ABAC + NATS 低延迟分布式同步

ABAC 规则策略变更时，通过 NATS 微秒级广播到所有节点：

```go
notifier, _ := natsadapter.NewNATSNotifier(
    &natsadapter.NATSConfig{
        URL:       "nats://localhost:4222",
        JetStream: true,  // 启用持久化，确保消息不丢失
    },
    policy.WithChannel("casbin.abac.policy.changes"),
)

e, _ := enforcer.NewEnforcer(
    enforcer.WithModelPath("resources/abac_rule_model.conf"),
    enforcer.WithPolicyPath("resources/abac_rule_policy.csv"),
    enforcer.WithNotifier(notifier),
    enforcer.WithLogger(log),
)

// 添加 ABAC 规则策略 → 微秒级通知所有节点
_ = e.AddPolicy(`r.sub == "bob"`, "data2", "write")
```

## 配置说明

### NATSConfig

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `URL` | `string` | `nats://localhost:4222` | NATS 服务器地址 |
| `Name` | `string` | 自动生成 | 客户端名称 |
| `JetStream` | `bool` | `false` | 是否启用 JetStream 持久化 |
| `Timeout` | `time.Duration` | 默认 | 连接超时 |

### NotifierConfig（通过 policy.NotifierOption 配置）

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `Channel` | `casbin:policy:changes` | NATS Subject 名称 |
| `Source` | 自动生成 | 本节点标识 |
| `BufferSize` | `256` | 事件缓冲区大小 |
| `RetryInterval` | `1s` | 发布失败重试间隔 |
| `RetryCount` | `3` | 发布失败重试次数 |

## 文件结构

```
go-casbin-nats-adapter/
├── config.go       # 配置类型与选项函数
├── constants.go    # 常量定义
├── notifier.go     # 核心结构体与构造函数
├── publish.go      # 事件发布逻辑
└── subscribe.go    # 事件订阅与消费逻辑
```

## License

Apache-2.0
