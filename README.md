# go-casbin-nats-adapter

基于 NATS 的策略变更通知适配器，用于分布式环境下的低延迟策略同步

## 功能特性

- ⚡ **超低延迟**：NATS 延迟在微秒级，适合高并发场景
- 🔄 **分布式策略同步**：A 节点修改策略后，通过 NATS 广播变更事件，B/C/D 节点自动重载
- 🔌 **复用外部连接**：使用外部传入的 NATS 连接，避免重复创建，由调用方管理连接生命周期
- 💾 **JetStream 可选**：启用 JetStream 后支持消息持久化
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
    "github.com/nats-io/nats.go"
)

func main() {
    // 1. 创建 NATS 连接（由调用方管理生命周期）
    nc, _ := nats.Connect("nats://localhost:4222")
    defer nc.Close()

    // 2. 可选：获取 JetStream 上下文（启用消息持久化）
    js, _ := nc.JetStream()

    // 3. 创建 NATS 通知器（传入已有连接，不负责创建/关闭连接）
    notifier, _ := natsadapter.NewNATSNotifier(
        nc,     // *nats.Conn（必须）
        js,     // nats.JetStreamContext（可选，传 nil 则不使用 JetStream）
        policy.WithChannel("casbin.policy.changes"),
        policy.WithSource("node-1"),
    )

    // 4. 创建执行器并集成 NATS 通知器
    e, _ := enforcer.NewEnforcer(
        enforcer.WithModelPath("resources/rbac_model.conf"),
        enforcer.WithPolicyPath("resources/rbac_policy.csv"),
        enforcer.WithNotifier(notifier),
    )
    defer e.Close()

    // 修改策略后自动通知其他节点
    _ = e.AddPolicy("alice", "data3", "read")
}
```

## 多租户使用

每个租户使用独立的 NATS Subject，实现策略变更通知隔离：

```go
// 所有租户共享同一个 NATS 连接
nc, _ := nats.Connect("nats://localhost:4222")
defer nc.Close()
js, _ := nc.JetStream()

// 租户1：独立 Subject
notifier1, _ := natsadapter.NewNATSNotifier(nc, js,
    policy.WithChannel("casbin.tenant1.policy.changes"),
    policy.WithSource("tenant1-node-1"),
)

e1, _ := enforcer.NewEnforcer(
    enforcer.WithModelPath("resources/rbac_with_domains_model.conf"),
    enforcer.WithPolicyPath("resources/rbac_with_domains_policy.csv"),
    enforcer.WithNotifier(notifier1),
)

// 租户2：独立 Subject + ABAC 规则策略
notifier2, _ := natsadapter.NewNATSNotifier(nc, js,
    policy.WithChannel("casbin.tenant2.policy.changes"),
    policy.WithSource("tenant2-node-1"),
)

e2, _ := enforcer.NewEnforcer(
    enforcer.WithModelPath("resources/abac_rule_model.conf"),
    enforcer.WithPolicyPath("resources/abac_rule_policy.csv"),
    enforcer.WithNotifier(notifier2),
)
```

## 复用连接池（推荐）

在微服务架构中，通常由网关层统一管理 NATS 连接池。`NATSNotifier` 接受外部传入的连接，天然支持复用：

```go
import (
    natsadapter "github.com/kamalyes/go-casbin-nats-adapter"
    gwglobal "github.com/kamalyes/go-rpc-gateway/global"
    "github.com/kamalyes/go-casbin/policy"
)

// 从网关连接池获取 NATS 连接
natsConn := gwglobal.GetNats()

var js nats.JetStreamContext
if natsConn.JetStream != nil {
    js = natsConn.JetStream
}

// 创建通知器，复用网关的 NATS 连接
notifier, _ := natsadapter.NewNATSNotifier(
    natsConn.Conn,  // 底层 *nats.Conn
    js,             // JetStream 上下文（可选）
    policy.WithChannel("casbin.policy.tenant123.changes"),
    policy.WithSource("access-control-node-1"),
)
```

## ABAC + NATS 低延迟分布式同步

ABAC 规则策略变更时，通过 NATS 微秒级广播到所有节点：

```go
nc, _ := nats.Connect("nats://localhost:4222")
js, _ := nc.JetStream() // 启用持久化，确保消息不丢失

notifier, _ := natsadapter.NewNATSNotifier(nc, js,
    policy.WithChannel("casbin.abac.policy.changes"),
)

e, _ := enforcer.NewEnforcer(
    enforcer.WithModelPath("resources/abac_rule_model.conf"),
    enforcer.WithPolicyPath("resources/abac_rule_policy.csv"),
    enforcer.WithNotifier(notifier),
)

// 添加 ABAC 规则策略 → 微秒级通知所有节点
_ = e.AddPolicy(`r.sub == "bob"`, "data2", "write")
```

## 配置说明

### NewNATSNotifier 参数

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `conn` | `*nats.Conn` | 是 | NATS 连接实例（由调用方管理生命周期） |
| `js` | `nats.JetStreamContext` | 否 | JetStream 上下文，传 `nil` 则不使用持久化 |
| `opts` | `...policy.NotifierOption` | 否 | 通知器配置选项 |

### NotifierConfig（通过 policy.NotifierOption 配置）

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `Channel` | `casbin:policy:changes` | NATS Subject 名称 |
| `Source` | 自动生成 | 本节点标识（用于消息去重，避免自消费） |
| `BufferSize` | `256` | 事件缓冲区大小 |
| `RetryInterval` | `1s` | 发布失败重试间隔 |
| `RetryCount` | `3` | 发布失败重试次数 |

### 连接管理说明

`NATSNotifier` **不负责** NATS 连接的创建和关闭：

- 连接由调用方创建和管理（如 `nats.Connect()` 或连接池）
- `Close()` 只取消订阅，不会关闭底层 `*nats.Conn`
- 这使得多个 Notifier 可以安全共享同一个连接

## 文件结构

```bash
go-casbin-nats-adapter/
├── config.go       # 配置类型与选项函数
├── constants.go    # 常量定义
├── notifier.go     # 核心结构体与构造函数
├── publish.go      # 事件发布逻辑
└── subscribe.go    # 事件订阅与消费逻辑
```

## License

Apache-2.0
