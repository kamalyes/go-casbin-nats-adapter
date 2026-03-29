/*
 * @Author: kamalyes 501893067@qq.com
 * @Date: 2025-03-28 00:00:00
 * @LastEditors: kamalyes 501893067@qq.com
 * @LastEditTime: 2025-03-28 00:00:00
 * @FilePath: \go-casbin\go-casbin-nats-adapter\constants.go
 * @Description: NATS 适配器常量定义
 *
 * Copyright (c) 2025 by kamalyes, All Rights Reserved.
 */

package natsadapter

import "time"

const (
	natsReconnectWait = 2 * time.Second // NATS 重连等待时间
	natsMaxReconnects = 10              // NATS 最大重连次数
	jetStreamName     = "CASBIN_POLICY" // JetStream Stream 名称
)
