# cforward

English README: [README.md](README.md)。

Cloudflare 优选 IP 端口转发器（Go）。

监听 `0.0.0.0:1234`，把 TCP 流量转发到 Cloudflare 优选 IP 的 443 端口。
优选 IP 由 [CloudflareSpeedTest (cfst)](https://github.com/XIU2/CloudflareSpeedTest)
扫描产生；转发过程中持续监控每个 IP 的延迟、速度、成功率、重试率并打分；
上游断流时**保持客户端连接**，立刻换下一个最高分 IP 重拨继续转发。

## 使用

```sh
# cfst 可执行文件及 ip.txt 位于 ./cfst/（已被 .gitignore 排除，需自行放入）
go build -o cforward .
./cforward
```

首次启动会自动执行 cfst 建池（约 1-2 分钟）；期间到达的客户端连接会被拒绝。
池子持久化在 `pool.json`，重启后无需等待第一轮 cfst 即可转发。

### 配置

所有配置项都支持 **命令行 flag** 或 **JSON 配置文件**（`-config`），优先级：
命令行 flag > 配置文件 > 默认值。

```sh
./cforward -config config.json
# 或混合：配置文件为主，个别项用 flag 覆盖
./cforward -config config.json -pool-min 50 -debug
```

配置文件示例（`config.example.json`，缺省字段用默认值）：

```json
{
  "listen": ":1234",
  "target_port": 443,
  "cfst_dir": "./cfst",
  "pool_file": "pool.json",
  "pool_min": 100,
  "pool_max": 200,
  "cooldown": "5m",
  "fail_evict": 5,
  "dial_timeout": "3s",
  "no_data_timeout": "10s",
  "cfst_url": "https://speed.cloudflare.com/__down?bytes=3000000",
  "cfst_threads": 100,
  "cfst_tests": 2,
  "cfst_dl_count": 10,
  "cfst_dl_time": 10,
  "cfst_top": 20,
  "status_interval": "60s",
  "debug": false
}
```

### 参数

| Flag | 默认 | 说明 |
|---|---|---|
| `-config` | | JSON 配置文件路径（命令行 flag 优先级更高） |
| `-listen` | `:1234` | 监听地址 |
| `-cfst-dir` | `./cfst` | cfst 工作目录（含 ip.txt / ipv6.txt） |
| `-cfst-path` | `<cfst-dir>/cfst` | cfst 可执行文件路径 |
| `-target-port` | `443` | 转发目标端口（CF IP 上） |
| `-pool-file` | `pool.json` | 池子持久化文件 |
| `-pool-min` | `100` | 池内 IP 低于此数触发补池 |
| `-pool-max` | `200` | 池子上限，超出踢最低分 |
| `-cooldown` | `5m` | 补池完成后不足时，下次补池的最小间隔 |
| `-fail-evict` | `5` | 累计失败达到此数移出池子 |
| `-dial-timeout` | `3s` | 拨号超时 |
| `-no-data-timeout` | `10s` | 路径死亡判定时长（TCP_USER_TIMEOUT） |
| `-cfst-url` | `https://speed.cloudflare.com/__down?bytes=3000000` | cfst 下载测速地址 |
| `-cfst-threads` | `100` | cfst 延迟测速线程数 (`-n`) |
| `-cfst-tests` | `2` | cfst 每 IP 测速次数 (`-t`) |
| `-cfst-dl-count` | `10` | cfst 下载测速数量 (`-dn`) |
| `-cfst-dl-time` | `10` | cfst 单 IP 下载测速时长上限秒 (`-dt`) |
| `-cfst-top` | `20` | cfst 结果入池数量 (`-p`) |
| `-status-interval` | `60s` | 状态块打印间隔 |
| `-debug` | `false` | debug 日志 |

## 行为

### 转发与切换

- 每个客户端连接独立处理；拨号时按分数降序尝试池内 IP，失败立即降级下一个（同连接内失败过的 IP 不再重试）。
- 上游连接设置 **TCP keepalive + TCP_USER_TIMEOUT**：路径真实死亡（数据/保活探测无 ACK）时内核在约 `-no-data-timeout` 内让读写报错；活路径上的空闲连接不会误杀。
- 上游断流（RST / 超时）→ 保持客户端连接，立刻换下一个最高分 IP 重拨续传，客户端无感（断流瞬间已读走未送达的字节会丢，应用层重发机制可覆盖）。
- 上游正常关闭（FIN）→ 视为会话结束，关闭客户端连接（客户端自行重连）。

### 计分

四项指标归一化到 0–100，总分 `0.4L + 0.3S + 0.2U + 0.1R`：

- L 延迟分 = `100 × (1 − 延迟ms/1000)`，延迟取拨号耗时
- S 速度分 = `100 × (1 − e^(−速度MB/s÷20))`
- U 成功率 = 成功拨号且传输未失败 ÷ 总拨号次数 × 100
- R 重试分 = `(1 − 重试率) × 100`，重试归因到连接最终使用的 IP

- 入池初始分由 cfst 的平均延迟/下载速度代入（U、R 按 1.0 计）
- 实时统计以 EMA（α=0.3）滚动更新；实时样本 ≥5 后完全接管初始分，之前按样本数加权过渡
- 连续失败 3 次 → 分数 ×0.1；累计失败 `-fail-evict` 次 → 移出池子
- 连续 3 轮 cfst 未出现且无实时样本的 IP → 移出

### 补池（事件驱动，无定时器）

满足任一条件触发一轮 cfst：

1. 启动时
2. 池内 IP < `-pool-min`
3. 池内最高分 < 50（数量够但全是烂 IP 也刷新）

一轮完成后仍不足则至少冷却 `-cooldown` 再补；cfst 在跑时不重复触发。

cfst 参数：`-n 100 -t 2 -dn 10 -dt 10 -p 20 -tlr 0 -o ""`（前 20 名入池，丢包 IP 过滤，结果从 stdout 解析）。

## 日志

- 关键事件用 `slog` 输出（默认 info 级别，`-debug` 开 debug）
- 每 `-status-interval`（默认 60s）打印一次**状态块**：池子大小/水位、活跃连接数、Top10 表格（分数/延迟/速度）

```
── cforward 状态 ────────────────────────────────
  pool: 20 IPs (min 100 / max 200)    活跃连接: 18
  #    IP                    分数      延迟           速度
  1    172.64.229.241       69.7     72ms    1.8MB/s
  2    104.18.32.170        69.3     80ms    1.7MB/s
```
