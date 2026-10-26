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
| `-cfst-dd` | `true` | 跳过 cfst 下载测速（默认开启）；设 `-cfst-dd=false` 可启用下载测速 |
| `-min-active-bytes` | `65536` | 段必须传输这么多字节才算成功 |
| `-min-active-span` | `2s` | 或数据必须持续流动至少这么久（有变化的流量） |
| `-dead-skip` | `60s` | 反复失败的 IP 跳过时长 |
| `-dead-after` | `2` | 连续失败多少次才拉黑 IP（稳定性优先） |
| `-max-per-ip` | `12` | 试用池单 IP 并发软上限（已验证 IP 不限；0=不限） |
| `-max-per-client` | `32` | 单客户端并发段硬上限（制止连接风暴；0=不限） |
| `-stall-timeout` | `30s` | 客户端发过数据但上游长时间无新字节 → 掐断半死连接（0=关闭） |
| `-stall-window` | `5m` | 仅检测连接初期（避免误伤长轮询等合法静默） |
| `-admin-addr` | `127.0.0.1:18080` | HTTP 管理接口（JSON 状态）；留空关闭 |
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

**只有活跃流量才是金标准。** 一段转发只有在"上游有回包、且流量有持续变化
（时间跨度 ≥ `-min-active-span`）或达到一定体积（≥ `-min-active-bytes`），且总字节 ≥8KB——慢速 TLS 握手不能冒充活跃流量"时才记为成功。
任何非活跃且非客户端过错的段——纯 TCP 静默、单个小应答（"只回一个 200"）、只有 TLS
握手没有数据、静默关闭、连接被拒——一律记失败**并立即拉黑** `-dead-skip` 时长，
下一个连接直接跳过，重试风暴不会堆积在死 IP 上。只有客户端自身过错为中性。
失败驱动连败惩罚（3 次分数 ×0.1）与累计淘汰（`-fail-evict` 次移出）；
新一轮 cfst 会解除死标记。速度 EMA 只由活跃段喂入。
选路是**两池模型**——cfst 扫回来的大部分 IP 不可用，少数能用的只有跑过真流量才知道，所以好 IP 是稀缺资源：

- **已验证池**：跑出过真实流量的 IP（`Verified` 永久生效，无时间窗口）——所有连接优先走这里，池内按并发数负载均衡
- **试用池**：其余全部，仅在已验证池不可用时轮换试用；试用 IP 一旦跑出真流量即晋升锁定。试用池受 `-max-per-ip` 约束（单个坏 IP 不能吸走整个池）客户端连上后一字节不发（`idle`）为中性——换 IP 救不了沉默的客户端。**半死路径会被掐断**：连接初期（`-stall-window` 内），客户端发过数据但上游超过 `-stall-timeout` 没有新字节 → 主动断开（记为 `stall` 失败），客户端立刻感知并重连到健康 IP，而不是无限等待。**稳定性优先**：IP 要连续 `-dead-after` 次失败判定才会被拉黑，长距离弱网络上的一次性抖动不会烧掉 IP、不会搅动池子。

### 流量：默认不下载测速

下载测速**默认关闭**（`-cfst-dd=true`）：每轮 cfst 只花几 KB，而不是 `-cfst-dl-count × url 大小`（约 30MB）。结果按延迟排序，实时吞吐（来自真实转发流量，EMA α=0.3）在几个连接后补上速度分——初始排名无论如何都是延迟单调的。唯一代价："能连不能下"的 IP 不再一次性过滤，由实时打分 + 淘汰在几个连接内清理。交互式（上网浏览）场景延迟优先通常更合适。

重度下载场景可用 `-cfst-dd=false` 重新开启下载测速；想控制其流量，调小 `-cfst-dl-count` 并用 `-cfst-url` 缩小下载体积（如 `bytes=300000`），每轮约 1MB。

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

## 致谢

- [XIU2/CloudflareSpeedTest](https://github.com/XIU2/CloudflareSpeedTest) —— 本项目的
  优选 IP 扫描工具。cforward 运行它来发现最优 Cloudflare IP；安卓 app 直接打包其
  官方发行版（二进制 + `ip.txt` + GPL-3.0 许可文本），开箱即用。该项目以
  GNU GPL v3.0 协议开源（第三方声明见 [NOTICE](NOTICE)），感谢 XIU2 及所有贡献者。

## License

[MIT](LICENSE)
