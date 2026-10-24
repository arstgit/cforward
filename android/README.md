# cforward-android

Android 壳：在手机上运行 [cforward](../)（Cloudflare 优选 IP 智能转发器）。

手机上的 TCP 客户端把目标地址填成 `127.0.0.1:<端口>`，流量经由 cforward
自动落到当前最优的 CF 优选 IP，断流自动切换。

## 构建

```sh
./android/build-apk.sh        # 输出 app/build/outputs/apk/debug/app-debug.apk
```

脚本自动完成：交叉编译 Go 核心（android/arm64）→ 打包 assets（cfst + ip.txt）→
Gradle 构建。需要 JDK 17+、Android SDK（首次自动下载）。

## 安装

1. 把 `android/app/build/outputs/apk/debug/app-debug.apk` 传到手机（侧载，需允许"安装未知应用"）
2. 打开 app → 点 **Start**（授予通知权限；前台服务常驻通知显示 "relaying"）
3. 首次启动自动跑 cfst 建池（约 1-2 分钟，仅几 KB 流量），期间状态页显示
   "pool empty, waiting for first cfst round…"

## 客户端配置

1. 客户端的目标地址填 `127.0.0.1`，端口 `1234`（app 内可改）
2. 若客户端以 VPN 模式接管全机流量：在其**应用排除/分流规则**里把 cforward
   **排除**，否则 cforward 的出站拨号会被接管造成环路
3. 手机设置里关闭 cforward 的电池优化（防止后台被杀）

## 界面

- **状态**：池子大小 / 已验证 IP 数 / 死跳过数 / 活跃连接 / cfst 状态 / Top8 IP（分数、延迟、速度），每 2 秒刷新（读 Go 核心的 `127.0.0.1:18080/status`）
- **设置**：监听端口、目标端口（默认 443）、池子下限、cfst 免下载测速（省流量，默认开）；Apply 后自动重启转发
- **日志**：Go 核心日志尾部（`filesDir/cforward.log`）

## 说明

- 免 root：监听 127.0.0.1、从 app 私有目录执行静态二进制
- 前台服务类型 `specialUse`（侧载不受 Play 时限限制）
- cfst 测速在手机本机执行 = 手机当前网络的最优选 IP，比服务器更准
- 池子/评分持久化在 app 数据目录，重启不丢
- 打包的 cfst 二进制与 `ip.txt` 来自
  [XIU2/CloudflareSpeedTest](https://github.com/XIU2/CloudflareSpeedTest)（MIT 协议），
  由 `build-apk.sh` 从官方 Release 自动拉取

## 已知风险

- 个别 OEM ROM 的 SELinux/杀后台策略严格，首次运行如进程起不来，看日志尾部
  （`filesDir/cforward.log`）确认 exec 是否被拒
- Android 15+ 电池限制需在系统设置里允许后台运行
