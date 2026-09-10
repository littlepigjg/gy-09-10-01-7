# API 网关限流熔断服务

一个基于 **Python + FastAPI** 的反向代理网关，提供多算法限流与自动熔断能力，内置可视化控制台，支持规则动态调整，限流规则与熔断状态持久化到 MySQL。**数据库已容器化打包，克隆即可一键运行。**

---

## 一、快速开始（推荐，一条命令）

前置条件：已安装 [Docker](https://docs.docker.com/engine/install/) 与 Docker Compose（Docker Desktop / Docker Engine 自带）。

```bash
# 1. 克隆项目
git clone <your-repo-url> gy-09-10-01
cd gy-09-10-01

# 2. 一键构建并启动（应用 + MySQL 全部容器化）
docker compose up -d --build

# 3. 查看状态，等待两个容器都变为 healthy
docker compose ps
```

启动完成后访问：

| 入口 | 地址 | 说明 |
|------|------|------|
| **控制台页面** | http://localhost:8888/ | 实时流量、限流事件、熔断状态、规则管理 |
| 实时统计 API | http://localhost:8888/api/stats | JSON 实时计数 |
| Dashboard API | http://localhost:8888/api/dashboard | 全量数据 |
| 反向代理入口 | http://localhost:8888/proxy/{service}/{path} | 经限流/熔断后转发 |

> MySQL 容器端口映射到宿主 **3307**（避免与本机已装的 MySQL 3306 冲突）；应用与数据库在容器内网通信，无需关心。

停止与清理：

```bash
docker compose down          # 停止并删除容器（保留数据卷）
docker compose down -v       # 同时清空数据库数据（恢复到全新初始化）
```

---

## 二、首次启动会自动完成什么

1. 启动 MySQL 8.0，账号 `root` / 密码 `root`，自动创建数据库 `ratelimiter`。
2. 首次启动自动执行 [sql/init.sql](sql/init.sql)，建表并写入 3 条示例限流规则、3 个示例后端熔断器。
3. 应用容器通过 [wait_for_db.py](wait_for_db.py) 轮询等待数据库就绪后再启动。
4. 应用启动时 SQLAlchemy 也会 `create_all` 兜底建表，即使不挂载 init.sql 也能正常运行。
5. 运行期间限流事件、流量统计、熔断状态由后台任务定时批量持久化到 MySQL。

---

## 三、目录结构

```
.
├── main.py                    # FastAPI 主应用：反向代理 + 管理 API + 后台持久化
├── config.py                  # 配置（数据库连接等，支持环境变量覆盖）
├── database.py                # 异步 MySQL 连接（SQLAlchemy + aiomysql）
├── models.py                  # ORM 模型（规则/熔断状态/事件/流量统计）
├── wait_for_db.py             # 容器入口：等待 MySQL 就绪
├── requirements.txt
├── Dockerfile                 # 应用镜像
├── docker-compose.yml         # 应用 + MySQL 一键编排
├── sql/
│   └── init.sql               # 数据库初始化脚本（MySQL 容器自动执行）
├── rate_limiter/
│   ├── base.py                # 限流器基类与结果对象
│   ├── token_bucket.py        # 令牌桶
│   ├── sliding_window.py      # 滑动窗口（时间槽）
│   ├── fixed_window.py        # 固定窗口
│   └── manager.py             # 规则匹配与多算法分发
├── circuit_breaker/
│   ├── state_machine.py       # 熔断器状态机 CLOSED/OPEN/HALF_OPEN
│   └── manager.py             # 多服务熔断管理
├── archive/
│   ├── codec.py               # (path,hour) 压缩块：聚合/zlib 编解码/时间范围下钻
│   └── archiver.py            # 归档扫描、块 upsert、事务确认后删除原始行
├── tests/
│   └── test_archive_codec.py  # 压缩块与并发扫描不变量测试（仅标准库）
└── frontend/
    ├── index.html             # 控制台页面（Chart.js 实时图表）
    └── archives.html          # 冷热归档分析页（压缩比 + 区间下钻）
```

---

## 四、核心功能

### 限流算法

| 算法 | 实现文件 | 特点 |
|------|----------|------|
| 令牌桶 Token Bucket | [token_bucket.py](rate_limiter/token_bucket.py) | 按时间匀速补充令牌，支持突发；key 级 `RLock` 保证高并发计数准确，惰性清理空桶 |
| 滑动窗口 Sliding Window | [sliding_window.py](rate_limiter/sliding_window.py) | `OrderedDict` 时间槽，自动淘汰窗口外旧槽，内存可控 |
| 固定窗口 Fixed Window | [fixed_window.py](rate_limiter/fixed_window.py) | 按固定时间片计数，简单高效 |

规则按路径匹配（精确匹配优先，其次最长通配符匹配，支持 `*`、`?`）。

### 熔断状态机

```
            失败次数 >= 阈值
  CLOSED ───────────────────────► OPEN
     ▲                              │
     │ 半开试探成功 >= 阈值          │ 等待 recovery_timeout
     │                              ▼
     └──────────────────────── HALF_OPEN
                  │ 试探失败
                  └──────────────► OPEN（重新熔断）
```

- **CLOSED**：正常放行；连续失败达阈值转为 OPEN。
- **OPEN**：直接拒绝（返回 503）；超时后自动转 HALF_OPEN。
- **HALF_OPEN**：仅放行 `half_open_max_calls` 个试探请求；成功数达阈值恢复 CLOSED，任一失败回到 OPEN。

实现见 [state_machine.py](circuit_breaker/state_machine.py)。

### 控制台（http://localhost:8888/）

- 总请求 / 通过 / 拒绝 / 熔断数实时卡片
- 实时流量趋势图（总/通过/拒绝三条曲线，3 秒刷新）
- 限流规则的新建、编辑、删除（即时生效）
- 最近限流事件日志
- 熔断器状态卡片：状态、失败计数、半开试探数，支持改配置与一键重置

### rate_limit_events 冷热分层归档

热表 `rate_limit_events` 只保留近 **72h** 明细（`EVENT_RETENTION_HOURS` 可调），
后台每 10 分钟（`ARCHIVE_RUN_INTERVAL`）扫描一次，将超窗事件按 **(path, 整点小时桶)**
聚合为 zlib 压缩块写入新表 `rate_limit_event_archives`，同事务提交确认后再删除原始行。

- **压缩块内容**：分钟级 total/allowed/rejected/平均速率（稀疏存储）、算法/拒绝原因计数、
  Top 50 客户端 IP（高基数截断并打 `ipc` 标记）。实测 50 万行约 45MB 明细 → 0.6MB 块（~72x）。
- **不重不漏的并发保证**：归档水位线对齐完整小时且远早于当前时间，persist_events 并发批量
  插入的新行 `created_at ≈ now` 永远在窗口内、不会被选中；删除只针对本事务快照收集到的
  `tmp_archived_event_ids` 主键集合；块写入与删除同一事务，失败整轮回滚。
- **幂等**：`(path, hour_bucket)` 唯一约束 + `INSERT ... ON DUPLICATE KEY UPDATE`，
  重跑时与存量块做载荷级合并。
- **性能**：`created_at` 索引 + 主键 keyset 分批扫描（每批 2 万行），删除走临时表 JOIN
  主键分批执行（每批 5000），单轮 50 万行目标 <30s；`last_run` 记录分阶段耗时与行数。
- **时间范围下钻**：压缩块按分钟裁剪，可还原任意时间范围的近似分布；
  跨度 ≤6h / ≤3d / 更长分别自动按分钟 / 5 分钟 / 小时聚合，边界粗粒度桶标记 `partial`。
- **归档页**：[http://localhost:8888/archives](http://localhost:8888/archives)
  展示热表行数、整体/各路径压缩比、每个路径的可下钻区间，支持手动触发一轮归档与区间下钻图表。

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `EVENT_RETENTION_HOURS` | 72 | 热数据保留窗口（小时） |
| `ARCHIVE_RUN_INTERVAL` | 600 | 后台归档扫描间隔（秒） |
| `ARCHIVE_BATCH_ROWS` | 500000 | 单轮归档最大行数 |
| `ARCHIVE_SCAN_CHUNK` | 20000 | 扫描批大小 |
| `ARCHIVE_DELETE_CHUNK` | 5000 | 删除批大小 |

---

## 五、HTTP API 一览

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 控制台页面 |
| GET | `/api/dashboard` | 控制台全量数据 |
| GET | `/api/stats` | 实时计数 |
| GET/POST | `/api/rules` | 列出 / 新建限流规则 |
| PUT/DELETE | `/api/rules/{id}` | 更新 / 删除规则 |
| GET/POST | `/api/circuit-breakers` | 列出 / 新建熔断器 |
| PUT/DELETE | `/api/circuit-breakers/{name}` | 更新配置 / 删除 |
| POST | `/api/circuit-breakers/{name}/reset` | 手动重置为正常 |
| GET | `/archives` | 冷热归档分析页 |
| GET | `/api/archives` | 归档概览：各路径压缩比、可下钻区间、最近一轮耗时 |
| POST | `/api/archives/run` | 手动触发一轮归档（与后台任务互斥，执行中返回 409） |
| GET | `/api/archives/drill?path=&start=&end=&granularity=` | 按时间范围下钻还原近似分布 |
| ALL | `/proxy/{service}/{path}` | 反向代理（先限流后熔断再转发） |

下钻示例（naive ISO 时间；granularity 可省略以自动选择 minute/5min/hour）：

```bash
curl "http://localhost:8888/api/archives/drill?path=/api/*&start=2026-09-01T00:00:00&end=2026-09-01T06:00:00&granularity=5min"
```

新建规则示例：

```bash
curl -X POST http://localhost:8888/api/rules \
  -H "Content-Type: application/json" \
  -d '{"path":"/api/order/*","algorithm":"sliding_window","rate":50,"burst":50,"window_size":60}'
```

通过代理访问后端（熔断器中需配置同名 service 与 backend_url）：

```bash
curl http://localhost:8888/proxy/user-service/api/users/1
# 超限返回 429；后端熔断返回 503；后端不通返回 502/504
```

---

## 六、本地开发运行（不用 Docker）

需要 Python 3.11+ 与本地 MySQL 8.0（root 密码 root，或用环境变量覆盖）。

```bash
pip install -r requirements.txt

# 初始化数据库（会自动建库建表；也可手动执行 sql/init.sql）
mysql -uroot -proot < sql/init.sql

python main.py        # 默认监听 0.0.0.0:8888
```

支持的环境变量：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DB_HOST` | `127.0.0.1` | 数据库地址 |
| `DB_PORT` | `3306` | 数据库端口 |
| `DB_USER` | `root` | 用户名 |
| `DB_PASSWORD` | `root` | 密码 |
| `DB_NAME` | `ratelimiter` | 库名 |

---

## 七、常见问题

**1. `docker compose up` 拉取 `python` / `mysql` 镜像超时？**

国内网络访问 Docker Hub 可能较慢。两种办法：

- 为 Docker 配置镜像加速器（`/etc/docker/daemon.json`）：

  ```json
  { "registry-mirrors": ["https://docker.1ms.run"] }
  ```

  改后执行 `sudo systemctl restart docker`。

- 或先手动拉取加速镜像并打标准标签：

  ```bash
  docker pull docker.1ms.run/library/python:3.12-slim
  docker tag  docker.1ms.run/library/python:3.12-slim python:3.12-slim
  docker pull docker.1ms.run/library/mysql:8.0
  docker tag  docker.1ms.run/library/mysql:8.0 mysql:8.0
  ```

  本地已有镜像后，如遇 BuildKit 仍去联网校验元数据，可使用经典构建器：

  ```bash
  DOCKER_BUILDKIT=0 docker build -t gy-09-10-01-app .
  docker compose up -d --no-build
  ```

**2. 端口被占用？**

应用端口改 `docker-compose.yml` 中 `app.ports`（如 `"9000:8888"`）；MySQL 宿主端口默认已是 3307。

**3. 想恢复初始数据？**

```bash
docker compose down -v && docker compose up -d --build
```

**4. MySQL 8 认证报错 `'cryptography' package is required ...`？**

已在 [requirements.txt](requirements.txt) 中包含 `cryptography` 依赖（用于 `caching_sha2_password` 认证），重新构建镜像即可。

---

## 八、技术栈

- **Web 框架**：FastAPI + Uvicorn（异步高并发）
- **数据库**：MySQL 8.0 + SQLAlchemy 2.0（async）+ aiomysql
- **HTTP 转发**：httpx AsyncClient（连接池）
- **并发控制**：线程级 `RLock` 保证计数原子性，`OrderedDict` / deque 管理有限内存
- **前端**：原生 HTML/CSS/JS + Chart.js（CDN）
- **部署**：Docker + Docker Compose
