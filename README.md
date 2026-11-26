# duckport-binance-listing

把“币安 USDT 永续合约对应的币有没有现货”记成一张事件表 `data.binance_listing`，供回测和实盘按时间对齐使用。只在状态变化时写一行。

需要 duckport-server ≥ 1.2.0，并且同一台机器上有 binance-ingestor 在写 `data.exginfo`（合约名单从那里读）。

## 表结构

| 列 | 类型 | 含义 |
|---|---|---|
| `known_at` | TIMESTAMP | 从这一刻起可用：采样到变化的那次采样时刻。按这一列对齐，也是水位的时间列 |
| `event_time` | TIMESTAMP | 实际发生时刻：上架是现货第一根 1 分钟 K 线的开盘时间，下架是最后一根的开盘时间。查不到时为 NULL |
| `market` | VARCHAR | 目前只有 `spot` |
| `symbol` | VARCHAR | USDT 永续合约名，如 `1000SHIBUSDT` |
| `venue_symbol` | VARCHAR | 触发这次变化的现货交易对，如 `SHIBUSDT` |
| `state` | TINYINT | `1` 现货在交易，`2` 现货已下架。没有任何行 = 从没上过现货（`0`，不写行） |
| `source` | VARCHAR | `live`（实盘采样）或 `backfill`（历史重建） |

无主键，按 `(market, symbol, known_at)` 去重。取某一时刻的状态：`known_at <= t` 的最后一行。

**不要给这张表设 retention。** 它是事件表，删掉旧行就丢了状态。

## “有现货”的口径

- 合约的币只要有一个现货交易对在交易就算（任意计价）。只认部分计价时设 `SPOT_QUOTES=USDT`，历史重建和实盘采样要用同一个值。
- 合约和现货按名字对应：同名优先，其次去掉 `1000` / `1000000` / `1M` 前缀，另有少量别名（`LUNA2→LUNA`、`DODOX→DODO`、`RAYSOL→RAY`，在 `mapping.py`）。
- 只按名字对应。名字被新币复用的合约（如 `LITUSDT`）会对到同名的旧现货，表里照实记录。

## 实盘采样

每 5 分钟一次，默认在 `hh:02:30`、`hh:07:30`……（`SAMPLE_OFFSET_SECONDS=150`），避开 K 线收盘时刻。每次：

1. 拉现货 `exchangeInfo?symbolStatus=TRADING`（权重 20，每小时 240）。
2. 从 `data.exginfo` 读当前 USDT 永续名单，对应到现货的币。
3. 和表里每个合约的最后一行比较，有变化写一行，`known_at` = 本次采样时刻。
   - 上架（含下架后重新上架）：查一次 1 分钟 K 线填 `event_time`（权重 2）。
   - 下架：连续两次采样都看不到才写。
4. 不论有没有变化都推进水位，事件和水位在同一个事务里。

以下情况整次跳过，不写行、不推进水位，下一个 5 分钟再来：

- 现货名单拉不到；
- 原来有现货的合约里，还在名单中的不到 80%（交易所维护、返回被截断）；
- `data.exginfo` 里没有 USDT 永续；
- 采样时刻过去 90 秒还没写完（`known_at` 不能早于真正知道的时间）。

水位长时间不动就是插件没在正常工作，`duckport status` 会按 `WATERMARK_MAX_LAG_SECONDS`（默认 900）报超时。

## 历史重建

```bash
sudo duckport loadhist <实例>      # 会先停实例
sudo duckport start <实例>
```

第一次启动采样前先跑一次，否则现有的现货都会记成“刚刚才知道”。

做的事：取现货全部交易对（含已下架的）、每个交易对的首末 1 分钟 K 线、vision 上出现过的全部 USDT 永续，算出每个合约的上下架事件。`known_at` 模拟实盘：上架取 `event_time` 之后的第一个采样点（10:00 开盘 → 10:02:30），下架再晚一个采样点（最后一根 02:59 → 03:07:30），回测不会比实盘更早知道。

- 可以反复跑：每次把 `backfill` 行整批删掉重写，在一个事务里完成；`live` 行不动。
- 只重建第一次实盘采样（记在 `data.config_dict` 的 `binance_listing:live_since`）之前的历史，之后的归实盘。
- 约 2,900 个交易对、5,000 多次权重 2 的请求，经代理约 10 分钟；已用权重过 3000 会等到下一分钟。
- 同一个交易对中途停牌又恢复的空档看不到（只查首末两根 K 线）。
- 直接运行 `loadhist --dry-run` 只计算并打印，不写入。

## 配置

| 变量 | 默认 | 说明 |
|---|---|---|
| `PROXY_URL` | 空 | HTTP 代理 |
| `SPOT_QUOTES` | 空 | 算“有现货”的计价资产，逗号分隔；空 = 任意 |
| `SAMPLE_OFFSET_SECONDS` | `150` | 采样点相对 5 分钟整点的偏移 |
| `WATERMARK_MAX_LAG_SECONDS` | `900` | 水位超时阈值 |

## 开发

```bash
PYTHONPATH=. python -m pytest tests
```

测试用进程内的 DuckDB 执行真实的写入 SQL，不连 duckport-server，也不访问币安。
