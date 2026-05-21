# 深度价值成长策略_7 三市场代码逻辑梳理

本文梳理 `http://192.168.1.12:38080/stock-screener` 页面执行 `深度价值成长策略_7` 时，在 A股、H股、USA 三个市场下的：

- 页面/API入口
- 核心业务代码链路
- 市场差异点
- 核心函数入口

---

## 公共入口

### 页面入口
文件：`src/stock_analyse/interfaces/web/routes/misc.py`

```python
@app.route('/stock-screener', methods=['GET'])
def stock_screener():
    return render_template(
        'stock_screener.html',
        strategies=_get_stock_selection_strategies(),
        default_market='SH',
        default_strategy='1',
    )
```

这个页面本身只负责渲染 UI，不直接执行选股。

### 前端调用的核心 API
文件：`src/stock_analyse/interfaces/web/routes/analysis.py`

```python
@app.route('/api/select_stock', methods=['GET', 'POST'])
def select_stock():
```

前端提交 `market` 和 `strategy=7` 后，真正执行：

```python
context.analyzer.stock_select_process(strategy_code, market)
```

### 选股业务总入口
文件：`src/stock_analyse/interfaces/web/services/stock_analyzer_service.py`

```python
def stock_select_process(self, strategy_code, market):
    json_result = run_stock_selection_use_case.execute(market=market, strategy_code=strategy_code)
```

### UseCase 入口
文件：`src/stock_analyse/application/use_cases/run_stock_selection.py`

```python
def execute(market: str, strategy_code: str, orchestrator: StockSelectionOrchestrator | None = None) -> dict:
```

内部执行：

```python
file_utils, high_score_stocks = orchestrator.run_web_selection(market=market, strategy_type=strategy_type)
```

### 编排器入口
文件：`src/stock_analyse/application/orchestrators/stock_selection_orchestrator.py`

```python
def run_web_selection(self, market: str, strategy_type: int = 1):
    file_utils, high_score_stocks = self._get_full_market_scan_workflow().run(
        market=market,
        strategy_type=strategy_type,
        batch_size=20,
        strategy_filter='avg',
    )
```

### 全市场扫描主入口
文件：`src/stock_analyse/application/workflows/full_market_scan_workflow.py`

```python
def run(self, *, market: str, strategy_type: int = 1, batch_size: int = 20, strategy_filter: str = 'avg', ...):
```

核心两步：

```python
df_stocks_data = self._get_scan_candidates(runtime=runtime, strategy_filter=strategy_filter)
results = self.scan_stock(runtime, batch_size=batch_size, df_stocks_data=df_stocks_data)
```

---

## 一、A股 `SH / SZ`

### 1. A股候选股票入口
文件：`src/stock_analyse/application/workflows/full_market_scan_workflow.py`

```python
df_stocks_data = self.get_all_stocks(market=runtime.market)
df_selected = runtime.selector.select_stock(df_stocks_data, strategy_type=runtime.strategy_type, strategy_filter=strategy_filter)
```

#### A股股票池获取
文件：`src/stock_analyse/application/workflows/full_market_scan_workflow.py`

```python
def get_all_stocks(self, *, market: str) -> pd.DataFrame:
```

A股分支：

```python
if normalized_market in {'SH', 'SZ'}:
    df_stock = stock.get_stock_spot()
```

#### A股实时行情接口
文件：`src/stock_analyse/infrastructure/services/market_data_service.py`

```python
def get_stock_spot(self):
```

A股优先：

- `ak.stock_zh_a_spot_em()`
- 失败回退 `ak.stock_zh_a_spot()`

### 2. 策略 7 的业务入口
文件：`src/stock_analyse/domain/strategies/stock_select_strategy.py`

```python
elif strategy_type == 7:
    return self.deep_value_growth_strategy(df_stock)
```

继续进入：

文件：`src/stock_analyse/domain/strategies/selection_strategy_service.py`

```python
def deep_value_growth_strategy(...)
```

这是 `深度价值成长策略_7` 的核心实现。

### 3. A股策略 7 的核心逻辑

#### 第一步：逐只股票进入 `_worker`
文件：`selection_strategy_service.py`

```python
def _worker(row_tuple):
```

#### 第二步：初筛
提取：

- `PE_TTM / PE / 市盈率-动态`
- `总市值`

```python
pe_val = _get_frame_val(row, ['PE_TTM', 'PE_静态', '市盈率-动态', '市盈率', 'PE', 'pe_ttm'], -1)
mkt_cap = _get_frame_val(row, ['总市值', '市值', 'market_val'], 0)
```

A股市值门槛：

```python
is_valid_cap = mkt_cap >= 5e8 or mkt_cap <= 0
```

#### 第三步：获取财务快照
文件：`selection_strategy_service.py`

```python
df_financial = selector.stock.get_stock_border_financial_indicator(
    market=market, df_stock_spot=pd.DataFrame([row])
)
```

#### A股财务主入口
文件：`src/stock_analyse/infrastructure/services/market_data_service.py`

```python
def get_stock_border_financial_indicator(self, market="H", date='20240331', indicator='年报', df_stock_spot=None):
```

这里会做：

- 拉取 5 年财务指标
- 计算 `净利润_TTM`
- 计算 `每股收益_TTM`
- 计算 `PE_TTM`
- 统一 `ROE`
- 映射 `总市值 / 最新价`

#### 第四步：回填标准字段
文件：`selection_strategy_service.py`

回填到 `s_data`：

- `ROE`
- `净利润同比增长率`
- `营业总收入同比增长率`
- `资产负债率`
- `PE_TTM`

#### 第五步：计算四个核心业务字段
文件：`src/stock_analyse/domain/services/stock_strategy_service.py`

```python
def calculate_stock_data(self, df_history_data, df_stock_data, stock_code, df_financial=None):
```

内部生成：

- `行业`
- `股票类型分类`
- `五阶段判断模型`
- `四区价格分区`

#### 第六步：评分
文件：`stock_strategy_service.py`

```python
def calculate_score(self, df_history_data, df_stock, df_summary_data):
```

分数由以下组成：

- 阶段 x 分区矩阵
- 类型溢价
- 技术面分数

### 4. A股核心 API / 函数入口总结

#### API
- `/stock-screener`
- `/api/select_stock`

#### 主要函数入口
- `StockAnalyzerService.stock_select_process`
- `run_stock_selection.execute`
- `StockSelectionOrchestrator.run_web_selection`
- `FullMarketScanWorkflow.run`
- `StockSelectStrategy.select_stock`
- `SelectionStrategyService.deep_value_growth_strategy`
- `stockBorderInfo.get_stock_border_financial_indicator`
- `StockStrategy.calculate_stock_data`
- `StockStrategy.calculate_score`

---

## 二、H股 `H / HK`

H股整体链路与 A股一致，但股票池来源、财务来源、行业来源不同。

### 1. H股候选股票入口
文件：`src/stock_analyse/application/workflows/full_market_scan_workflow.py`

```python
else:
    df_stock = stock.get_stock_border_info()
```

H股不是只拿 spot，而是拿增强后的聚合股票信息。

### 2. H股行情来源
文件：`market_data_service.py`

```python
elif self.market == 'H' or self.market == 'HK':
    stock_df = ak.stock_hk_main_board_spot_em()
```

如果配置了 Futu，也可能优先走：

```python
stock_df = FutuMarketDataProvider(normalized_market).get_stock_spot(normalized_market)
```

### 3. H股策略 7 入口
与 A股完全一致：

- `StockSelectStrategy.select_stock`
- `SelectionStrategyService.deep_value_growth_strategy`
- `_worker`

### 4. H股财务主入口
仍然是：

```python
stockBorderInfo.get_stock_border_financial_indicator(...)
```

但 H股会走港股字段映射：

- `营业收入 -> 营业总收入`
- `营业收入同比增长率 -> 营业总收入同比增长率`
- `归属于母公司股东的净利润同比增长率_hk -> 净利润同比增长率`
- `平均净资产收益率 -> 净资产收益率`

另外如果财务为空，还会在策略层 fallback：

文件：`selection_strategy_service.py`

```python
if (df_financial is None or df_financial.empty) and market in ('usa', 'H', 'HK'):
    detail_df = futu_provider.get_stock_snapshot_detail(stock_code, market)
```

### 5. H股与 A股的关键区别

#### 区别 1：ROE 目前不稳定
H股没有像 usa 一样强制通过 `净利润 / 净资产 * 100` 兜底计算 `ROE`。

#### 区别 2：PE 来源更依赖：
- `市盈率`
- `市盈率-TTM`
- `滚动市盈率每股收益_hk`

#### 区别 3：行业字段
H股最终也是通过：

- `行业板块`
- `行业`

来消费，但上游更依赖缓存/DB/Futu owner plate 补充，不像 A股那么稳定。

### 6. H股核心 API / 函数入口总结

#### API
- `/stock-screener`
- `/api/select_stock`

#### 核心函数
与 A股相同，区别主要在数据提供函数：

- `stockBorderInfo.get_stock_border_info`
- `stockBorderInfo.get_stock_border_financial_indicator`
- `FutuMarketDataProvider.get_stock_spot`
- `FutuMarketDataProvider.get_stock_snapshot_detail`

---

## 三、USA 美股

美股链路与 H股更像，也更依赖 Futu。

### 1. usa 候选股票入口

在配置了 Futu 且市场为 usa 时：

文件：`full_market_scan_workflow.py`

```python
if self._should_source_candidates_from_strategy(market=runtime.market):
    df_selected = SelectionStrategyService().get_prefilter_candidates_or_raise(...)
```

也就是说，美股可能先走策略预筛候选，而不是全量行情池。

否则也可能走：

```python
stock.get_stock_border_info()
```

### 2. usa 行情来源
文件：`market_data_service.py`

```python
elif self.market == 'usa':
    stock_df = ak.stock_us_spot_em()
```

如果配置了 Futu，则可能优先走：

```python
FutuMarketDataProvider(normalized_market).get_stock_spot(normalized_market)
```

### 3. usa 策略 7 入口
仍然一致：

- `StockSelectStrategy.select_stock`
- `SelectionStrategyService.deep_value_growth_strategy`
- `_worker`

### 4. usa 财务入口
仍然是：

```python
stockBorderInfo.get_stock_border_financial_indicator(...)
```

美股内部增加了专门处理：

文件：`market_data_service.py`

#### 美股专门补齐
```python
if '市盈率-TTM' in df_stock_financial_all.columns and 'PE_TTM' not in df_stock_financial_all.columns:
    df_stock_financial_all['PE_TTM'] = df_stock_financial_all['市盈率-TTM']

if '市盈率' in df_stock_financial_all.columns and 'PE' not in df_stock_financial_all.columns:
    df_stock_financial_all['PE'] = df_stock_financial_all['市盈率']

if '净利润' in df_stock_financial_all.columns and '净资产' in df_stock_financial_all.columns:
    df_stock_financial_all.loc[mask, 'ROE'] = (净利润 / 净资产) * 100
```

### 5. usa 的关键业务差异

#### 差异 1：代码规范化
美股经常有：

- `AAPL`
- `AAPL.US`

所以在 `market_data_service.py` 和 `selection_strategy_service.py` 都做了标准化匹配。

#### 差异 2：财务 fallback 高度依赖 Futu
当标准财务数据不完整时，常走：

- `FutuMarketDataProvider.get_stock_snapshot_detail`

#### 差异 3：增长率字段通常缺失
即使 `PE_TTM`、`ROE`、`市值` 有了，
`净利润同比增长率`、`营业总收入同比增长率` 仍可能缺失，
这会直接影响：

- `股票类型分类`
- `五阶段判断模型`

### 6. usa 核心 API / 函数入口总结

#### API
- `/stock-screener`
- `/api/select_stock`

#### 核心函数
- `SelectionStrategyService.get_prefilter_candidates_or_raise`（可能先走）
- `SelectionStrategyService.deep_value_growth_strategy`
- `stockBorderInfo.get_stock_border_financial_indicator`
- `FutuMarketDataProvider.get_stock_spot`
- `FutuMarketDataProvider.get_stock_snapshot_detail`
- `StockStrategy.calculate_stock_data`
- `StockStrategy.calculate_score`

---

## 四、行业字段的真实来源

最终在 `calculate_stock_data()` 里使用的是：

文件：`src/stock_analyse/domain/services/stock_strategy_service.py`

```python
border_name = _get_val(['行业板块', '行业'], '')
```

也就是说，最终消费的是：

- `行业板块`
- 或 `行业`

### 上游来源

#### 路径 1：技术分析流程中，从 `stock_industry_{market}` 相关表查出
文件：`src/stock_analyse/application/workflows/technical_analysis_workflow.py`

```python
if ('行业板块' not in row.index or not str(row.get('行业板块', '')).strip()) and not should_reuse_scan_row:
    row['行业板块'] = stock_service.get_stock_industry_by_code(stock_code)
```

这里的 `get_stock_industry_by_code(stock_code)` 是上游入口。

#### 路径 2：底层依赖 MySQL / 缓存表
它本质上依赖已经缓存的行业数据表，核心表名通常是：

- `stock_industry_data_{market}`
- `stock_industry_{market}`

所以更准确地说：

**行业字段最终来源于 `stock_service.get_stock_industry_by_code(stock_code)`，其底层主要依赖 MySQL 缓存表 `stock_industry_data_{market}` / `stock_industry_{market}` 查询结果，并回填到 `行业板块` 字段，后续再由 `calculate_stock_data()` 消费。**

---

## 五、三个市场的差异总结

| 市场 | 股票池入口 | 行情来源 | 财务来源 | 主要风险 |
|---|---|---|---|---|
| A股 SH/SZ | `get_stock_spot()` | AkShare A股实时行情 | A股财报 + TTM计算 | 财务 enrich 失败、AkShare 超时 |
| H股 H/HK | `get_stock_border_info()` | AkShare HK / Futu | 港股财务映射 + Futu fallback | `ROE` 常缺、行业不稳定 |
| 美股 usa | 可能先走策略预筛 | AkShare US / Futu | 美股 snapshot detail + 映射计算 | 增长率缺失、Futu连接稳定性 |

---

## 六、最核心的函数入口顺序

如果后续需要自己追代码，最核心的入口顺序就是：

1. `/stock-screener`
2. `/api/select_stock`
3. `StockAnalyzerService.stock_select_process()`
4. `run_stock_selection.execute()`
5. `StockSelectionOrchestrator.run_web_selection()`
6. `FullMarketScanWorkflow.run()`
7. `StockSelectStrategy.select_stock(..., strategy_type=7)`
8. `SelectionStrategyService.deep_value_growth_strategy()`
9. `_worker()`
10. `stockBorderInfo.get_stock_border_financial_indicator()`
11. `StockStrategy.calculate_stock_data()`
12. `StockStrategy.calculate_score()`

---

## 七、`df_financial` 的定义与三市场结构差异

在 `深度价值成长策略_7` 中，核心财务对象来自：

文件：`src/stock_analyse/infrastructure/services/market_data_service.py`

```python
df_financial = get_stock_border_financial_indicator(...)
```

但这个对象在策略里的真实使用方式，不是“只取一行展示”，而是同时承担两种职责：

1. 在 `_worker()` 中取一行回填到 `s_data`
2. 在 `calculate_stock_data(..., df_financial=df_financial)` 中作为 **历史估值窗口** 使用，用来计算：

```python
pe_percentile = self._calculate_financial_pe_percentile(df_financial, pe)
```

因此整理 `df_financial` 时，必须以 **5年数据窗口** 为准，而不是只看 `iloc[0]` 的首行。

当前三市场的真实形态是：

- A股：**历史财务窗口对象**
- H股：**单行 Futu 快照对象**
- 美股：**单行 Futu 快照 + 标准化字段对象**

也就是说，三市场的 `df_financial` 并不是统一结构对象。

---

### 7.1 A股 `df_financial` 字段定义

#### 来源
A股的 `df_financial` 来自：

- AkShare 财务指标数据
- 本地 5 年财报窗口缓存
- 额外补充：
  - `净利润_TTM`
  - `每股收益_TTM`
  - `PE_TTM`
  - `PE_静态`
  - `总市值`
  - `最新价`
  - `ROE`

#### 典型字段结构
实际样例（贵州茅台 `600519`）返回列：

```text
报告期
净利润
净利润同比增长率
扣非净利润
扣非净利润同比增长率
营业总收入
营业总收入同比增长率
基本每股收益
每股净资产
每股资本公积金
每股未分配利润
每股经营现金流
销售净利率
销售毛利率
净资产收益率
净资产收益率-摊薄
营业周期
存货周转率
存货周转天数
应收账款周转天数
流动比率
速动比率
保守速动比率
产权比率
资产负债率
股票代码
年份
净利润_TTM
eps_去年
prev_year_same_q_eps
每股收益_TTM
净利润_年报
总市值
最新价
PE_TTM
PE_静态
ROE
```

#### 实例数据定义（5年窗口关键列）
以贵州茅台 `600519` 为例，`df_financial` 实际是 29 行 5年窗口对象。下面展示其在 `calculate_stock_data()` 真正会消费的关键列：

```text
股票代码 报告期 年份 ROE 净资产收益率 PE_TTM PE_静态 营业总收入同比增长率 净利润同比增长率 资产负债率 净利润 净利润_TTM 每股收益_TTM 总市值 最新价
600519 2019-03-31 2019  9.47  9.47 44.559308      NaN 22.21 31.91 19.12% 11221000000.0 44884000000.0   NaN 2.000000e+12 1600.0
600519 2019-06-30 2019 16.21 16.21 50.122801      NaN 16.80 26.56 27.56% 19951000000.0 39902000000.0   NaN 2.000000e+12 1600.0
600519 2019-09-30 2019 24.92 24.92 49.252996      NaN 15.53 23.13 19.52% 30455000000.0 40606666666.67   NaN 2.000000e+12 1600.0
...
600519 2025-12-31 2025 32.53 32.53 24.295432 24.295432 -1.20 -4.53 16.42% 82320000000.0 82320000000.0 65.66 2.000000e+12 1600.0
600519 2026-03-31 2026 10.57 10.57 24.179119 24.295432  6.34  1.47 12.12% 27243000000.0 82716000000.0 66.04 2.000000e+12 1600.0
```

字段合法性统计：

- `ROE`: 29/29 非空
- `PE_TTM`: 29/29 非空
- `净利润同比增长率`: 29/29 非空
- `营业总收入同比增长率`: 29/29 非空
- `资产负债率`: 29/29 非空，但当前仍是字符串百分号格式
- `总市值`: 29/29 非空，但这里是**当前市值广播到整段历史窗口**
- `最新价`: 29/29 非空，但这里是**当前价格广播到整段历史窗口**

#### A股结构特点

优点：

- 字段最丰富
- 真正提供了 5 年左右历史财务窗口
- 有 `净利润_TTM`
- 可直接支持 `PE_TTM` 百分位计算
- `净利润同比增长率`、`营业总收入同比增长率` 一般可用

问题：

- 某些股票会直接返回空表，例如招商银行测试中 `rows=0`
- `资产负债率` 仍可能是字符串百分号格式，如 `19.12%`
- `每股收益_TTM` 并不是所有股票都稳定有值
- `总市值`、`最新价` 是当前值广播到整个 5 年窗口，不是真正的历史时点值

#### 结论
A股 `df_financial` 是当前三市场中**最符合 `calculate_stock_data()` 设计预期**的对象，因为它真的提供了历史窗口。

---

### 7.2 H股 `df_financial` 字段定义

#### 来源
H股的 `df_financial` 当前主要来自：

- Futu snapshot detail
- 港股财报字段映射
- 部分行情字段回填：
  - `总市值`
  - `最新价`
  - `PE_静态`

#### 典型字段结构
实际样例（腾讯控股 `00700`）返回列：

```text
股票代码
报告期
报告日期
总市值
流通市值
市盈率
市盈率-TTM
市净率
每股收益
每股净资产
净利润
净资产
股息-TTM
股息率-TTM
收益率
上市日期
名称
年份
最新价
PE_静态
```

#### 实例数据定义（实际窗口形态）
以腾讯控股 `00700` 为例，当前 `df_financial` 只返回 1 行，不是 5 年窗口：

```text
股票代码 报告期 年份 名称 PE_静态 市盈率 市盈率-TTM 净利润 总市值 最新价 净资产
00700    空    NaN 腾讯控股 14.060147 16.849 15.751 2.489305e+11 3.500000e+12 380.0 1.277140e+12
```

字段合法性统计：

- `总市值`: 1/1 非空
- `最新价`: 1/1 非空
- `市盈率`: 1/1 非空
- `市盈率-TTM`: 1/1 非空
- `净利润`: 1/1 非空
- `净资产`: 1/1 非空
- `ROE`: 缺失
- `净利润同比增长率`: 缺失
- `营业总收入同比增长率`: 缺失

#### H股结构特点

优点：

- `股票代码`、`名称`、`总市值`、`最新价` 一般可用
- `市盈率`、`市盈率-TTM` 一般可用
- `净利润`、`净资产` 一般可用

问题：

- 当前只返回 1 行，不是 5 年窗口
- 通常没有 `ROE`
- 通常没有 `净资产收益率`
- 通常没有 `平均净资产收益率`
- 通常没有 `净利润同比增长率`
- 通常没有 `营业总收入同比增长率`
- `报告期` 常为空
- `年份` 常为 `null`
- 某些股票会直接空表，例如中国移动 `00941` 测试中 `rows=0`

#### 结论
H股的 `df_financial` 本质上是：

**单行 Futu 快照对象，不是完整历史财务窗口对象。**

因此它和 `calculate_stock_data()` 对历史百分位窗口的理想预期并不一致。

---

### 7.3 usa 股 `df_financial` 字段定义

#### 来源
美股的 `df_financial` 当前主要来自：

- Futu snapshot detail
- 本地映射补齐：
  - `市盈率-TTM -> PE_TTM`
  - `市盈率 -> PE`
  - `净利润 / 净资产 * 100 -> ROE`
  - `总市值`
  - `最新价`
  - `PE_静态`

#### 典型字段结构
实际样例（AAPL）返回列：

```text
股票代码
报告期
报告日期
总市值
流通市值
市盈率
市盈率-TTM
市净率
每股收益
每股净资产
净利润
净资产
股息-TTM
股息率-TTM
收益率
上市日期
名称
PE_TTM
PE
ROE
年份
最新价
PE_静态
```

#### 实例数据定义（实际窗口形态）
以 Apple `AAPL` 为例，当前 `df_financial` 只返回 1 行：

```text
股票代码 报告期 年份 名称 ROE PE_TTM PE_静态 PE 市盈率 市盈率-TTM 净利润 总市值 最新价 净资产
AAPL     空    NaN 苹果 102.882361 36.102 26.467660 39.974 39.974 36.102 1.095677e+11 2.900000e+12 190.0 1.064980e+11
```

字段合法性统计：

- `ROE`: 1/1 非空
- `PE_TTM`: 1/1 非空
- `总市值`: 1/1 非空
- `最新价`: 1/1 非空
- `净利润同比增长率`: 缺失
- `营业总收入同比增长率`: 缺失
- `资产负债率`: 缺失
- `净利润_TTM`: 缺失
- `每股收益_TTM`: 缺失

#### usa 结构特点

优点：

- `PE_TTM` 已被显式补齐
- `PE` 已被显式补齐
- `ROE` 已被显式补齐
- `总市值`、`最新价`、`净利润`、`净资产` 一般可以拿到

问题：

- 当前只返回 1 行，不是 5 年窗口
- `报告期` 常为空
- `年份` 常为 `null`
- 没有 `净利润同比增长率`
- 没有 `营业总收入同比增长率`
- 某些股票会直接空表，例如 `NVDA` 测试中 `rows=0`
- `ROE` 虽然有值，但可能非常大，例如 `102.88`，数学上成立，业务上要谨慎使用

#### 结论
美股的 `df_financial` 本质上是：

**单行 Futu 快照 + 本地字段标准化计算后的对象，不是历史财务窗口。**

---

## 八、`df_financial` 字段一致性与准确性分析

### 8.1 一致性较好的字段

以下字段在三市场中相对最稳定：

- `股票代码`
- `总市值`
- `最新价`
- `市盈率`
- `市盈率-TTM`
- `净利润`
- `净资产`
- `名称`

这些字段在 `H / usa` 的 Futu 快照结构中天然存在，A股也通常能通过补齐得到。

### 8.2 一致性较差的字段

以下字段不能认为是三市场统一 contract：

- `ROE`
- `净资产收益率`
- `平均净资产收益率`
- `净利润同比增长率`
- `营业总收入同比增长率`
- `报告期`
- `年份`
- `净利润_TTM`
- `每股收益_TTM`

### 8.3 内容较准确的字段

以下字段从当前样例看，内容可信度较高：

- `总市值`
- `最新价`
- `市盈率`
- `市盈率-TTM`
- `净利润`
- `净资产`

但要特别注意：

- A股的 `总市值` 和 `最新价` 是**当前行情值广播到整个 5 年窗口**，不是真实历史点值
- 所以这些字段适合做“当前策略分析”，不适合严格做历史估值回溯

### 8.4 列一致但内容/可用性不稳定的字段

- `ROE`
- `PE_TTM`
- `资产负债率`
- `净利润同比增长率`
- `营业总收入同比增长率`

### 8.5 最终判断

`df_financial` 在 `SH / H / usa` 下，**根本不是统一结构对象**。

更准确地说：

- A股：历史财务窗口对象
- H股：单行 Futu 快照对象
- 美股：单行 Futu 快照 + 标准化字段对象

所以后续 `deep_value_growth_strategy_7` 如果直接把三者当成同一结构消费，天然会出现：

- 字段名不一致
- 值缺失
- 口径不同
- 同一逻辑在不同市场下表现不一致

### 8.6 后续建议

建议将 `df_financial` 的使用分成两层：

#### 第一层：原始数据层
保留市场差异，不强行统一结构

#### 第二层：标准字段层
在进入策略前统一抽取成固定 contract，例如：

- `股票代码`
- `名称`
- `总市值`
- `最新价`
- `PE_TTM`
- `ROE`
- `净利润同比增长率`
- `营业总收入同比增长率`
- `行业板块`
- `资产负债率`

策略层只读这套标准字段，不直接碰原始 `df_financial` 列。
