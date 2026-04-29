# StockLib 模块文档

## 目录

1. [概述](#概述)
2. [模块列表](#模块列表)
3. [详细说明](#详细说明)
   - [dcf_model.py](#dcf_modelpy)
   - [mysql_cache.py](#mysql_cachepy)
   - [stock_ak_indicator.py](#stock_ak_indicatorpy)
   - [stock_annual_report.py](#stock_annual_reportpy)
   - [stock_border.py](#stock_borderpy)
   - [stock_company.py](#stock_companypy)
   - [stock_concept_data.py](#stock_concept_datapy)
   - [stock_concept_service.py](#stock_concept_servicepy)
   - [stock_data_init.py](#stock_data_initpy)
   - [stock_indicator_html.py](#stock_indicator_htmlpy)
   - [stock_indicator_quantitative.py](#stock_indicator_quantitativepy)
   - [stock_news_data.py](#stock_news_datapy)
   - [stock_sentiment_analysis.py](#stock_sentiment_analysispy)
   - [stock_strategy.py](#stock_strategypy)
   - [stock_wave_analyser.py](#stock_wave_analyserpy)
   - [technical_params.py](#technical_paramspy)
   - [utils_file_cache.py](#utils_file_cachepy)
   - [utils_report_date.py](#utils_report_datepy)
   - [utils_stock.py](#utils_stockpy)

---

## 概述

`stocklib` 是股票分析系统的核心数据访问层，提供了从数据采集、缓存、技术指标计算到策略分析的完整功能链。支持 A 股(SH/SZ)、港股(H)、美股(usa)和 ETF/债券(zq)等多个市场。

---

## 模块列表

| 模块名 | 主要功能 |
|--------|----------|
| `dcf_model.py` | DCF（现金流折现）估值模型 |
| `mysql_cache.py` | MySQL 数据库缓存管理 |
| `stock_ak_indicator.py` | AKShare 技术指标计算（策略信号） |
| `stock_annual_report.py` | 年报数据和三大报表获取 |
| `stock_border.py` | 市场全景数据（所有股票） |
| `stock_company.py` | 个股信息查询（公司详情、财务指标） |
| `stock_concept_data.py` | 同花顺概念板块数据爬取 |
| `stock_concept_service.py` | 概念/行业板块服务（含缓存） |
| `stock_data_init.py` | 数据初始化入口 |
| `stock_indicator_html.py` | 技术指标可视化（HTML输出） |
| `stock_indicator_quantitative.py` | 量化指标计算和绘图 |
| `stock_news_data.py` | 个股新闻数据获取 |
| `stock_sentiment_analysis.py` | 市场情绪分析 |
| `stock_strategy.py` | 股票策略评分系统 |
| `stock_wave_analyser.py` | 波浪分析（波峰波谷识别） |
| `technical_params.py` | 技术指标参数配置 |
| `utils_file_cache.py` | 文件缓存工具 |
| `utils_report_date.py` | 报告日期工具类 |
| `utils_stock.py` | 股票通用工具函数 |

---

## 详细说明

### dcf_model.py

**类：`stockDCFSimpleModel`**

DCF（现金流折现）估值模型，用于计算股票的内在价值。

#### 方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `__init__` | `market='SZ'` | - | 初始化模型 |
| `calculate_dcf` | `df, discount_rate=0.1, growth_rate=0.03` | `float` | 计算 DCF 价值 |
| `calculate_stock_price_range` | `zcfz, lrb, xjll` | `DataFrame` | 计算股价区间（保守/正常/乐观） |
| `calculate_stock_test` | `zcfz, lrb, xjll` | - | 测试方法 |

#### 计算逻辑

1. 基于经营性现金流计算未来 5 年自由现金流
2. 使用净利润同比增长率作为增长预测
3. 计算终值（Terminal Value）
4. 折现得到企业价值
5. 扣除净负债得到股权价值
6. 除以总股本得到每股价值

---

### mysql_cache.py

**类：`MySQLCache`**

MySQL 数据库缓存管理类，用于将 DataFrame 数据持久化到 MySQL 数据库。

#### 方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `__init__` | `db_user, db_password, db_host, db_port, db_name, cache_dir, market` | - | 初始化数据库连接 |
| `write_to_cache` | `date, report_type, data, force, market, file_type` | - | 写入数据到缓存 |
| `read_from_cache` | `date, report_type, market, file_type, conditions` | `DataFrame/None` | 从缓存读取数据 |
| `_get_table_name` | `date, report_type, file_type, market` | `str` | 生成表名 |
| `_table_exists` | `table_name` | `bool` | 检查表是否存在 |
| `_create_table_from_df` | `table_name, df` | - | 根据 DataFrame 创建表 |
| `_insert_data_to_table` | `table_name, df, date` | - | 插入数据 |
| `_read_from_mysql` | `table_name, date, conditions` | `DataFrame` | 从 MySQL 读取 |

#### 支持的表类型

- `history_{market}` - 历史数据
- `zcfz_{market}` / `lrb_{market}` / `xjll_{market}` - 三大报表
- `financial_{market}` - 财务指标
- `spot_em_zh_df_{market}` - 实时行情
- `stock_concept_{market}` / `stock_industry_{market}` - 板块数据

---

### stock_ak_indicator.py

**类：`stockAKIndicator`**

基于 AKShare 的股票技术指标计算类，生成各种交易策略信号。

#### 属性

| 属性名 | 说明 |
|--------|------|
| `current_date_str` | 当前日期字符串（YYYYMMDD） |
| `previous_date_str` | 前一天日期字符串 |
| `previous_year_str` | 一年前日期字符串 |

#### 方法

##### 数据获取

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `get_stock_code` | `df` | `str` | 从 DataFrame 提取股票代码 |
| `stock_day_data_code` | `stock_code, market, start_date_str, end_date_str` | `DataFrame` | 获取日线数据 |

##### 策略计算方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `strategy_mac` | `data, window=20` | `DataFrame` | 移动平均线策略（MA10/MA30金叉死叉） |
| `strategy_bollinger` | `data, short_window=10, long_window=30` | `DataFrame` | 布林带策略 |
| `strategy_macd` | `data, momentum_window=20` | `DataFrame` | MACD 策略 |
| `strategy_breakout` | `data, window=20` | `DataFrame` | 突破策略（支撑/阻力位） |
| `strategy_sar` | `data` | `DataFrame` | SAR 抛物线转向策略 |
| `mean_reversion_strategy` | `data, window=20, z_score_threshold=1` | `DataFrame` | 均值回归策略 |
| `sub_structure_arbitrage` | `data, short_window=5, long_window=20, deviation_threshold=0.05` | `DataFrame` | 次级结构套利策略 |
| `strategy_rsi` | `data, period=14, overbought=70, oversold=30` | `DataFrame` | RSI 相对强弱指标策略 |
| `strategy_kdj` | `data, fastk_period=9, slowk_period=3, slowd_period=3` | `DataFrame` | KDJ 随机指标策略 |
| `strategy_williams_r` | `data, time_period=14, overbought=-20, oversold=-80` | `DataFrame` | 威廉指标策略 |
| `strategy_adx` | `data, time_period=14, adx_threshold=25` | `DataFrame` | ADX 趋向指标策略 |

#### 策略信号字段说明

每个策略方法会添加以下信号字段到 DataFrame：

- `{strategy}_signal` - 信号值（1=买入, -1=卖出, 0=无信号）
- `{strategy}_signal_position` - 信号变化（diff）

---

### stock_annual_report.py

**类：`stockAnnualReport`**

年报数据和财务报表获取类。

#### 方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `__init__` | - | - | 初始化 |
| `format_float` | `x` | `str` | 格式化浮点数（静态方法） |
| `get_date_String` | `date_str` | `str` | 转换日期格式 |
| `get_stock_report_file` | `stock_code, market, start_date, end_date` | `list` | 下载年报 PDF 文件 |
| `get_stock_zygc` | `stock_code, market` | `DataFrame` | 获取主营构成 |
| `get_stock_report` | `stock_code, market, indicator, years` | `(zcfz, lrb, xjll)` | 获取三大报表 |
| `get_stock_code` | `market, symbol` | `str` | 提取股票代码 |
| `filter_stock_reprt_df` | `df, years, date_column` | `DataFrame` | 按年份过滤报表 |
| `filter_stock_reprt_indicator` | `df, date_column, indicator` | `DataFrame` | 按报告类型过滤 |

#### 支持的市场

- `SH` / `SZ` - A 股（使用新浪财经数据）
- `H` - 港股（使用东方财富数据）
- `usa` - 美股（使用东方财富数据）

---

### stock_border.py

**类：`stockBorderInfo`**

市场全景数据管理类，获取所有股票的市场数据。

#### 属性

| 属性名 | 说明 |
|--------|------|
| `market` | 市场代码（SH/SZ/H/usa） |
| `usa` | 美股标识 |
| `ETF` | ETF/债券标识 |
| `HongKong` | 港股标识 |
| `max_workers` | 并发线程数（默认20） |

#### 方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `get_stock_all_info` | - | `DataFrame` | 获取所有股票资金流 |
| `get_stock_zh_a_spot_em_df` | - | `DataFrame` | 获取 A 股实时行情（带备用接口） |
| `get_stock_spot` | - | `DataFrame` | 获取股票实时行情（带缓存） |
| `get_stock_border_report` | `market, date, indicator, fields_unification` | `(zcfz, lrb, xjll)` | 获取市场三大报表 |
| `get_stock_border_financial_indicator` | `market, date, indicator, df_stock_spot` | `DataFrame` | 获取市场财务指标 |
| `get_stock_board_all_concept_name` | - | `DataFrame` | 获取所有概念板块 |
| `get_stock_all_code` | - | `DataFrame` | 获取所有股票代码 |
| `get_stock_hsgt_hold_stock_em` | - | `DataFrame` | 获取北向资金持仓 |
| `get_famous_stock_info` | - | `DataFrame` | 获取知名股票数据 |
| `get_stock_fhps_info` | `date` | `DataFrame` | 获取分红配送数据 |
| `get_stock_zcfz_analysis` | `market, date` | `DataFrame` | 获取资产负债分析 |

---

### stock_company.py

**类：`stockCompanyInfo`**

个股信息查询类，提供单个股票的详细信息查询。

#### 属性

| 属性名 | 说明 |
|--------|------|
| `market` | 市场代码 |
| `symbol` | 股票代码 |
| `xq_a_token` | 雪球 API Token |

#### 方法

##### 板块信息

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `get_stock_board_all_concept_name` | - | `DataFrame` | 获取所有概念板块 |
| `get_stock_board_all_industry_name` | - | `DataFrame` | 获取所有行业板块 |
| `get_stock_concept_by_name` | `concept_name, industry_sectors` | `DataFrame` | 按名称获取概念成分股 |
| `get_stock_industry_by_name` | `concept_name, industry_sectors` | `DataFrame` | 按名称获取行业成分股 |
| `get_stock_industry_by_code` | `code, date` | `str` | 按代码获取所属行业 |
| `get_stock_concept_by_code` | `code, date` | `str` | 按代码获取所属概念 |

##### 公司信息

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `get_stock_zyjs` | - | `DataFrame` | 获取主营业务介绍 |
| `get_stock_name` | - | `str` | 获取股票名称 |
| `get_stock_individual_info` | - | `DataFrame` | 获取个股详细信息 |
| `get_stock_individual_info_em` | - | `(df, list_date, industry)` | 获取个股信息（东方财富） |

##### 新闻和资金流

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `get_stock_news` | - | `DataFrame` | 获取个股新闻 |
| `get_stock_fund_flow` | - | `DataFrame` | 获取行业资金流 |
| `get_stock_individual_fund_flow` | - | `DataFrame` | 获取个股历史资金流 |

##### 财务数据

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `get_stock_financial_analysis_indicator` | `start_year` | `DataFrame` | 获取财务分析指标 |
| `get_stock_report` | `indicator, years` | `(zcfz, lrb, xjll)` | 获取三大报表 |
| `get_stock_zygc_ym` | - | `DataFrame` | 获取主营构成（按月份） |
| `get_stock_zycwzb` | - | `DataFrame` | 获取主要财务指标 |
| `get_stock_yjbb` | - | `DataFrame` | 获取业绩报表 |
| `get_stock_yjkb` | - | `DataFrame` | 获取业绩快报 |
| `get_stock_yjyg` | - | `DataFrame` | 获取业绩预告 |
| `get_stock_dzjy` | - | `DataFrame` | 获取大宗交易 |
| `get_stock_hsgt` | - | `DataFrame` | 获取沪深港通持股 |
| `get_stock_gdzjc` | - | `DataFrame` | 获取股东增减持 |
| `get_stock_jgdy` | - | `DataFrame` | 获取机构调研 |
| `get_stock_fhps` | - | `DataFrame` | 获取分红配送 |

##### 历史数据

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `get_stock_history_data` | `start_date_str, end_date_str` | `DataFrame` | 获取历史行情数据 |
| `get_stock_history_data_em` | `start_date_str, end_date_str` | `DataFrame` | 获取历史数据（东方财富） |
| `get_stock_history_data_sina` | `start_date_str, end_date_str` | `DataFrame` | 获取历史数据（新浪财经） |

---

### stock_concept_data.py

**类：`stockConceptData`**

同花顺概念板块数据爬取类。

#### 方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `stock_board_concept_graph_ths` | `symbol` | `DataFrame` | 获取概念图谱 |
| `_get_file_content_ths` | `file` | `str` | 获取 JS 文件内容 |
| `stock_board_concept_name_ths` | - | `DataFrame` | 获取所有概念板块名称 |
| `_stock_board_concept_code_ths` | - | `dict` | 获取概念代码映射 |
| `stock_board_concept_cons_ths` | `symbol, stock_board_ths_map_df` | `DataFrame` | 获取概念成分股 |
| `stock_board_concept_info_ths` | `symbol, stock_board_ths_map_df` | `DataFrame` | 获取概念简介 |
| `stock_board_concept_hist_ths` | `start_year, symbol` | `DataFrame` | 获取概念历史指数数据 |
| `stock_board_cons_ths` | `symbol` | `DataFrame` | 通过代码获取成分股 |
| `get_concept_by_stock` | `symbol` | `list` | 获取股票所属概念列表 |

---

### stock_concept_service.py

**类：`stockConcepService`**

概念/行业板块服务类，包含数据缓存功能。

#### 属性

| 属性名 | 说明 |
|--------|------|
| `max_workers` | 并发线程数 |
| `min_score` | 高分最低阈值 |
| `market` | 市场代码 |

#### 方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `__init__` | `max_workers, min_score, market` | - | 初始化服务 |
| `get_all_sectors_and_stocks` | - | `(concept_sectors, industry_sectors)` | 获取所有板块和成分股 |

#### 数据流程

1. 从缓存读取概念/行业板块列表
2. 如缓存未命中，从 AKShare 获取
3. 遍历板块获取成分股
4. 写入 MySQL 缓存

---

### stock_data_init.py

**类：`stockDataInit`**

数据初始化入口类，用于批量初始化股票数据。

#### 方法

| 方法名 | 参数 | 说明 |
|--------|------|------|
| `__init__` | `market, symbol` | 初始化 |
| `init_stock_by_day` | - | 初始化每日数据（实时行情、北向资金） |
| `init_stock_allmarket_by_day` | - | 初始化所有市场每日数据 |
| `init_stock_by_year` | `report_date` | 初始化年度数据（报表、财务指标、分红） |
| `init_stock_allmarket_by_year` | `report_date` | 初始化所有市场年度数据 |

#### 数据分类

**按日初始化：**
- 每天的股票实时数据
- 每天新闻数据
- 股票历史成交数据
- 北向资金当日持仓排行

**按年和季度初始化：**
- 财务指标数据
- 财务报表（年报、季报、半年报）
- 股息数据

**固定数据：**
- 板块数据
- 板块归属
- 概念数据
- 概念归属
- 公司介绍
- 知名股票数据

---

### stock_indicator_html.py

**类：`stockIndicatorHtml`**

技术指标可视化类，生成 HTML 格式的图表。

#### 方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `__init__` | `current_date` | - | 初始化 |
| `get_stock_code` | `df` | `str` | 获取股票代码 |
| `plot_sma` | `data, window=20` | `str` | 绘制移动平均线（HTML） |
| `plot_stock_wave` | `df` | `str` | 绘制小波分析图（HTML） |
| `plot_stock_Bollinger` | `df` | `str` | 绘制布林带（HTML） |
| `plot_stock_fft` | `df` | `str` | 绘制傅里叶变换图（HTML） |
| `stock_day_data_code` | `stock_code, market, start_date_str, end_date_str` | `DataFrame` | 获取日线数据 |

---

### stock_indicator_quantitative.py

**类：`stockIndicatorQuantitative`**

量化指标计算和绘图类。

#### 方法

##### 数据获取

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `get_stock_code` | `df` | `str` | 获取股票代码 |
| `stock_day_data_code` | `stock_code, market, start_date_str, end_date_str` | `DataFrame` | 获取日线数据 |

##### 策略绘图

| 方法名 | 参数 | 说明 |
|--------|------|------|
| `plot_strategy_mac` | `data, window=20` | 绘制均线策略图 |
| `plot_strategy_bollinger` | `data` | 绘制布林带策略图 |
| `plot_strategy_macd` | `data` | 绘制 MACD 策略图 |
| `plot_strategy_breakout` | `data, window=20` | 绘制突破策略图 |
| `plot_strategy_sar` | `data` | 绘制 SAR 策略图 |
| `plot_mean_reversion_strategy` | `data, window=20, z_score_threshold=1` | 绘制均值回归策略图 |
| `plot_sub_structure_arbitrage` | `data, short_window=5, long_window=20` | 绘制次级结构套利图 |
| `plot_strategy_rsi` | `data, period=14, overbought=70, oversold=30` | 绘制 RSI 策略图 |
| `plot_strategy_kdj` | `data, fastk_period=9, slowk_period=3` | 绘制 KDJ 策略图 |
| `plot_strategy_williams_r` | `data, time_period=14` | 绘制威廉指标图 |
| `plot_strategy_adx` | `data, time_period=14, adx_threshold=25` | 绘制 ADX 策略图 |

---

### stock_news_data.py

**类：`stockNewsData`**

个股新闻数据获取类（静态方法类）。

#### 方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `stock_news_em` | `symbol, pageSize, chrome_driver_path` | `DataFrame` | 获取东方财富个股新闻 |
| `save_to_excel` | `df, symbol` | - | 保存新闻到 Excel |

#### 数据来源

- 东方财富搜索 API
- 使用 Selenium + ChromeDriver 爬取

---

### stock_sentiment_analysis.py

**类：`StockSentimentAnalysis`**

市场情绪分析类，基于新闻和公告计算情绪得分。

#### 属性

| 属性名 | 说明 |
|--------|------|
| `market` | 市场代码 |
| `symbol` | 股票代码 |
| `news_cache` | 新闻缓存 |

#### 方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `__init__` | `market, symbol` | - | 初始化分析引擎 |
| `_setup_logging` | - | - | 配置日志 |
| `get_sentiment_analysis` | - | `(score, analysis)` | 获取情绪分析结果 |
| `get_comprehensive_news_data` | `stock_code, days` | `dict` | 获取综合新闻数据 |
| `calculate_advanced_sentiment_analysis` | `comprehensive_news_data` | `dict` | 计算高级情绪分析 |
| `calculate_sentiment_score` | `sentiment_analysis` | `float` | 计算情绪得分 |

#### 情绪词典

**正向词汇：** 上涨、涨停、利好、突破、增长、盈利、收益、回升、强势、看好、买入、推荐...

**负向词汇：** 下跌、跌停、利空、破位、下滑、亏损、风险、回调、弱势、看空、卖出、减持...

#### 新闻类型权重

| 类型 | 权重 |
|------|------|
| 公司新闻 | 1.0 |
| 公告 | 1.2 |
| 研究报告 | 0.9 |
| 行业新闻 | 0.7 |

---

### stock_strategy.py

**类：`StockStrategy`**

股票策略评分系统，综合多种技术指标计算买入评分。

#### 属性

| 属性名 | 说明 |
|--------|------|
| `date_utils` | 日期工具实例 |
| `market` | 市场代码 |

#### 方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `__init__` | `market` | - | 初始化 |
| `_setup_logging` | - | - | 配置日志 |
| `calculate_stock_data` | `df_history_data, df_stock_data, stock_code` | `DataFrame` | 计算股票综合数据 |
| `calculate_score` | `df_history_data, df_stock, df_summary_data` | `(score, buy_signal_str)` | 计算综合评分 |
| `calculate_score_indicate` | `df_history_data, df_stock` | `(score, buy_signal_str)` | 指标评分计算 |
| `calculate_vol_inc` | `df, ratio, df_stock` | `int` | 计算成交量放大得分 |
| `has_recent_buy_signal` | `df, date_column, signal_column` | `bool` | 检查最近是否有买入信号 |
| `calculate_score_simple` | `df` | `(score, buy_signal_str)` | 简单评分计算 |
| `get_recommendation` | `score` | `str` | 根据评分获取建议 |

#### 评分权重分配

| 指标 | 权重 |
|------|------|
| 波浪趋势分析 | 0-70分 |
| MACD 信号 | 动态计算 |
| 成交量放大 | 0-40分 |
| RSI 信号 | 10分 |
| KDJ 信号 | 5分 |
| 突破信号 | 5分 |
| 布林带信号 | 10分 |
| 威廉指标 | 5分 |
| ADX 策略 | 5分 |
| 均值回归 | 5分 |

#### 建议等级

| 分数 | 建议 |
|------|------|
| >= 50 | 强烈推荐买入 |
| >= 30 | 建议买入 |
| >= 10 | 建议持有 |
| >= 5 | 建议观望 |
| < 5 | 建议观望 |

---

### stock_wave_analyser.py

**类：`StockWaveAnalyzer`**

波浪分析类，识别股票价格的波峰和波谷。

#### 属性

| 属性名 | 说明 |
|--------|------|
| `market` | 市场代码 |
| `symbol` | 股票代码 |
| `min_period` | 最小周期（默认4或5） |
| `price_threshold` | 价格阈值（默认0.01或0.05） |

#### 方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `__init__` | `market, symbol` | - | 初始化分析引擎 |
| `_setup_logging` | - | - | 配置日志 |
| `get_stock_data` | `days=200` | `DataFrame` | 获取股票数据 |
| `show_waves` | `stock_df, peaks, troughs` | - | 显示波浪图 |
| `identify_waves` | `close_prices, stock_df, min_period, price_threshold` | `list` | 识别波浪转折点 |
| `analysis_stock_trend` | `stock_df` | `(df_wave, total_trend, last_trend)` | 分析股票趋势 |
| `get_stock_trend` | `df_wave` | `str` | 获取整体趋势 |
| `get_last_trend` | `df_wave` | `str` | 获取最后趋势状态 |
| `analysis_stock_wave` | `stock_df` | `DataFrame` | 分析股票波浪 |
| `get_stock_df_date` | `start_idx, stock_df` | `str` | 获取日期字符串 |

#### 趋势判断

**整体趋势：**
- `上升` - 上升序列单调递增
- `下降` - 下降序列单调递减
- `波动上升` - 上升占比 > 60%
- `波动下降` - 下降占比 > 60%
- `波动` - 其他情况

**最后趋势状态：**
- `翻转中` - 最后类型为上升
- `探底中` - 最后类型为下降

---

### technical_params.py

**数据类：`TechnicalParams`**

技术指标参数配置类。

#### 字段

| 字段名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `ma_periods` | `Dict[str, int]` | `{'short': 5, 'medium': 20, 'long': 60}` | 移动平均线周期 |
| `rsi_period` | `int` | `14` | RSI 周期 |
| `bollinger_period` | `int` | `20` | 布林带周期 |
| `bollinger_std` | `int` | `2` | 布林带标准差倍数 |
| `volume_ma_period` | `int` | `20` | 成交量 MA 周期 |
| `atr_period` | `int` | `14` | ATR 周期 |

#### 类方法

| 方法名 | 返回值 | 说明 |
|--------|--------|------|
| `default()` | `TechnicalParams` | 返回默认参数配置 |

---

### utils_file_cache.py

**类：`FileCacheUtils`**

文件缓存工具类，支持 CSV 和 Pickle 格式缓存。

#### 方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `__init__` | `market, cache_dir` | - | 初始化缓存工具 |
| `_get_cache_filepath` | `date, report_type, file_type` | `str` | 生成缓存文件路径 |
| `read_from_csv` | `date, report_type` | `DataFrame/None` | 从 CSV 读取 |
| `write_to_csv` | `date, report_type, data, force` | - | 写入 CSV |
| `write_to_csv_force` | `zcfz, lrb, xjll, date` | - | 强制写入三大报表 |
| `write_to_cache_serialized` | `date, report_type, data, force` | - | 序列化写入缓存 |
| `read_from_serialized` | `date, report_type` | `DataFrame/None` | 从序列化读取 |
| `write_to_cache_db` | `date, report_type, data, force` | - | 写入数据库缓存 |

#### 缓存目录结构

```
cache/
├── financial_reports/
│   ├── financial_indicator/
│   ├── stock_report/
│   ├── history/
│   └── {report_type}/
```

---

### utils_report_date.py

**类：`ReportDateUtils`**

报告日期工具类，处理各种报告日期相关的计算。

#### 方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `__init__` | - | - | 初始化 |
| `get_report_year_str_list` | `years=5` | `list` | 获取最近 N 年的日期列表 |
| `get_current_report_year_st` | `format, market` | `str` | 获取当前报告期年份 |
| `get_current__history_date_str` | `format, days` | `str` | 获取历史日期字符串 |
| `get_report_date_add_str` | `date_str, days, format, postfix_str` | `str` | 日期加法计算 |
| `get_current_history_date_st` | - | `str` | 获取当前历史日期 |
| `get_start_history_date_st` | `days=180` | `str` | 获取起始历史日期 |
| `get_report_year_str` | `days, format, postfix_str` | `str` | 获取报告年份字符串 |
| `get_report_hk_year_str` | `days, postfix_str` | `str` | 获取港股报告年份 |
| `get_history_date_str` | `days, format` | `str` | 获取历史日期 |
| `get_report_last_five_year` | `date` | `str` | 获取 5 年前的年份 |
| `get_stock_code` | `market, symbol` | `str` | 提取股票代码 |
| `pivot_financial_usa_data` | `df, index_cols, item_col, value_col...` | `DataFrame` | 透视财务数据 |
| `map_lrb_share_to_a_share` | `h_share_df, market` | `DataFrame` | 利润表映射到 A 股格式 |
| `map_zcfz_share_to_a_share` | `h_share_df, market` | `DataFrame` | 资产负债表映射到 A 股格式 |
| `map_xjll_share_to_a_share` | `h_share_df, market` | `DataFrame` | 现金流量表映射到 A 股格式 |
| `financial_indicator_map_hk_fields` | `df` | `DataFrame` | 港股财务指标字段映射 |
| `financial_indicator_map_usa_fields` | `df` | `DataFrame` | 美股财务指标字段映射 |
| `calculate_stock_progress` | - | `float` | 计算当日交易进度 |

#### 市场日期规则

| 市场 | 年报日期 | 说明 |
|------|----------|------|
| SH/SZ | 0331 | 3月31日 |
| H/usa | 1231 | 12月31日 |

---

### utils_stock.py

**类：`StockUtils`**

股票通用工具函数类。

#### 方法

| 方法名 | 参数 | 返回值 | 说明 |
|--------|------|--------|------|
| `__init__` | - | - | 初始化 |
| `get_stock_zh_code` | `code` | `str` | 根据代码获取带市场前缀的代码 |
| `format_history_stock_code` | `stock_zh_a_hist_df, stock_code` | `DataFrame` | 格式化历史数据列名 |
| `pd_convert_to_float` | `df, col_name` | `DataFrame` | 将字段转换为 float 类型 |

#### 市场前缀规则

| 代码开头 | 前缀 | 市场 |
|----------|------|------|
| 6 | sh | 上海主板 |
| 0, 3 | sz | 深圳（主板/创业板） |
| 8, 4, 9 | bj | 北京（北交所） |

#### 单位转换规则

| 单位 | 转换 |
|------|------|
| % | 除以 100 |
| 万 | 乘以 10000 |
| 亿 | 乘以 100000000 |

---

## 使用示例

### 获取个股历史数据

```python
from stocklib.stock_company import stockCompanyInfo

# 初始化个股服务
stock = stockCompanyInfo(marker='SH', symbol='601318')

# 获取历史数据
df = stock.get_stock_history_data(
    start_date_str='20240101',
    end_date_str='20241231'
)
```

### 计算技术指标

```python
from stocklib.stock_ak_indicator import stockAKIndicator

indicator = stockAKIndicator()

# 计算 MACD 策略信号
df_with_macd = indicator.strategy_macd(df)

# 计算 RSI 策略信号
df_with_rsi = indicator.strategy_rsi(df)
```

### 获取市场全景数据

```python
from stocklib.stock_border import stockBorderInfo

border = stockBorderInfo(market='SH')

# 获取所有股票实时行情
df_spot = border.get_stock_spot()

# 获取三大报表
zcfz, lrb, xjll = border.get_stock_border_report(
    market='SH',
    date='20240331'
)
```

### DCF 估值计算

```python
from stocklib.dcf_model import stockDCFSimpleModel

dcf = stockDCFSimpleModel(market='SH')

# 计算股价区间
df_result = dcf.calculate_stock_price_range(zcfz, lrb, xjll)
```

---

## 依赖说明

主要依赖包：

- `akshare` - 股票数据获取
- `pandas` - 数据处理
- `numpy` - 数值计算
- `talib` - 技术指标计算
- `pymysql` / `sqlalchemy` - MySQL 数据库
- `requests` / `beautifulsoup4` - 网页爬取
- `scipy` - 科学计算（波峰波谷识别）
- `matplotlib` / `mpld3` - 可视化
- `selenium` - 浏览器自动化

---

## 注意事项

1. **缓存机制**：系统使用多级缓存（文件缓存 + MySQL 缓存），注意缓存刷新
2. **并发控制**：部分方法使用线程池（默认20线程），注意 API 限流
3. **市场差异**：不同市场的数据字段和报告期存在差异，注意字段映射
4. **异常处理**：网络请求可能失败，建议添加异常处理
5. **数据权限**：部分数据需要相应的数据权限或 Token

---

## 领域模型

基于领域驱动设计（DDD）分析，本系统包含以下核心业务领域和概念：

---

### 1. 股票实体 (Stock Entity)

代表一支股票的基本信息和标识。

**对应类：** `stockCompanyInfo`, `stockBorderInfo`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `get_stock_name()` | `stockCompanyInfo` | 获取股票名称 |
| `get_stock_individual_info()` | `stockCompanyInfo` | 获取个股详细信息 |
| `get_stock_individual_info_em()` | `stockCompanyInfo` | 获取个股信息（东方财富） |
| `get_stock_history_data()` | `stockCompanyInfo` | 获取历史行情数据 |
| `get_stock_all_code()` | `stockBorderInfo` | 获取所有股票代码 |
| `get_stock_spot()` | `stockBorderInfo` | 获取实时行情 |

**属性：**
- 股票代码 (symbol)
- 股票名称 (name)
- 市场类型 (market: SH/SZ/H/usa/zq)
- 所属行业 (industry)
- 所属概念 (concepts)
- 上市日期 (list_date)

---

### 2. 财务报表聚合 (Financial Report Aggregate)

代表公司财务报表的完整集合，包含三大报表。

**对应类：** `stockAnnualReport`, `stockCompanyInfo`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `get_stock_report()` | `stockAnnualReport` | 获取三大报表（资产负债表、利润表、现金流量表） |
| `get_stock_zcfz_analysis()` | `stockBorderInfo` | 获取资产负债分析 |
| `get_stock_zygc()` | `stockAnnualReport` | 获取主营构成 |
| `get_stock_zygc_ym()` | `stockCompanyInfo` | 获取主营构成（按月份） |
| `get_stock_zycwzb()` | `stockCompanyInfo` | 获取主要财务指标 |
| `get_stock_yjbb()` | `stockCompanyInfo` | 获取业绩报表 |
| `get_stock_yjkb()` | `stockCompanyInfo` | 获取业绩快报 |
| `get_stock_yjyg()` | `stockCompanyInfo` | 获取业绩预告 |

**组成：**
- 资产负债表 (Balance Sheet / ZCFZ)
- 利润表 (Income Statement / LRB)
- 现金流量表 (Cash Flow Statement / XJLL)

---

### 3. 财务指标值对象 (Financial Indicator Value Object)

代表从财务报表计算出的各类财务指标。

**对应类：** `stockCompanyInfo`, `stockBorderInfo`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `get_stock_financial_analysis_indicator()` | `stockCompanyInfo` | 获取财务分析指标 |
| `get_stock_border_financial_indicator()` | `stockBorderInfo` | 获取市场财务指标 |

**指标类型：**
- 盈利能力：ROE、ROA、毛利率、净利率
- 偿债能力：资产负债率、流动比率、速动比率
- 运营能力：应收账款周转率、存货周转率、总资产周转率
- 成长能力：营收增长率、净利润增长率
- 估值指标：PE、PB、PS、股息率

---

### 4. 估值模型 (Valuation Model)

代表股票估值计算模型。

**对应类：** `stockDCFSimpleModel`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `calculate_dcf()` | `stockDCFSimpleModel` | 计算 DCF 价值 |
| `calculate_stock_price_range()` | `stockDCFSimpleModel` | 计算股价区间（保守/正常/乐观） |

**估值方法：**
- DCF（现金流折现）
- 情景分析（保守/正常/乐观）

---

### 5. 技术指标实体 (Technical Indicator Entity)

代表股票的技术分析指标和信号。

**对应类：** `stockAKIndicator`, `stockIndicatorQuantitative`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `strategy_mac()` | `stockAKIndicator` | 移动平均线策略 |
| `strategy_bollinger()` | `stockAKIndicator` | 布林带策略 |
| `strategy_macd()` | `stockAKIndicator` | MACD 策略 |
| `strategy_rsi()` | `stockAKIndicator` | RSI 策略 |
| `strategy_kdj()` | `stockAKIndicator` | KDJ 策略 |
| `strategy_breakout()` | `stockAKIndicator` | 突破策略 |
| `strategy_sar()` | `stockAKIndicator` | SAR 策略 |
| `strategy_williams_r()` | `stockAKIndicator` | 威廉指标策略 |
| `strategy_adx()` | `stockAKIndicator` | ADX 策略 |
| `mean_reversion_strategy()` | `stockAKIndicator` | 均值回归策略 |

**指标分类：**
- 趋势指标：MA、MACD、ADX、SAR
- 震荡指标：RSI、KDJ、Williams %R
- 波动指标：布林带、ATR
- 成交量指标：OBV、成交量比率

---

### 6. 交易策略聚合 (Trading Strategy Aggregate)

代表综合多种指标的交易策略和评分系统。

**对应类：** `StockStrategy`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `calculate_score()` | `StockStrategy` | 计算综合评分 |
| `calculate_score_indicate()` | `StockStrategy` | 指标评分计算 |
| `calculate_score_simple()` | `StockStrategy` | 简单评分计算 |
| `get_recommendation()` | `StockStrategy` | 获取投资建议 |
| `has_recent_buy_signal()` | `StockStrategy` | 检查买入信号 |
| `calculate_vol_inc()` | `StockStrategy` | 计算成交量得分 |

**策略组成：**
- 趋势策略（30%）
- RSI 策略（15%）
- MACD 策略（30%）
- KDJ 策略（15%）
- 成交量策略（10%）

---

### 7. 波浪分析实体 (Wave Analysis Entity)

代表股票价格的波浪形态分析。

**对应类：** `StockWaveAnalyzer`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `analysis_stock_wave()` | `StockWaveAnalyzer` | 分析股票波浪 |
| `analysis_stock_trend()` | `StockWaveAnalyzer` | 分析股票趋势 |
| `identify_waves()` | `StockWaveAnalyzer` | 识别波浪转折点 |
| `get_stock_trend()` | `StockWaveAnalyzer` | 获取整体趋势 |
| `get_last_trend()` | `StockWaveAnalyzer` | 获取最后趋势状态 |
| `show_waves()` | `StockWaveAnalyzer` | 可视化波浪 |

**波浪要素：**
- 波峰（Peak）
- 波谷（Trough）
- 上升浪（Up Wave）
- 下降浪（Down Wave）
- 波动百分比（Amplitude）

---

### 8. 板块/概念实体 (Sector/Concept Entity)

代表股票所属的行业板块和概念板块。

**对应类：** `stockConceptData`, `stockConcepService`, `stockCompanyInfo`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `stock_board_concept_name_ths()` | `stockConceptData` | 获取所有概念板块 |
| `stock_board_concept_cons_ths()` | `stockConceptData` | 获取概念成分股 |
| `stock_board_concept_info_ths()` | `stockConceptData` | 获取概念简介 |
| `get_all_sectors_and_stocks()` | `stockConcepService` | 获取所有板块和成分股 |
| `get_stock_board_all_concept_name()` | `stockCompanyInfo` | 获取所有概念板块 |
| `get_stock_board_all_industry_name()` | `stockCompanyInfo` | 获取所有行业板块 |
| `get_stock_concept_by_code()` | `stockCompanyInfo` | 按代码获取所属概念 |
| `get_stock_industry_by_code()` | `stockCompanyInfo` | 按代码获取所属行业 |

**板块类型：**
- 行业板块（Industry）
- 概念板块（Concept）

---

### 9. 新闻实体 (News Entity)

代表与股票相关的新闻资讯。

**对应类：** `stockNewsData`, `StockSentimentAnalysis`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `stock_news_em()` | `stockNewsData` | 获取个股新闻 |
| `get_stock_news()` | `stockCompanyInfo` | 获取个股新闻 |
| `get_comprehensive_news_data()` | `StockSentimentAnalysis` | 获取综合新闻数据 |

**新闻类型：**
- 公司新闻
- 公司公告
- 研究报告
- 行业新闻

---

### 10. 情绪分析值对象 (Sentiment Analysis Value Object)

代表基于新闻的市场情绪分析结果。

**对应类：** `StockSentimentAnalysis`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `get_sentiment_analysis()` | `StockSentimentAnalysis` | 获取情绪分析结果 |
| `calculate_advanced_sentiment_analysis()` | `StockSentimentAnalysis` | 计算高级情绪分析 |
| `calculate_sentiment_score()` | `StockSentimentAnalysis` | 计算情绪得分 |

**情绪维度：**
- 整体情绪（Overall Sentiment）
- 情绪趋势（Sentiment Trend）
- 置信度（Confidence Score）
- 正负向比例（Positive/Negative Ratio）

---

### 11. 资金流实体 (Fund Flow Entity)

代表股票的资金流入流出情况。

**对应类：** `stockBorderInfo`, `stockCompanyInfo`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `get_stock_all_info()` | `stockBorderInfo` | 获取所有股票资金流 |
| `get_stock_fund_flow()` | `stockCompanyInfo` | 获取行业资金流 |
| `get_stock_individual_fund_flow()` | `stockCompanyInfo` | 获取个股历史资金流 |
| `get_stock_hsgt_hold_stock_em()` | `stockBorderInfo` | 获取北向资金持仓 |
| `get_stock_hsgt()` | `stockCompanyInfo` | 获取沪深港通持股 |
| `get_stock_dzjy()` | `stockCompanyInfo` | 获取大宗交易 |
| `get_stock_gdzjc()` | `stockCompanyInfo` | 获取股东增减持 |

**资金流向：**
- 主力净流入
- 超大单净流入
- 大单净流入
- 中单净流入
- 小单净流入
- 北向资金（沪深港通）

---

### 12. 分红配送值对象 (Dividend Value Object)

代表股票的分红配送信息。

**对应类：** `stockBorderInfo`, `stockCompanyInfo`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `get_stock_fhps_info()` | `stockBorderInfo` | 获取分红配送数据 |
| `get_stock_fhps()` | `stockCompanyInfo` | 获取分红配送 |

**分红要素：**
- 现金分红金额
- 股息率
- 送股比例
- 转增比例
- 预案公告日
- 除权除息日

---

### 13. 可视化值对象 (Visualization Value Object)

代表技术指标的可视化输出。

**对应类：** `stockIndicatorHtml`, `stockIndicatorQuantitative`, `StockWaveAnalyzer`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `plot_sma()` | `stockIndicatorHtml` | 绘制移动平均线（HTML） |
| `plot_stock_wave()` | `stockIndicatorHtml` | 绘制小波分析图（HTML） |
| `plot_stock_Bollinger()` | `stockIndicatorHtml` | 绘制布林带（HTML） |
| `plot_stock_fft()` | `stockIndicatorHtml` | 绘制傅里叶变换图（HTML） |
| `plot_strategy_mac()` | `stockIndicatorQuantitative` | 绘制均线策略图 |
| `plot_strategy_bollinger()` | `stockIndicatorQuantitative` | 绘制布林带策略图 |
| `plot_strategy_macd()` | `stockIndicatorQuantitative` | 绘制 MACD 策略图 |
| `show_waves()` | `StockWaveAnalyzer` | 显示波浪图 |

**输出格式：**
- HTML（mpld3）
- Matplotlib 图表

---

### 14. 缓存仓储 (Cache Repository)

代表数据缓存的存储和读取机制。

**对应类：** `MySQLCache`, `FileCacheUtils`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `write_to_cache()` | `MySQLCache` | 写入 MySQL 缓存 |
| `read_from_cache()` | `MySQLCache` | 从 MySQL 读取缓存 |
| `write_to_csv()` | `FileCacheUtils` | 写入 CSV 缓存 |
| `read_from_csv()` | `FileCacheUtils` | 从 CSV 读取缓存 |
| `write_to_cache_serialized()` | `FileCacheUtils` | 序列化写入缓存 |
| `read_from_serialized()` | `FileCacheUtils` | 从序列化读取 |

**缓存策略：**
- 多级缓存（文件 + MySQL）
- 按日期分区
- 按报告类型分类

---

### 15. 数据初始化应用服务 (Data Initialization Application Service)

代表系统数据的批量初始化服务。

**对应类：** `stockDataInit`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `init_stock_by_day()` | `stockDataInit` | 初始化每日数据 |
| `init_stock_allmarket_by_day()` | `stockDataInit` | 初始化所有市场每日数据 |
| `init_stock_by_year()` | `stockDataInit` | 初始化年度数据 |
| `init_stock_allmarket_by_year()` | `stockDataInit` | 初始化所有市场年度数据 |

**初始化数据类型：**
- 每日：实时行情、北向资金、新闻
- 年度：财务报表、财务指标、分红数据
- 固定：板块数据、公司信息

---

### 16. 日期工具 (Date Utility)

代表报告日期的计算和转换工具。

**对应类：** `ReportDateUtils`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `get_report_year_str()` | `ReportDateUtils` | 获取报告年份字符串 |
| `get_current_report_year_st()` | `ReportDateUtils` | 获取当前报告期年份 |
| `get_history_date_str()` | `ReportDateUtils` | 获取历史日期 |
| `get_report_last_five_year()` | `ReportDateUtils` | 获取 5 年前年份 |

**日期规则：**
- A 股：年报截止 3月31日
- 港股/美股：年报截止 12月31日

---

### 17. 股票工具 (Stock Utility)

代表股票相关的通用工具函数。

**对应类：** `StockUtils`

**核心方法：**

| 方法 | 所属类 | 说明 |
|------|--------|------|
| `get_stock_zh_code()` | `StockUtils` | 获取带市场前缀的股票代码 |
| `format_history_stock_code()` | `StockUtils` | 格式化历史数据 |
| `pd_convert_to_float()` | `StockUtils` | 数值字段转换 |

---

## 领域模型关系图

```
┌─────────────────────────────────────────────────────────────────┐
│                        股票实体 (Stock)                          │
│  - 股票代码、名称、市场、行业、概念                               │
└──────────────┬────────────────────────────────┬─────────────────┘
               │                                │
      ┌────────▼────────┐              ┌───────▼────────┐
      │   财务报表聚合    │              │   技术指标实体   │
      │  (ZCFZ/LRB/XJLL) │              │  (MA/MACD/RSI) │
      └────────┬────────┘              └───────┬────────┘
               │                                │
      ┌────────▼────────┐              ┌───────▼────────┐
      │  财务指标值对象   │              │  交易策略聚合   │
      │   (ROE/PE/PB)   │              │  (评分/信号)   │
      └────────┬────────┘              └───────┬────────┘
               │                                │
      ┌────────▼────────┐              ┌───────▼────────┐
      │   估值模型      │              │  波浪分析实体   │
      │    (DCF)       │              │ (波峰/波谷)   │
      └─────────────────┘              └────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      板块/概念实体 (Sector)                      │
│  - 行业板块、概念板块、成分股关系                                 │
└──────────────┬────────────────────────────────┬─────────────────┘
               │                                │
      ┌────────▼────────┐              ┌───────▼────────┐
      │    新闻实体      │              │   资金流实体   │
      │ (公司/行业新闻)  │              │ (主力/北向资金)│
      └────────┬────────┘              └────────────────┘
               │
      ┌────────▼────────┐
      │  情绪分析值对象  │
      │  (情绪得分/趋势) │
      └─────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      基础设施层 (Infrastructure)                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  缓存仓储     │  │  数据初始化   │  │   工具类      │          │
│  │(MySQL/文件)  │  │  (批量导入)   │  │ (日期/股票)   │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 领域事件 (Domain Events)

| 事件 | 触发条件 | 处理逻辑 |
|------|----------|----------|
| 股票数据更新 | 每日定时任务 | 更新实时行情、资金流 |
| 财报发布 | 季度/年度 | 更新三大报表、财务指标 |
| 买入信号触发 | 技术指标满足条件 | 生成买入建议、评分 |
| 情绪变化 | 新闻数据更新 | 重新计算情绪得分 |
| 板块变动 | 成分股调整 | 更新板块归属关系 |

---

## 聚合根 (Aggregate Roots)

| 聚合根 | 包含实体/值对象 | 业务规则 |
|--------|-----------------|----------|
| Stock | 个股信息、历史数据、财务指标 | 代码唯一性、市场合法性 |
| FinancialReport | 资产负债表、利润表、现金流量表 | 三表勾稽关系 |
| TradingStrategy | 技术指标、评分、建议 | 评分阈值规则 |
| Sector | 板块信息、成分股列表 | 板块分类规则 |

