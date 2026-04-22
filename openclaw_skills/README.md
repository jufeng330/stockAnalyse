# OpenClaw Stock Skills

基于 stocklib 封装的股票分析 OpenClaw Skills 集合。

## Skills 列表

| Skill | 功能 | 对应 stocklib 模块 |
|-------|------|-------------------|
| [stock-data](#stock-data) | 股票基础数据获取 | stock_company, stock_border, stock_annual_report |
| [stock-technical](#stock-technical) | 技术指标计算 | stock_ak_indicator |
| [stock-strategy](#stock-strategy) | 策略评分系统 | stock_strategy, stock_wave_analyser |
| [stock-valuation](#stock-valuation) | DCF 估值模型 | dcf_model |
| [stock-concept](#stock-concept) | 概念板块查询 | stock_concept_data, stock_concept_service |
| [stock-news](#stock-news) | 新闻情绪分析 | stock_news_data, stock_sentiment_analysis |
| [stock-wave](#stock-wave) | 波浪分析 | stock_wave_analyser |
| [stock-cache](#stock-cache) | 缓存管理 | mysql_cache, utils_file_cache |
| [stock-init](#stock-init) | 数据初始化 | stock_data_init |

---

## 安装

1. 确保 stock_analyse 项目可用
2. 将本目录下的 skills 复制到 OpenClaw 的 skills 目录
3. 配置 Python 路径指向 stock_analyse

---

## Skill 详情

### stock-data

股票基础数据获取 Skill。

**Actions:**
- `info` - 获取个股基本信息
- `history` - 获取历史行情
- `spot` - 获取市场实时行情
- `report` - 获取三大报表
- `financial` - 获取财务指标
- `dividend` - 获取分红配送
- `fund_flow` - 获取资金流向
- `holders` - 获取股东信息

**示例:**
```bash
python main.py --action=info --market=SH --symbol=601318
python main.py --action=history --market=SH --symbol=601318 --start_date=20240101
python main.py --action=spot --market=SH
```

---

### stock-technical

技术指标计算 Skill。

**Actions:**
- `ma` - 移动平均线
- `macd` - MACD 指标
- `rsi` - RSI 相对强弱指标
- `kdj` - KDJ 随机指标
- `bollinger` - 布林带
- `breakout` - 突破策略
- `sar` - SAR 抛物线转向
- `williams` - 威廉指标
- `adx` - ADX 趋向指标
- `all` - 计算所有指标

**示例:**
```bash
python main.py --action=macd --market=SH --symbol=601318
python main.py --action=rsi --market=SH --symbol=601318 --params='{"period": 14}'
python main.py --action=all --market=SH --symbol=601318
```

---

### stock-strategy

策略评分系统 Skill。

**Actions:**
- `score` - 计算综合评分
- `signals` - 获取买入信号
- `recommend` - 获取投资建议
- `batch` - 批量分析市场

**评分标准:**
- >= 50: 强烈推荐买入
- >= 30: 建议买入
- >= 10: 建议持有
- < 10: 建议观望

**示例:**
```bash
python main.py --action=score --market=SH --symbol=601318
python main.py --action=recommend --market=SH --symbol=601318
python main.py --action=batch --market=SH --min_score=50
```

---

### stock-valuation

DCF 估值模型 Skill。

**Actions:**
- `dcf` - DCF 估值计算
- `price_range` - 股价区间预测
- `compare` - 估值比较分析

**示例:**
```bash
python main.py --action=dcf --market=SH --symbol=601318
python main.py --action=price_range --market=SH --symbol=601318
python main.py --action=compare --market=SH --symbol=601318
```

---

### stock-concept

概念板块查询 Skill。

**Actions:**
- `list_concept` - 概念板块列表
- `list_industry` - 行业板块列表
- `components` - 板块成分股
- `by_stock` - 查询股票所属板块
- `detail` - 板块详情

**示例:**
```bash
python main.py --action=list_concept --market=SH
python main.py --action=components --market=SH --name=人工智能
python main.py --action=by_stock --market=SH --symbol=601318
```

---

### stock-news

新闻情绪分析 Skill。

**Actions:**
- `news` - 获取新闻
- `sentiment` - 情绪分析
- `comprehensive` - 综合分析

**示例:**
```bash
python main.py --action=news --market=SH --symbol=601318 --days=15
python main.py --action=sentiment --market=SH --symbol=601318 --days=15
```

---

### stock-wave

波浪分析 Skill。

**Actions:**
- `analyze` - 波浪分析
- `trend` - 趋势分析
- `visualize` - 可视化

**示例:**
```bash
python main.py --action=analyze --market=SH --symbol=601318 --days=200
python main.py --action=trend --market=SH --symbol=601318 --days=200
```

---

### stock-cache

缓存管理 Skill。

**Actions:**
- `status` - 查询缓存状态
- `refresh` - 刷新缓存
- `clear` - 清除缓存
- `warmup` - 预热缓存

**示例:**
```bash
python main.py --action=status --data_type=spot --market=SH
python main.py --action=clear --data_type=all
```

---

### stock-init

数据初始化 Skill。

**Actions:**
- `daily` - 每日数据初始化
- `yearly` - 年度数据初始化
- `fixed` - 固定数据初始化
- `all` - 全量初始化

**示例:**
```bash
python main.py --action=daily --market=SH
python main.py --action=yearly --market=SH --date=20240331
python main.py --action=all --market=all
```

---

## 通用参数说明

### 市场代码 (market)

| 代码 | 市场 |
|------|------|
| SH | 上海主板 |
| SZ | 深圳主板/创业板 |
| H | 港股 |
| usa | 美股 |
| zq | 债券/ETF |

### 日期格式

所有日期参数使用 `YYYYMMDD` 格式，例如：`20240101`

### 返回格式

所有 Skill 返回统一的 JSON 格式：

```json
{
  "success": true|false,
  "data": {},
  "message": "提示信息"
}
```

---

## 依赖

- Python 3.8+
- stock_analyse 项目
- OpenClaw 运行时环境

---

## 目录结构

```
openclaw_skills/
├── README.md
├── stock-data/
│   ├── _meta.json
│   ├── SKILL.md
│   ├── DESIGN.md
│   └── main.py
├── stock-technical/
│   ├── _meta.json
│   ├── SKILL.md
│   ├── DESIGN.md
│   └── main.py
├── stock-strategy/
│   ├── _meta.json
│   ├── SKILL.md
│   ├── DESIGN.md
│   └── main.py
├── stock-valuation/
│   ├── _meta.json
│   ├── SKILL.md
│   ├── DESIGN.md
│   └── main.py
├── stock-concept/
│   ├── _meta.json
│   ├── SKILL.md
│   ├── DESIGN.md
│   └── main.py
├── stock-news/
│   ├── _meta.json
│   ├── SKILL.md
│   ├── DESIGN.md
│   └── main.py
├── stock-wave/
│   ├── _meta.json
│   ├── SKILL.md
│   ├── DESIGN.md
│   └── main.py
├── stock-cache/
│   ├── _meta.json
│   ├── SKILL.md
│   ├── DESIGN.md
│   └── main.py
└── stock-init/
    ├── _meta.json
    ├── SKILL.md
    ├── DESIGN.md
    └── main.py
```

---

## 开发说明

每个 Skill 包含以下文件：

1. **`_meta.json`** - Skill 元数据定义，包含名称、版本、参数、返回值等
2. **`SKILL.md`** - Skill 使用说明文档
3. **`DESIGN.md`** - Skill 设计方案文档
4. **`main.py`** - Skill 主程序入口

### 添加新 Skill

1. 创建新目录 `stock-{name}/`
2. 创建上述 4 个文件
3. 在 `main.py` 中实现业务逻辑
4. 更新本 README

---

## License

MIT
