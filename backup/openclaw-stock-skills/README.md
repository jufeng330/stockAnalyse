# OpenClaw Stock Skills - 股票分析技能包

## 概述

这是一个符合 OpenClaw 规范的股票分析技能包，包含6个专业技能，覆盖从基础数据到高级策略的完整分析链路。

## 技能列表

| 技能名称 | 功能描述 | 适用场景 |
|---------|---------|---------|
| **stock-basic** | 基础数据 | 个股信息、历史行情、市场全景、板块查询 |
| **stock-financial** | 财务数据 | 三大报表、财务指标、历史趋势、指标排名 |
| **stock-technical** | 技术分析 | 均线、MACD、RSI、KDJ、布林带等 |
| **stock-strategy** | 交易策略 | 双均线、布林带、RSI等策略回测 |
| **stock-sentiment** | 市场情绪 | 新闻情感、舆情风险、情绪指标 |
| **stock-sector** | 板块分析 | 行业/概念板块、成分股、板块资金流 |

## 目录结构

```
openclaw-skills/
├── stock-basic/
│   ├── _meta.json          # 技能元数据
│   ├── SKILL.md            # 技能文档
│   └── scripts/
│       └── main.py         # 执行脚本
├── stock-financial/
│   ├── _meta.json
│   ├── SKILL.md
│   └── scripts/
│       └── main.py
├── stock-technical/
│   ├── _meta.json
│   ├── SKILL.md
│   └── scripts/
│       └── main.py
├── stock-strategy/
│   ├── _meta.json
│   ├── SKILL.md
│   └── scripts/
│       └── main.py
├── stock-sentiment/
│   ├── _meta.json
│   ├── SKILL.md
│   └── scripts/
│       └── main.py
├── stock-sector/
│   ├── _meta.json
│   ├── SKILL.md
│   └── scripts/
│       └── main.py
└── README.md
```

## 快速开始

### 1. 安装依赖

```bash
pip install akshare pandas numpy beautifulsoup4 requests jieba snownlp
```

### 2. 使用示例

#### 获取股票基本信息

```python
from openclaw-skills.stock-basic.scripts.main import get_stock_info

info = get_stock_info('SH', '601318')
print(info)
```

#### 获取历史行情

```python
from openclaw-skills.stock-basic.scripts.main import get_stock_history

history = get_stock_history('SH', '601318', '20240101', '20241231')
print(history)
```

#### 获取财务指标

```python
from openclaw-skills.stock-financial.scripts.main import get_financial_indicators

indicators = get_financial_indicators('SH', '601318', '20240101', '20241231')
print(indicators)
```

#### 计算技术指标

```python
from openclaw-skills.stock-technical.scripts.main import calculate_indicators

df_indicators = calculate_indicators('SH', '601318', '20240101', '20241231')
print(df_indicators)
```

## 技能详细说明

### stock-basic - 基础数据

**功能：**
- 个股信息查询（名称、行业、概念）
- 历史行情数据（日线、周线、月线）
- 市场全景数据（所有股票实时行情）
- 板块查询（概念板块、行业板块）
- 资金流向数据（主力净流入、北向资金）

**适用场景：**
- 股票基本信息查询
- 历史数据获取
- 市场整体分析
- 板块成分股查询

### stock-financial - 财务数据

**功能：**
- 三大财务报表（资产负债表、利润表、现金流量表）
- 财务指标分析（ROE、PE、PB、净利润等）
- 历史财务趋势对比
- 指标市场排名

**适用场景：**
- 财报分析
- 基本面选股
- 行业对比分析
- 价值投资研究

### stock-technical - 技术分析

**功能：**
- 趋势指标（MA、EMA、MACD、布林带）
- 动量指标（RSI、KDJ、CCI）
- 成交量指标（OBV、VWAP）
- 自定义参数计算

**适用场景：**
- 技术面分析
- 趋势判断
- 买卖点识别
- 技术指标组合

### stock-strategy - 交易策略

**功能：**
- 双均线策略
- 布林带策略
- RSI策略
- 策略回测（收益、回撤、夏普比率）
- 自定义策略开发

**适用场景：**
- 量化策略开发
- 策略回测验证
- 交易信号生成
- 风险控制

### stock-sentiment - 市场情绪

**功能：**
- 新闻抓取与情感分析
- 评论热度分析
- 舆情风险监测
- 情绪指标量化

**适用场景：**
- 市场热点判断
- 舆情风险预警
- 情绪择时
- 新闻事件分析

### stock-sector - 板块分析

**功能：**
- 行业/概念板块查询
- 成分股明细
- 板块资金流分析
- 板块相对强度

**适用场景：**
- 板块轮动分析
- 行业比较研究
- 板块资金追踪
- 配置组合构建

## 依赖项

所有技能共同依赖：
- `akshare` - 股票数据源
- `pandas` - 数据处理
- `numpy` - 数值计算

部分技能额外依赖：
- `beautifulsoup4` - 网页解析（stock-basic）
- `requests` - HTTP请求（stock-basic）
- `jieba` - 中文分词（stock-sentiment）
- `snownlp` - 情感分析（stock-sentiment）

## 注意事项

1. **数据延迟**：数据来源于第三方，可能存在延迟
2. **请求频率**：注意控制请求频率，避免触发限流
3. **投资风险**：数据和分析仅供参考，不构成投资建议
4. **市场代码**：确保使用正确的市场代码（SH/SZ/H/usa/zq）
5. **日期格式**：日期参数使用 YYYYMMDD 格式

## 版本历史

- v1.0.0 (2024-01-01) - 初始版本发布

## 技术支持

如遇问题，请检查：
1. 依赖包是否完整安装
2. 网络连接是否正常
3. 市场代码和日期格式是否正确
4. 是否触发了数据源的请求限制

## 许可证

本技能包仅供学习和研究使用。

---

**免责声明**：本技能包提供的数据和分析仅供参考，不构成任何投资建议。投资有风险，入市需谨慎。
