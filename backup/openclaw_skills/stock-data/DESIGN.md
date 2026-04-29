# Skill 设计方案: stock-data

## 概述

股票基础数据获取 Skill，提供个股信息查询、历史行情数据获取、市场全景数据查询等功能。

## 功能范围

1. **个股信息查询**
   - 获取股票基本信息（名称、行业、上市日期等）
   - 获取个股详细资料
   - 获取历史行情数据

2. **市场全景数据**
   - 获取所有股票实时行情
   - 获取股票代码列表
   - 获取市场资金流数据

3. **财务数据**
   - 获取三大报表（资产负债表、利润表、现金流量表）
   - 获取财务分析指标
   - 获取分红配送信息

## 输入参数

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| action | string | 是 | 操作类型：info/history/spot/report/financial/dividend |
| market | string | 是 | 市场代码：SH/SZ/H/usa/zq |
| symbol | string | 条件 | 股票代码，action=spot时可不传 |
| start_date | string | 否 | 开始日期，格式 YYYYMMDD |
| end_date | string | 否 | 结束日期，格式 YYYYMMDD |
| years | int | 否 | 获取历史年份数，默认5年 |

## 输出格式

```json
{
  "success": true,
  "data": {},
  "message": ""
}
```

## 使用示例

```bash
# 获取个股信息
python main.py --action=info --market=SH --symbol=601318

# 获取历史行情
python main.py --action=history --market=SH --symbol=601318 --start_date=20240101 --end_date=20241231

# 获取市场实时行情
python main.py --action=spot --market=SH

# 获取三大报表
python main.py --action=report --market=SH --symbol=601318 --years=5
```

## 依赖

- stock_analyse.stocklib.stock_company
- stock_analyse.stocklib.stock_border
- stock_analyse.stocklib.stock_annual_report

## 错误处理

| 错误码 | 说明 |
|--------|------|
| 1001 | 参数错误 |
| 1002 | 股票代码不存在 |
| 1003 | 数据获取失败 |
| 1004 | 网络请求超时 |
