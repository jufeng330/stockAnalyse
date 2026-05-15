# 股票数据接口说明文档

本文档说明了如何使用本系统 `stockCompanyInfo` 等服务及底层的 `akshare` 接口分别获取指定股票的财报、成交历史、股息率、市场信息等。

## 1. 内部 Python API 说明 (基于 `company_data_service.py`)

首先需要初始化类并提供股票代码和市场参数。

```python
from stock_analyse.infrastructure.services.company_data_service import stockCompanyInfo

# marker支持: "SH", "SZ", "H", "usa" 等市场标识
# symbol: 股票代码（例如：A股 "601668" 或 美股 "TSLA" 等）
stock_service = stockCompanyInfo(marker='SH', symbol='601668')
```

### 获取股票的财报数据 (财务指标分析)
该接口返回包含净利润、营业收入、ROE、资产负债等详细财务分析指标的DataFrame。
```python
# start_year 指定想要查看的财报年份，通常会结合过去5年的窗口返回数据
df_financial = stock_service.get_stock_financial_analysis_indicator(start_year="2024")
print(df_financial)
```

### 获取股票成交历史 (历史K线及技术指标)
该接口返回包含历史开盘、收盘、最高、最低价格及各种技术指标计算结果的DataFrame。
```python
# 传入开始和结束日期，格式 "YYYYMMDD"
df_history = stock_service.get_stock_history_data(start_date_str="20240101", end_date_str="20241231")
print(df_history)
```

### 获取股息率等估值数据
该接口主要用来获取市盈率(PE)、市净率(PB)、股息率及总市值等数据。
```python
# 估值数据包括：trade_date, pe, pe_ttm, pb, ps, ps_ttm, dv_ratio (股息率), dv_ttm (股息率TTM), total_mv (总市值) 等
df_indicator = stock_service.get_stock_indicator_data()
print(df_indicator)
```

### 获取公司个股市场基本信息
该接口返回公司注册地址、主营业务、成立日期、董事长、最新股本等基本面的市场信息。
```python
# 对于A股，通常使用 em 结尾的方法调用东方财富接口获取更详细的资讯
df_info = stock_service.get_stock_individual_info_em()
print(df_info)
```

---

## 2. Web HTTP 接口调用案例 (Curl)

系统通过 Flask 提供了一些 HTTP 接口可以调用上述的数据和分析流程。

### 接口: `/api/analyze_stock_ai`
该接口会对指定的股票结合财报、成交历史和市场行情发起一次深度的 AI 分析。

**请求案例:**
```bash
curl -X POST http://127.0.0.1:5000/api/analyze_stock_ai \
  -H "Content-Type: application/json" \
  -d '{
    "stock_code": "601668",
    "market": "SH",
    "client_id": "test_client_001",
    "analysis_depth": "standard"
  }'
```

**返回说明:**
```json
{
  "success": true,
  "data": "",
  "message": "股票 601668 AI分析已启动",
  "task_mode": "async",
  "client_id": "test_client_001"
}
```

*注：* 该接口为异步请求。分析的进度以及包括财报、指标在内的具体结果数据，将通过系统基于 `client_id` 建立的 SSE (Server-Sent Events) 流 (`/api/sse?client_id=test_client_001`) 推送给客户端。

---

### 接口: `/api/history/analyse` (GET)
该接口返回某只股票在特定日期的历史报告结果页面，里面包含了历史财务和技术分析的详细结果。

**请求案例:**
```bash
curl -X GET "http://127.0.0.1:5000/api/history/analyse?stock=601668&date=20240101"
```

**返回说明:**
返回对应的渲染后的 HTML 内容，其中包含指定日期的财报摘要、行情估值以及分析记录。
