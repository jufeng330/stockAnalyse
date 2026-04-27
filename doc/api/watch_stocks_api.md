# Watch Stocks API

本文件仅描述当前已经实现的关注股票列表 Phase 1 接口，不包含尚未落地的进场决策、股票分析记录、持仓计划分析记录子流程。

## 页面入口

- `GET /watch-stocks`
- `GET /index`

两个入口都渲染真实模板页 `templates/watch_stocks.html`，不再返回静态原型 HTML。

## 统一返回结构

成功：

```json
{
  "success": true,
  "data": {},
  "message": ""
}
```

失败：

```json
{
  "success": false,
  "message": "关注股票不存在",
  "error": {
    "code": "not_found",
    "message": "关注股票不存在"
  }
}
```

## 数据模型字段

`watch_stocks` 当前已实现字段：

- `id`: 关注股票 ID，格式示例 `WS-ABC123DEF456`
- `stock_code`: 股票代码
- `stock_name`: 股票名称
- `market`: 市场
- `industry`: 行业
- `asset_type`: 资产类型
- `source`: 来源
- `note`: 备注
- `status`: 当前状态
- `current_price`: 当前价格
- `pe`: PE
- `current_stage`: 当前阶段
- `current_price_zone`: 当前价格区间
- `suggested_action`: 当前建议
- `last_conclusion_summary`: 最新结论摘要
- `last_analysis_at`: 最近分析时间
- `created_at`: 创建时间
- `updated_at`: 更新时间

### 当前可用状态值

- `watching`
- `archived`

默认列表只返回 `watching` 状态；归档使用软删除，不做物理删除。

## 1. 股票检索接口

`GET /api/trading-decision/watch-stocks/stock-search`

用于“新增关注股票”时按股票代码或名称搜索候选股票。

### Query 参数

- `query`: 搜索词；支持代码前缀/精确匹配、名称包含匹配
- `market`: 市场，可传 `A股` / `H` / `港股` / `usa` / `美股` / `SH` / `SZ`
- `limit`: 返回条数上限，默认 `20`

### 数据来源

当前实现**只复用** `get_stock_spot()`：

- A股：`stock_zh_a_spot_em()` / fallback `stock_zh_a_spot()`
- 港股：`stock_hk_main_board_spot_em()`
- 美股：`stock_us_spot_em()`

### 返回示例

```json
{
  "success": true,
  "data": [
    {
      "code": "300750",
      "name": "宁德时代",
      "market": "A股",
      "display_label": "300750 - 宁德时代 (A股)",
      "source": "spot",
      "current_price": 182.4,
      "pe": 21.8
    }
  ],
  "message": ""
}
```

### 字段说明

- `current_price`: 优先从 spot 结果中的 `最新价 / 最新价格 / 当前价 / 收盘价` 映射
- `pe`: 优先从 spot 结果中的 `市盈率-动态 / 动态市盈率 / 市盈率` 映射

空 query 直接返回空数组，不返回全市场股票清单。

## 1.1 新增关注股票弹窗当前表单约束

### 资产类型固定选项

- `指数资产`
- `红利资产`
- `成长龙头`
- `周期/资源`
- `高弹性主题`

### 当前默认隐藏字段

当前页面的“新增关注股票”弹窗已隐藏以下字段，但后端接口仍接受它们：

- `current_stage`
- `current_price_zone`
- `suggested_action`
- `last_analysis_at`
- `last_conclusion_summary`
- `note`

### 自动回填字段

当前页面会在用户从搜索结果选择股票后自动回填：

- `stock_code`
- `stock_name`
- `market`
- `current_price`
- `pe`

其中 `current_price` 与 `pe` 在当前页面中为只读展示值，提交时会一并带给后端。

空 query 直接返回空数组，不返回全市场股票清单。

## 2. 查询关注股票列表

`GET /api/trading-decision/watch-stocks`

### Query 参数

- `keyword`: 模糊匹配 `stock_code / stock_name / industry / note`
- `market`: 精确匹配市场
- `asset_type`: 精确匹配资产类型
- `stage`: 精确匹配 `current_stage`
- `price_zone`: 精确匹配 `current_price_zone`
- `status`: 精确匹配状态；不传时默认 `watching`
- `page`: 页码，默认 `1`
- `page_size`: 每页条数，默认 `20`，最大 `100`

### 返回示例

```json
{
  "success": true,
  "data": {
    "items": [
      {
        "id": "WS-3BE39AA0C77B",
        "stock_code": "300750",
        "stock_name": "宁德时代",
        "market": "A股",
        "industry": "新能源",
        "asset_type": "成长型",
        "status": "watching",
        "suggested_action": "适合做第一笔决策"
      }
    ],
    "summary": {
      "watch_count": 1,
      "decision_ready_count": 1,
      "analysis_completed_count": 0,
      "planned_count": 0
    },
    "pagination": {
      "page": 1,
      "page_size": 20,
      "total": 1
    },
    "filters": {
      "keyword": "",
      "market": "",
      "asset_type": "",
      "stage": "",
      "price_zone": "",
      "status": "",
      "page": 1,
      "page_size": 20
    }
  },
  "message": ""
}
```

## 3. 新增关注股票

`POST /api/trading-decision/watch-stocks`

### 必填字段

- `stock_code`
- `stock_name`
- `market`
- `asset_type`

### 请求示例

```json
{
  "stock_code": "300750",
  "stock_name": "宁德时代",
  "market": "A股",
  "industry": "新能源",
  "asset_type": "成长龙头",
  "source": "manual",
  "current_price": 182.4,
  "pe": 21.8
}
```

### 说明

- 当前页面中的创建弹窗默认只展示必要字段与自动回填字段，因此上面的示例更贴近当前真实前端请求。
- `note`、`current_stage`、`current_price_zone`、`suggested_action`、`last_conclusion_summary`、`last_analysis_at` 仍可由 API 直接提交，但当前新增弹窗默认隐藏这些字段。
- `current_price` 与 `pe` 通常来自股票检索接口返回值，前端会在选中股票后自动带入。

## 4. 查询单个关注股票

`GET /api/trading-decision/watch-stocks/<id>`

- 不存在时返回 `404` + `error.code = not_found`

## 5. 更新关注股票

`PUT /api/trading-decision/watch-stocks/<id>`

支持更新当前已实现的基础字段，包括：

- `stock_code`
- `stock_name`
- `market`
- `industry`
- `asset_type`
- `source`
- `note`
- `current_price`
- `pe`
- `current_stage`
- `current_price_zone`
- `suggested_action`
- `last_conclusion_summary`
- `last_analysis_at`

## 6. 归档关注股票

`POST /api/trading-decision/watch-stocks/<id>/archive`

- 将 `status` 更新为 `archived`
- 默认列表页和列表 API 不再展示该条记录

## 页面按钮说明

当前列表中的三个业务按钮已接通跳转，但仍是占位入口：

- `/entry-decision?watch_stock_id=<id>`
- `/stock-analysis-record?watch_stock_id=<id>`
- `/trade-plan-analysis?watch_stock_id=<id>`

这些跳转不代表对应子流程已在本阶段实现，只表示页面入口已经预留好 `watch_stock_id` 参数。
