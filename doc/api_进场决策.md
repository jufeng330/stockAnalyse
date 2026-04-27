# 进场决策 API 业务逻辑文档与技术实现方案

## 1. 文档目标

本文用于重构当前进场决策接口：

- `POST /api/trading-decision/watch-stocks/<id>/entry-decision/analyze`

当前问题是：

- 该接口直接复用了单股分析 `/api/analyze_stock_ai` 的 AI 处理链路
- 输出是“通用个股分析结果”，而不是“进场决策卡”结构
- 输出结构与 `进场决策_600900.md` 这类实战决策模板不一致
- 缺少明确的人机协同暂停点
- 缺少面向“是否现在买、买第几笔、买多少、怎么应对、何时卖”的专属角色链

本方案目标是将进场决策改造成：

1. **AI 角色驱动** 的专属业务链
2. 输出结构对齐 `进场决策_600900.md` 的章节化结果
3. 支持 **人工补充信息后继续分析**
4. 继续复用系统现有的：
   - Flask 路由模式
   - 异步任务执行器 `executor`
   - SSE 通道 `/api/sse`
   - 已有市场数据 / 财务数据 / 股票基础信息访问能力
5. 不再直接复用当前单股分析 8 角色通用链路

---

## 2. 当前实现问题说明

当前实现位于：

- `src/stock_analyse/interfaces/web/routes/trading_decision.py:125`

当前逻辑是：

1. 根据 `watch_stock_id` 读取 `watch_stocks`
2. 调用 `build_stock_ai_payload(...)`
3. 调用 `start_stock_ai_analysis(...)`
4. 实际进入：
   - `src/stock_analyse/interfaces/web/routes/analysis.py:73`
   - `context.analyzer.stock_ai_analysis_process(...)`
5. 触发的是当前“单股分析 AI 流程”

这个流程的问题不是技术不能运行，而是**业务语义错误**：

- 单股分析的目标是“生成通用研究结论”
- 进场决策的目标是“生成可执行的建仓决策卡”
- 两者输出结构、AI 角色职责、数据依赖、人机交互方式均不同

因此，进场决策应该有自己的：

- 专属角色链
- 专属上下文数据模型
- 专属中间状态
- 专属暂停/继续机制
- 专属最终结果结构

---

## 3. 目标输出结构

目标结果应对齐：

- `/mnt/github/stock/进场决策_600900.md`
- `/mnt/github/stock/进场决策模板_空白实战版.md`

即最终不是输出一个通用 `decision/scores/signals` 结构，而是输出一个 **结构化进场决策报告对象**，至少包含以下章节：

1. 标的基本信息
2. Step 1 宏观判断
3. Step 2 资产归类
4. Step 3 价值阶段判断
5. Step 4 价格分区
6. Step 5 三笔买入计划
7. Step 6 涨后应对
8. Step 7 跌后应对
9. 卖出规则
10. 最终一页决策卡
11. 复盘区模板

因此接口返回应该围绕“章节化决策对象”设计，而不是围绕“通用分析对象”设计。

同时，**每个章节的 JSON 输出不能只有结论字段，还必须包含支撑该结论的 reasoning / reason 字段**，用于解释：

- 为什么得出该结论
- 依据了哪些输入信息
- 为什么不是相邻的其他结论
- 该结论与后续动作之间的逻辑关系

---

## 4. 总体业务模式

## 4.1 模式选择

推荐采用：

**异步角色链 + SSE 流式日志 + 可暂停的人机协同状态机**

理由：

- 进场决策属于多阶段长任务
- 中途可能需要用户补充财务、估值、仓位信息
- 与当前系统现有的 SSE + executor 模式兼容
- 用户体验与现有单股分析页面一致，改造成本低

## 4.2 总体流程

```text
页面发起 analyze 请求
-> 后端创建 entry decision session
-> 角色 1 自动执行
-> 角色 2 自动执行
-> 角色 3 检查是否缺少人工信息
   -> 若缺：发出 pause 事件，请用户补充
   -> 若全：继续执行
-> 角色 4 检查估值信息
   -> 若缺：发出 pause 事件，请用户补充
-> 角色 5 检查仓位信息
   -> 若缺：发出 pause 事件，请用户补充
-> 角色 6 自动执行
-> 聚合结果为完整决策卡
-> SSE 发 final_result
-> 用户确认后保存记录
```

---

## 5. 六大 AI 角色设计

## 5.1 角色一：宏观 AI 分析师

### 核心职责

判断当前大环境是否适合研究和投资该类标的。

### 输入

优先复用系统当前已有或容易从现有服务获得的数据：

- `trade_date`
- 市场类型 `market`
- 市场风格 / 风险偏好 / 流动性 / 利率方向
- 大盘环境摘要
- 行业风格标签

### 推荐数据来源

优先级建议：

1. **短期 MVP**：
   - 由服务层根据当前市场生成简化宏观上下文
   - 使用已有市场数据接口/方法的汇总结果
2. **已实现能力优先复用**：
   - `stockBorderInfo(...).get_stock_spot()` 中市场侧数据
   - 当前 stock analyzer / snapshot 中已有的 `market_context`
3. **若当前没有稳定宏观结构化接口**：
   - 允许由后端拼装简版宏观上下文 DTO

### 输出

```json
{
  "macro_quick_view": "...",
  "macro_conclusion": {
    "benefit_asset_types": ["红利", "防御"],
    "avoid_asset_types": ["高估值主题"],
    "suggested_total_position": "60%",
    "fit_with_target": "是"
  },
  "macro_reasoning": "说明为什么当前宏观环境更适合哪些资产、不适合哪些资产，以及该标的为什么顺应或不顺应当前环境。",
  "research_needed": true,
  "reason": "..."
}
```

### 人机边界

- 完全自动化
- 不需要人工补充

---

## 5.2 角色二：资产分类 AI 分析师

### 核心职责

根据股票基础属性，定义其投资打法。

### 输入

- `watch_stock` 基础信息
  - `stock_code`
  - `stock_name`
  - `market`
  - `industry`
  - `asset_type`
- 公司基础资料
- 概念标签
- 龙头地位 / 商业模式摘要
- 角色一输出

### 推荐数据来源

优先复用：

1. `watch_stocks` 表现有字段：
   - `industry`
   - `asset_type`
2. 当前单股分析快照中已有：
   - `company_profile`
   - `company_name`
   - `business_intro`
   - `industry`
   - `concepts`
3. 若缺少部分资料，可从现有公司资料服务补充

### 输出

```json
{
  "asset_classification": "A股医药CXO行业龙头成长股",
  "classification_reasoning": "说明为什么该标的应被归类到该资产类型，这类资产历史上主要靠什么上涨，以及为什么适合当前推荐打法。",
  "upside_logic": ["盈利增长", "估值修复"],
  "risk_logic": ["政策扰动", "订单波动", "估值压缩"],
  "recommended_playbook": ["小仓试错再验证", "趋势确认后加仓"],
  "forbidden_playbook": ["一次性重仓赌博"]
}
```

### 人机边界

- 完全自动化

---

## 5.3 角色三：价值阶段 AI 分析师

### 核心职责

按照“五阶段 + 四维度”判断当前价值位置。

### 输入

自动输入：

- 技术面信号
- 市场情绪
- 公司基础资料
- 角色一、角色二输出

人工补充输入：

- 最新财报关键数据
  - 收入增速
  - 利润增速
  - 现金流情况
  - 毛利率 / 净利率变化
- 机构预期摘要

### 推荐数据来源

自动部分优先复用：

1. 当前 quant / indicator 能力
   - MA
   - MACD
   - RSI
   - KDJ
   - ADX
   - 成交量
2. 当前 analyzer snapshot 中已有：
   - `technical`
   - `sentiment`
3. 当前财务服务中已有财报数据时，先自动预填；若缺失或不完整，再人工补充

### 输出

```json
{
  "value_stage": "B",
  "value_stage_label": "修复初期",
  "four_dimension_assessment": {
    "fundamental": {"status": "改善", "reason": "说明财务与经营层面的判断依据"},
    "expectation": {"status": "中性偏乐观", "reason": "说明市场预期和机构预期的判断依据"},
    "valuation": {"status": "中位", "reason": "说明估值位置和同行对比依据"},
    "price_action": {"status": "企稳", "reason": "说明趋势、量能、止跌与价格行为依据"}
  },
  "stage_reason": "...",
  "stage_reasoning": "说明为什么四维判断最终落到该阶段，而不是相邻阶段。",
  "suggested_action_in_stage": "第一笔"
}
```

### 人机边界

- 若财务数据或预期摘要缺失，必须暂停并提示补充
- 这是第一个关键 pause 点

---

## 5.4 角色四：价格分区 AI 分析师

### 核心职责

把价值判断映射到具体价格区间与动作信号。

### 输入

自动输入：

- 当前价格
- 角色三输出
- 技术支撑阻力位

人工补充输入：

- 当前估值数据 / 估值判断
  - PE / PB / PEG / 股息率等
  - 或者用户直接给“偏低/合理/偏高”的估值判断

### 推荐数据来源

优先复用当前已实现能力：

1. `watch_stocks.current_price`
2. 股票检索接口当前回填的 `pe`
3. `stockBorderInfo(...).get_stock_spot()` 中价格与估值列
4. 若已有历史估值分析能力，也可接入

### 输出

```json
{
  "price_zone": "合理偏低区",
  "zone_reasoning": "说明为什么当前价格被归入这个区间，而不是便宜区、合理区或高估区。",
  "action_signal": "可以买第一笔",
  "action_reasoning": "说明这个价格区间为什么对应当前动作信号，以及应不应该立即出手。",
  "cheap_reason": "...",
  "danger_reason": "...",
  "regret_if_not_buy": "...",
  "mistake_if_buy_wrong": "..."
}
```

### 人机边界

- 若缺估值信息，暂停要求补充
- 这是第二个关键 pause 点

---

## 5.5 角色五：买卖计划 AI 分析师

### 核心职责

生成三笔买入计划与今日执行指令。

### 输入

自动输入：

- 角色三输出
- 角色四输出
- 当前价格 / 技术关键位

人工补充输入：

- 用户当前仓位
- 用户最大目标仓位
- 可选：投资目标（短线/波段/中期/长期）

### 推荐数据来源

- 当前仓位 / 最大目标仓位：由页面表单录入
- 默认值可来自页面已有保存字段，或用户偏好配置

### 输出

```json
{
  "target_position": {
    "current_position": "0%",
    "max_target_position": "15%",
    "remaining_position": "15%"
  },
  "position_reasoning": "说明目标仓位为什么这样设定，为什么不是更激进或更保守。",
  "buy_plan": {
    "first_leg": {
      "decision": "执行",
      "ratio": "5%",
      "trigger": "...",
      "reasoning": "说明第一笔为什么现在可以上，以及它承担的是试错还是确认功能。"
    },
    "second_leg": {
      "decision": "条件触发后执行",
      "ratio": "5%",
      "trigger": "...",
      "reasoning": "说明第二笔需要什么确认条件，为什么不能和第一笔一起买。"
    },
    "third_leg": {
      "decision": "条件触发后执行",
      "ratio": "5%",
      "trigger": "...",
      "reasoning": "说明第三笔为什么要等趋势或逻辑进一步兑现。"
    }
  },
  "today_instruction": {
    "should_buy_today": true,
    "buy_leg": "第一笔",
    "buy_ratio": "5%",
    "next_watch_point": "...",
    "reasoning": "说明今天为什么该买/不该买，为什么是这一笔而不是下一笔。"
  }
}
```

### 人机边界

- 若缺仓位信息，暂停要求补充
- 这是第三个关键 pause 点

---

## 5.6 角色六：风险控制 AI 分析师

### 核心职责

生成上涨应对、下跌应对、卖出规则和最终退出路径。

### 输入

- 角色二资产特征
- 角色五买入计划
- 当前价格关键位 / 趋势信号

### 输出

```json
{
  "post_buy_rise_plan": {
    "action": "...",
    "reasoning": "说明上涨后为什么应该继续持有、暂停加仓或小量补仓。"
  },
  "post_buy_fall_plan": {
    "action": "...",
    "reasoning": "说明下跌后为什么是补仓、暂停还是止损，而不是情绪化操作。"
  },
  "sell_rules": {
    "logic_falsification": {
      "trigger": "...",
      "action": "...",
      "reasoning": "说明这是核心逻辑证伪信号，为什么触发后必须执行卖出或减仓。"
    },
    "valuation_overstretch": {
      "trigger": "...",
      "action": "...",
      "reasoning": "说明估值透支时为什么不能继续加仓，为什么要减仓。"
    },
    "technical_breakdown": {
      "trigger": "...",
      "action": "...",
      "reasoning": "说明哪些技术变化意味着趋势已经坏掉。"
    }
  },
  "exit_plan": {
    "first_reduce_point": "...",
    "second_reduce_point": "...",
    "full_exit_condition": "...",
    "reasoning": "说明整个退出路径的设计逻辑，为什么先减、再减、最后清。"
  }
}
```

### 人机边界

- 完全自动化

---

## 6. 人机协同工作流

## 6.1 核心原则

进场决策不是“全自动一次跑完”，而是**可暂停、可补充、可继续**的工作流。

## 6.2 协同节点

### 自动执行节点

- 角色一
- 角色二
- 角色六

### 可能暂停节点

- 角色三：缺财务/预期数据
- 角色四：缺估值信息
- 角色五：缺个人仓位信息

## 6.3 暂停事件结构建议

SSE 新增事件：

- `decision_pause`

事件体示例：

```json
{
  "stage": "value_stage_analysis",
  "missing_fields": [
    "financial_summary.revenue_growth",
    "financial_summary.profit_growth",
    "expectation_summary"
  ],
  "prompt": "请补充最新财报关键数据和机构预期摘要后继续。",
  "resume_token": "EDS-XXXX"
}
```

## 6.4 继续执行接口建议

新增接口：

- `POST /api/trading-decision/entry-decisions/<session_id>/resume`

用途：

- 在用户补充完信息后，从暂停点继续执行后续角色

---

## 7. API 设计

## 7.1 页面初始化接口

### `GET /entry-decision?watch_stock_id=<id>`

保持现有页面路由模式，但页面数据需要新增：

- session 初始化默认值
- 已存在的暂停会话
- 待补充字段默认值
- 最近一次决策结果

### 建议页面读模型

```json
{
  "watch_stock": {...},
  "form_defaults": {
    "trade_date": "2026-04-27",
    "investment_horizon": "中期",
    "current_position": "0%",
    "max_target_position": "15%"
  },
  "active_session": {
    "id": "EDS-XXXX",
    "status": "paused",
    "current_role": "价值阶段AI分析师",
    "missing_fields": [...]
  },
  "latest_record": {...}
}
```

---

## 7.2 启动分析接口

### `POST /api/trading-decision/watch-stocks/<watch_stock_id>/entry-decision/analyze`

### 新定位

该接口不再启动通用单股分析，而是启动 **Entry Decision 专属角色链**。

### 请求体建议

```json
{
  "trade_date": "2026-04-27",
  "investment_horizon": "中期",
  "analysis_depth": "standard",
  "client_id": "entry_client_xxx",
  "manual_inputs": {
    "financial_summary": {
      "revenue_growth": "待补充",
      "profit_growth": "待补充",
      "cashflow_status": "待补充",
      "margin_trend": "待补充"
    },
    "expectation_summary": "",
    "valuation_input": {
      "pe": null,
      "pb": null,
      "valuation_judgement": ""
    },
    "position_input": {
      "current_position": "0%",
      "max_target_position": "15%"
    }
  }
}
```

### 同步返回

```json
{
  "success": true,
  "data": {
    "session_id": "EDS-XXXX",
    "task_mode": "async",
    "client_id": "entry_client_xxx",
    "status": "running"
  },
  "message": "进场决策分析已启动"
}
```

### SSE 事件建议

- `log`
- `singleProgress`
- `decision_role_result`
- `decision_pause`
- `final_result`
- `completion`
- `error`

---

## 7.3 继续分析接口

### `POST /api/trading-decision/entry-decisions/<session_id>/resume`

### 请求体建议

```json
{
  "client_id": "entry_client_xxx",
  "manual_inputs": {
    "financial_summary": {
      "revenue_growth": "改善",
      "profit_growth": "改善",
      "cashflow_status": "健康",
      "margin_trend": "稳定"
    },
    "expectation_summary": "机构预期开始上修，但尚未一致乐观。",
    "valuation_input": {
      "pe": 18.5,
      "pb": 3.2,
      "valuation_judgement": "合理偏低"
    },
    "position_input": {
      "current_position": "0%",
      "max_target_position": "15%"
    }
  }
}
```

### 返回

```json
{
  "success": true,
  "data": {
    "session_id": "EDS-XXXX",
    "status": "running"
  },
  "message": "进场决策分析已继续执行"
}
```

---

## 7.4 保存记录接口

建议新增：

- `POST /api/trading-decision/entry-decision-records`

而不是继续仅靠 `PUT /watch-stocks/<id>` 回写摘要。

### 原因

进场决策是独立业务事实，应保存历史记录，不能只保留当前摘要。

### 请求体建议

```json
{
  "watch_stock_id": "WS-XXXX",
  "session_id": "EDS-XXXX",
  "final_result": { ...完整决策对象... },
  "confirmed_overrides": {
    "current_stage": "B",
    "current_price_zone": "合理偏低区",
    "suggested_action": "适合买入",
    "suggested_entry_leg": "第一笔"
  }
}
```

### 保存结果

1. 落库 `entry_decision_records`
2. 回写 `watch_stocks` 摘要字段：
   - `current_stage`
   - `current_price_zone`
   - `suggested_action`
   - `last_conclusion_summary`
   - `last_analysis_at`
   - `last_decision_record_id`

---

## 7.5 历史记录接口

建议新增：

- `GET /api/trading-decision/entry-decision-records?watch_stock_id=...`
- `GET /api/trading-decision/entry-decision-records/<record_id>`

用于：

- 页面历史列表
- 决策详情回看
- 与 `trade-plan-analysis` 保持一致

---

## 8. 最终结果结构设计

## 8.1 建议顶层结构

`final_result.data` 建议改为：

```json
{
  "watch_stock": {...},
  "basic_info": {...},
  "macro_analysis": {...},
  "asset_classification": {...},
  "value_stage_analysis": {...},
  "price_zone_analysis": {...},
  "buy_plan_analysis": {...},
  "risk_control_analysis": {...},
  "decision_card": {...},
  "manual_inputs": {...},
  "meta": {
    "session_id": "EDS-XXXX",
    "status": "completed",
    "completed_roles": [
      "macro",
      "asset_classification",
      "value_stage",
      "price_zone",
      "buy_plan",
      "risk_control"
    ]
  }
}
```

## 8.2 页面核心展示映射

| 结果字段 | 页面展示区域 |
|---|---|
| `basic_info` | 标的基本信息 |
| `macro_analysis` | Step 1 宏观判断 |
| `asset_classification` | Step 2 资产归类 |
| `value_stage_analysis` | Step 3 价值阶段 |
| `price_zone_analysis` | Step 4 价格分区 |
| `buy_plan_analysis` | Step 5 三笔计划 |
| `risk_control_analysis.post_buy_rise_plan` | Step 6 涨后应对 |
| `risk_control_analysis.post_buy_fall_plan` | Step 7 跌后应对 |
| `risk_control_analysis.sell_rules` | 卖出规则 |
| `decision_card` | 最终一页决策卡 |

---

## 9. 技术实现方案

## 9.1 推荐新增模块

建议新增以下实现，不要继续把逻辑堆在现有 `analysis.py` 里：

```text
src/stock_analyse/application/orchestrators/entry_decision_orchestrator.py
src/stock_analyse/application/dto/entry_decision_state.py
src/stock_analyse/application/agents/entry_decision/
  macro_analyst.py
  asset_classification_analyst.py
  value_stage_analyst.py
  price_zone_analyst.py
  buy_plan_analyst.py
  risk_control_analyst.py
src/stock_analyse/interfaces/web/services/entry_decision_service.py
src/stock_analyse/infrastructure/persistence/trading_decision/
  entry_decision_session_repository.py
  entry_decision_record_repository.py
```

---

## 9.2 推荐复用现有模式

### 复用点一：异步执行模式

继续复用：

- `executor`
- `analysis_tasks`
- `StreamingAnalyzer`
- `/api/sse`

但新建专属启动函数，例如：

- `start_entry_decision_analysis(...)`

而不是复用：

- `start_stock_ai_analysis(...)`

### 复用点二：页面路由模式

保留：

- `routes/trading_decision.py` 作为 HTTP 入口

### 复用点三：现有股票资料/行情/技术数据能力

优先复用：

- `stockBorderInfo(...).get_stock_spot()`
- 当前 quant 指标服务
- 当前单股分析使用的数据汇总方法
- 当前公司资料服务

### 复用点四：trade-plan-analysis 的保存模式

建议参照 `trade_plan_analysis_records`：

- 独立记录表
- 独立 list/detail API
- 保存后回写 `watch_stocks` 摘要

---

## 9.3 角色编排器设计

新增编排器：

- `EntryDecisionOrchestrator`

### 主要职责

1. 初始化会话状态
2. 调用各角色 agent
3. 检测缺失人工输入
4. 发出 pause 事件
5. 接收 resume 后从断点继续
6. 聚合最终结构化决策对象

### 状态对象建议

```python
@dataclass
class EntryDecisionState:
    request: dict
    watch_stock: dict
    auto_context: dict
    manual_inputs: dict
    role_outputs: dict
    current_role: str
    status: str  # running / paused / completed / failed
    missing_fields: list[str]
    final_result: dict
```

---

## 9.4 角色 Agent 统一接口

建议每个角色都实现统一接口：

```python
class EntryDecisionAgent(Protocol):
    role_name: str

    def run(self, state: EntryDecisionState) -> dict:
        ...
```

每个角色只关心：

- 输入字段
- 输出结构
- 是否缺少人工输入

如果缺少输入，返回：

```json
{
  "status": "paused",
  "missing_fields": [...],
  "prompt": "请补充..."
}
```

---

## 9.5 会话与持久化设计

## 会话表 `entry_decision_sessions`

用途：

- 存放运行中的分析会话
- 支撑 pause / resume

建议字段：

- `id`
- `watch_stock_id`
- `stock_code`
- `trade_date`
- `status`
- `current_role`
- `manual_inputs_json`
- `role_outputs_json`
- `missing_fields_json`
- `final_result_json`
- `created_at`
- `updated_at`

## 记录表 `entry_decision_records`

用途：

- 存放已确认保存的正式进场决策

建议字段：

- `id`
- `watch_stock_id`
- `session_id`
- `stock_code`
- `stock_name`
- `market`
- `trade_date`
- `current_stage`
- `current_price_zone`
- `suggested_action`
- `suggested_entry_leg`
- `conclusion_summary`
- `decision_card_json`
- `full_result_json`
- `created_at`
- `updated_at`

---

## 9.6 页面交互建议

前端交互建议调整为三段：

### 阶段 A：启动分析

用户点击“生成决策”后：

1. 建立 SSE
2. 调 `POST /entry-decision/analyze`
3. 接收角色日志与阶段结果

### 阶段 B：中途补充

若收到 `decision_pause`：

1. 页面弹出待补充表单区
2. 用户填写财务/估值/仓位信息
3. 调 `POST /entry-decisions/<session_id>/resume`

### 阶段 C：确认与保存

收到 `final_result` 后：

1. 页面按章节展示完整决策卡
2. 用户可调整少量确认字段
3. 点击保存，调 `POST /entry-decision-records`

---

## 10. 推荐输入数据来源映射

以下是按“尽量复用当前已实现接口或数据方法”的建议。

| 需要的数据 | 推荐来源 |
|---|---|
| 股票基础信息 | `watch_stocks` 表 |
| 当前价格 | `watch_stocks.current_price`，缺失时用 `stock-search` / `get_stock_spot()` |
| PE | `watch_stocks.pe`，缺失时用 `get_stock_spot()` |
| 市场类型 | `watch_stocks.market` |
| 行业/资产类型 | `watch_stocks.industry` / `watch_stocks.asset_type` |
| 公司基础资料 | 复用单股分析 snapshot 的 `company_profile/business_intro/industry/concepts` |
| 技术指标 | 复用当前 quant / indicator 数据能力 |
| 市场情绪 | 复用当前 single-stock snapshot 的 `sentiment` |
| 市场环境摘要 | 复用当前 single-stock snapshot 的 `market_context` 或服务层聚合 |
| 财务指标 | 优先复用当前财务数据方法；不足时人工补充 |
| 机构预期摘要 | 初期人工补充 |
| 估值判断 | 初期人工补充 + 当前 PE/PB 辅助 |
| 当前仓位 / 最大仓位 | 页面人工输入 |

---

## 11. 推荐实施顺序

### 第 1 步：切断错误复用

修改：

- `src/stock_analyse/interfaces/web/routes/trading_decision.py:125`

目标：

- 不再直接调用 `build_stock_ai_payload(...)`
- 不再直接调用 `start_stock_ai_analysis(...)`

### 第 2 步：建立专属会话模型

新增：

- `entry_decision_sessions` 表
- session repository
- pause / resume 基础能力

### 第 3 步：先实现 6 角色链 MVP

建议第一版就落地 6 角色，但允许部分人工输入较多：

- 财务摘要人工补
- 预期摘要人工补
- 估值判断人工补
- 仓位信息人工补

### 第 4 步：输出结构化决策卡

让 `final_result.data` 直接对齐 Markdown 模板结构。

### 第 5 步：增加正式记录保存

新增：

- `entry_decision_records`
- list/detail/save API
- watch_stocks 摘要回写

### 第 6 步：页面改造

- 支持 pause 事件
- 支持 resume 提交
- 支持章节化展示

---

## 12. 与当前个股分析模式的关系

本方案是“参考个股分析的运行模式”，不是“复用个股分析的业务语义”。

### 可以参考的部分

- 异步任务模式
- SSE 推送模式
- 流式日志展示模式
- Flask route + service 模式
- 后台 executor 执行模式

### 不应该复用的部分

- 单股分析的 8 角色业务链
- 单股分析的 `decision/scores/signals/risks/evidence` 输出结构
- 单股分析的最终结果映射逻辑

### 正确关系

应改为：

- **技术执行框架参考单股分析**
- **业务角色与结果结构独立定义**

---

## 13. 最终推荐结论

当前 `POST /api/trading-decision/watch-stocks/<id>/entry-decision/analyze` 的实现逻辑确实不正确，问题根因不是接口地址，而是其内部仍然走了“单股分析 AI 链路”。

正确改造方向应为：

1. 为进场决策建立 **专属 6 角色 AI 链**
2. 建立 **pause / resume** 的人机协同分析会话
3. 输出结构对齐 `进场决策_600900.md`
4. 复用现有 **executor + SSE + 数据访问方法**
5. 新增 **entry_decision_sessions / entry_decision_records** 持久化模型
6. 页面从“通用分析结果页”改成“章节化决策卡页”

这会让进场决策从“分析页的变体”真正升级为“可执行的买前决策产品”。

---

## 14. 推荐落地文件

建议后续实现优先涉及：

- `src/stock_analyse/interfaces/web/routes/trading_decision.py`
- `src/stock_analyse/interfaces/web/services/trading_decision_service.py`
- `src/stock_analyse/application/orchestrators/entry_decision_orchestrator.py`
- `src/stock_analyse/application/dto/entry_decision_state.py`
- `src/stock_analyse/application/agents/entry_decision/*.py`
- `src/stock_analyse/infrastructure/persistence/trading_decision/schema_manager.py`
- `src/stock_analyse/infrastructure/persistence/trading_decision/entry_decision_session_repository.py`
- `src/stock_analyse/infrastructure/persistence/trading_decision/entry_decision_record_repository.py`
- `templates/entry_decision.html`
- `tests/application/trading_decision/test_trading_decision_routes.py`
