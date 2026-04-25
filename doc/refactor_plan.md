# stockAnalyse 渐进式分层重构计划

## 1. 文档目标

本文档用于指导 `/mnt/github/stock/stockAnalyse` 的重构工作。

重构原则不是一次性大改，也不是先统一改目录再修功能，而是：

- **按 Feature 逐个迁移**
- **每次只改一个清晰边界的能力**
- **新旧实现并存一段时间**
- **每一步都可验证、可回滚**
- **最终收敛到完整分层架构**

目标是在尽量不影响现有功能可用性的前提下，把当前项目逐步演进到更清晰、可测试、可维护的结构。

---

## 2. 当前项目的主要问题

当前项目已经具备较多功能，但存在以下典型问题：

1. **入口重复**
   - `main.py`
   - `__main__.py`
   - `stock_web.py`
   - `stock_server_quantitative.py`
   - `web_sse/stock_analyzer_service.py`
   - `openclaw_skills/*/scripts/main.py`

   同一能力被多个入口重复包装，后续改动容易不同步。

2. **包结构不清晰**
   多处依赖 `sys.path.append(...)` 才能导入模块，说明当前结构没有形成稳定包边界。

3. **基础库过胖**
   `stocklib/stock_border.py` 同时承担数据抓取、缓存、市场分支、并发、格式处理等职责，耦合严重。

4. **业务编排与基础设施混在一起**
   Web、技能脚本、分析流程都直接依赖 `stocklib`，导致业务无法独立测试。

5. **配置和密钥管理不统一**
   存在硬编码 token、secret_key 等风险。

6. **技能层与 Web 层重复封装**
   OpenClaw skills 和 Web 侧很多逻辑本质相同，但没有统一的应用服务层承接。

---

## 3. 目标架构

重构完成后的目标架构如下：

```text
stockAnalyse/
  src/stock_analyse/
    domain/
      models/
      services/
      strategies/
      value_objects/
    application/
      dto/
      use_cases/
      orchestrators/
    infrastructure/
      config/
      logging/
      cache/
      streaming/
      llm/
      data_sources/
        akshare/
        mysql/
        files/
    interfaces/
      cli/
      web/
      api/
      skills/
    shared/
      enums.py
      errors.py
      utils.py
  tests/
    unit/
    integration/
    snapshots/
  scripts/
  config/
    default.json
    example.json
```

### 分层职责

#### domain
只放领域概念与规则：
- 股票、市场、指标、策略信号、评分结果等模型
- 技术指标计算规则
- 选股评分规则
- 估值判断规则

#### application
只放业务用例：
- 分析单只股票
- 扫描高分股票
- 获取基础数据
- 获取技术指标
- 获取新闻情绪
- 估值比较
- 生成进场决策数据

#### infrastructure
只放技术实现：
- AkShare 调用
- MySQL 访问
- 文件缓存
- SSE 推送
- LLM 调用
- 配置读取

#### interfaces
只放对外入口：
- Flask 路由
- CLI 命令
- skills 脚本
- API 层输出格式

---

## 4. 重构总原则

### 4.1 只允许按 Feature 迁移
每次只迁移一个 Feature，例如：
- 股票基础信息查询
- 行情 spot 查询
- 历史数据查询
- 技术指标分析
- 新闻情绪分析
- 估值分析
- 单股综合分析
- 股票筛选

不允许一轮任务里同时大规模修改多个 Feature 的核心逻辑。

### 4.2 不先删除旧实现
每个 Feature 的重构遵循：

1. 保留旧入口和旧逻辑
2. 新建分层实现
3. 在旧入口中接入新实现
4. 验证结果一致
5. 再决定是否删除旧逻辑

### 4.3 不先大规模移动文件
第一阶段避免大面积 `mv` 文件。
优先做法是：
- 在 `src/stock_analyse/` 下新增分层模块
- 让旧代码调用新模块
- 等迁移完成后，再进行目录收敛

### 4.4 每个 Feature 都必须有验证点
每次迁移都至少有一项验证：
- 命令行输出验证
- JSON 返回结构验证
- Web 页面行为验证
- 快照对比验证
- 核心字段一致性验证

### 4.5 每一步都要可回滚
每个 Feature 的改动都应能通过单独 commit 或单独目录回退，不要把多个 Feature 的迁移混在一个提交里。

---

## 5. 推荐的重构顺序

按风险最小、收益最大排序，推荐如下顺序：

### Phase 0：建立重构地基
先不改业务行为，只铺基础设施。

### Phase 1：配置与安全 Feature
先处理配置和密钥，避免后续继续扩散。

### Phase 2：基础数据访问 Feature
从最底层、最容易复用的只读能力开始。

### Phase 3：技术分析 Feature
把单只股票技术分析迁移到 application/domain。

### Phase 4：新闻与情绪 Feature
迁移新闻情绪逻辑。

### Phase 5：估值 Feature
迁移估值和价格区间逻辑。

### Phase 6：单股综合分析 Feature
把散落在 Web/SSE 中的综合分析收敛成统一 use case。

### Phase 7：选股扫描 Feature
迁移 scanner 相关流程。

### Phase 8：skills 接口 Feature
把 skills 统一改为调用 application 层。

### Phase 9：Web/API 接口 Feature
合并 Web 重复入口。

### Phase 10：收尾清理
删除旧适配层、清理 `sys.path.append`、统一目录。

---

## 6. 各阶段详细计划

---

## Phase 0：建立重构地基

### 目标
在不改现有行为的前提下，为新架构提供基础目录和公共能力。

### 范围
新增，不替换旧逻辑。

### 输出
新增以下目录：

```text
src/stock_analyse/
  domain/
  application/
  infrastructure/
  interfaces/
  shared/
tests/
config/
```

### 任务
1. 新建 `src/stock_analyse/__init__.py`
2. 新建分层空目录
3. 新建：
   - `infrastructure/config/settings.py`
   - `infrastructure/logging/logger.py`
   - `shared/errors.py`
   - `shared/enums.py`
4. 让新模块可被导入，但暂不替换旧代码

### 验证
- 能从项目根目录正常导入 `stock_analyse`
- 不影响当前已有命令和 Web 入口

### 回滚方式
- 直接删除新增目录即可

---

## Phase 1：配置与安全 Feature

### 目标
统一配置读取方式，移除硬编码密钥和 secret。

### 现状问题
当前多个文件存在：
- 硬编码 `secret_key`
- 硬编码 AI token
- 配置分散在代码中

### 重构后设计
新增：

```text
config/default.json
config/example.json
infrastructure/config/settings.py
```

提供统一对象：
- `Settings`
- `AISettings`
- `WebSettings`
- `CacheSettings`

### 本阶段只改一个 Feature
**Feature：统一配置读取**

### 实施步骤
1. 新建 `Settings` 配置读取类
2. 把默认配置迁移到 `config/default.json`
3. 支持从环境变量覆盖敏感字段
4. 修改以下位置只接配置对象，不再写死：
   - Flask secret key
   - AI API key
   - token
   - cache path
5. 保持旧字段名兼容，避免一口气改所有代码

### 验证
- Web 可以正常启动
- skills 命令仍能跑
- 配置缺失时能给出稳定错误信息

### 完成标志
- 仓库内不再出现新增硬编码 key
- 新代码不允许直接写死密钥

---

## Phase 2：基础数据访问 Feature

### 目标
把最常用的数据获取能力从 `stocklib/stock_border.py` 里拆出最小可复用单元。

### 设计原则
不一次拆完整个 `stock_border.py`，而是按数据类型逐步抽离。

### 子 Feature 顺序

#### Feature 2.1：股票基础信息查询
目标能力：
- symbol/name/industry/basic info

新增：
```text
infrastructure/data_sources/akshare/company_info_gateway.py
application/use_cases/get_stock_info.py
```

旧入口先继续保留，但内部开始调用新 gateway。

#### Feature 2.2：实时行情 spot 查询
新增：
```text
infrastructure/data_sources/akshare/spot_gateway.py
application/use_cases/get_stock_spot.py
```

#### Feature 2.3：历史行情查询
新增：
```text
infrastructure/data_sources/akshare/history_gateway.py
application/use_cases/get_stock_history.py
```

#### Feature 2.4：财报查询
新增：
```text
infrastructure/data_sources/akshare/financial_report_gateway.py
application/use_cases/get_financial_reports.py
```

### 实施方式
每完成一个子 Feature，就让以下一个入口先接入：
- 优先接 `openclaw_skills/stock-data/scripts/main.py`
- 再接 Web 或其他调用方

这样可以把风险控制在最小范围。

### 验证
每个子 Feature 都要验证：
- 同一个 symbol + market 下，新旧输出关键字段一致
- JSON 结构不乱
- 错误路径可控

### 完成标志
- 基础查询能力不再直接由巨大 `stock_border.py` 独占
- 新代码都走 gateway + use case

---

## Phase 3：技术分析 Feature

### 目标
把技术指标分析从当前分散的实现中收敛到 domain/application。

### 当前涉及文件
- `scanner/stock_analyzer.py`
- `stocklib/stock_indicator_quantitative.py`
- `stocklib/stock_ak_indicator.py`
- `openclaw_skills/stock-technical/scripts/main.py`

### 重构后目标

```text
domain/services/technical_indicator_service.py
application/use_cases/analyze_technical_indicators.py
interfaces/skills/stock_technical.py
```

### 子 Feature 顺序

#### Feature 3.1：单指标计算服务
先只迁移：
- MA
- MACD
- RSI
- Bollinger

#### Feature 3.2：技术信号汇总
把多指标输出统一成标准 DTO：
- indicator values
- signal
- summary

#### Feature 3.3：skills 接口切换
只改 `stock-technical` skill，让它走新 use case。

### 验证
- 对同一股票输出数值差异在可接受范围内
- skill 的输出 JSON 字段保持兼容

### 完成标志
- 技术指标计算不再依赖 Web 或脚本入口
- 技术分析成为可独立复用的核心能力

---

## Phase 4：新闻与情绪 Feature

### 目标
将新闻、情绪和建议逻辑从当前工具类中抽到独立用例。

### 当前涉及
- `stocklib/stock_news_data.py`
- `stocklib/stock_sentiment_analysis.py`
- `openclaw_skills/stock-news/scripts/main.py`

### 重构后目标

```text
infrastructure/data_sources/akshare/news_gateway.py
application/use_cases/get_stock_news.py
application/use_cases/analyze_sentiment.py
```

### 子 Feature 顺序
1. 新闻列表获取
2. 情绪打分
3. 综合新闻摘要
4. `stock-news` skill 接入

### 验证
- `sentiment_score`、`trend` 等关键字段兼容
- 新闻为空时返回结构稳定

---

## Phase 5：估值 Feature

### 目标
把估值与价格区间能力独立出来，避免继续散落在 skill 和 stocklib 内。

### 当前涉及
- `stocklib/dcf_model.py`
- `stocklib/stock_annual_report.py`
- `openclaw_skills/stock-valuation/scripts/main.py`

### 重构后目标

```text
domain/services/valuation_service.py
application/use_cases/compare_valuation.py
application/use_cases/get_price_range.py
```

### 子 Feature 顺序
1. DCF 估值
2. 价格区间
3. compare 输出
4. `stock-valuation` skill 接入

### 验证
- compare/price_range JSON 输出结构兼容
- 数据缺失时能显式返回错误或空值说明

---

## Phase 6：单股综合分析 Feature

### 目标
把“单股分析”收敛成统一 use case，成为 Web、SSE、CLI、skills 的共同核心。

### 当前涉及
- `web_sse/stock_analyzer_service.py`
- `stock_server_quantitative.py`
- `stock_web.py`
- `stockAI/stockAgent/stock_ai_analyzer.py`

### 重构后目标

```text
application/use_cases/analyze_single_stock.py
application/orchestrators/stock_analysis_orchestrator.py
```

### 输入
- symbol
- market
- date range
- strategy options
- ai config

### 输出
统一 DTO：
- summary
- technical
- financial
- sentiment
- ai report
- charts metadata

### 子 Feature 顺序

#### Feature 6.1：非流式综合分析
先做纯同步版本

#### Feature 6.2：AI 分析注入
把 LLM 调用从 Web 层抽到 infra/llm

#### Feature 6.3：SSE 流式包装
只把 streaming 作为接口层包装，不进入业务层

### 验证
- 旧 Web 页面功能可继续调用
- 新旧综合分析关键字段一致

---

## Phase 7：选股扫描 Feature

### 目标
把 scanner 从脚本风格改造成可测试的扫描用例。

### 当前涉及
- `scanner/top_stock_scanner.py`
- `scanner/stock_select_strategy.py`
- `scanner/stock_financial_analyser.py`
- `scanner/stock_report_analyser.py`
- `scanner/stock_fh_analyser.py`

### 重构后目标

```text
domain/strategies/
application/use_cases/select_stocks.py
application/orchestrators/stock_selection_orchestrator.py
```

### 子 Feature 顺序
1. 定义统一选股策略接口
2. 迁移单个策略实现
3. 迁移批量扫描并发调度
4. 迁移结果汇总与文件输出
5. 切换 `stock-strategy` skill

### 验证
- 同一策略在同一输入下，高分股票集合大体一致
- 并发扫描的异常处理稳定

---

## Phase 8：skills 接口 Feature

### 目标
让所有 skills 都只做参数解析和输出，不再承担业务逻辑。

### 范围
- `openclaw_skills/*`
- `openclaw-stock-skills/*`

### 策略
不一次性清理两套目录，而是：

1. 先统一新 skills 的调用方式
2. 新逻辑全部接入 `application/use_cases`
3. 等全部迁移完成后，再决定保留哪一套目录

### 验证
- 每个 skill 的命令参数兼容
- 返回 JSON 结构兼容

### 完成标志
- skill 中不再直接写核心业务逻辑
- skill 变成稳定薄适配层

---

## Phase 9：Web/API 接口 Feature

### 目标
合并 Web 重复入口，统一成一个清晰的接口层。

### 当前问题
- `stock_web.py`
- `stock_server_quantitative.py`
- `web_sse/stock_analyzer_service.py`

职责交叉明显。

### 目标结构

```text
interfaces/web/
  app.py
  routes/
    analysis.py
    selector.py
    history.py
  presenters/
  streaming/
```

### 子 Feature 顺序
1. 统一 app 初始化
2. 提取 analysis route
3. 提取 selector route
4. 提取 history route
5. 提取 auth/config route
6. 最后移除旧 Web 入口

### 验证
- 页面能访问
- SSE 仍可用
- API 返回兼容

---

## Phase 10：收尾与清理

### 目标
在所有 Feature 已迁移完成后，统一清理旧结构。

### 清理内容
1. 删除无用的 `sys.path.append`
2. 删除废弃旧入口
3. 删除重复 skills 目录
4. 收敛旧 stocklib 兼容层
5. 统一导入路径
6. 更新 README 和开发文档

### 注意
此阶段必须放到最后，不提前做。

---

## 7. 每个 Feature 的标准实施模板

以后每个 Feature 都建议按这个模板执行。

### 7.1 Feature 定义
- 名称：
- 范围：
- 旧入口：
- 新模块：
- 风险等级：低 / 中 / 高

### 7.2 改造步骤
1. 新增 domain/application/infra 模块
2. 编写 DTO 和异常结构
3. 接入一个最小入口
4. 做结果对比
5. 扩大接入范围
6. 保留旧兼容层

### 7.3 验证项
- 输入兼容
- 输出兼容
- 异常兼容
- 性能是否明显退化
- 并发是否受影响

### 7.4 回滚方式
- 旧入口恢复到旧实现
- 新模块保留但不接入
- 不做 destructive 删除

---

## 8. 测试与验证策略

为了保证“一个 Feature 一个 Feature 重构”真正安全，建议同时建立以下测试方式。

### 8.1 快照测试
适合 skills 和 API 输出：
- 同一输入，保留旧输出快照
- 新实现输出与旧实现比对

### 8.2 契约测试
适合 JSON 返回：
- 字段名
- 字段类型
- 必填字段
- 错误结构

### 8.3 冒烟测试
至少覆盖：
- 单股分析
- 技术指标
- 新闻情绪
- 估值 compare
- 选股扫描

### 8.4 手工验证
对于 Web/SSE：
- 页面能打开
- 请求能返回
- 流式日志能持续输出
- 失败时错误可见

---

## 9. 每个阶段的提交策略

建议按以下粒度提交，而不是一次性大提交：

1. `chore: add layered package skeleton`
2. `refactor: centralize settings and secret loading`
3. `refactor: extract stock info gateway`
4. `refactor: extract stock spot use case`
5. `refactor: migrate technical skill to application layer`
6. `refactor: extract sentiment use case`
7. `refactor: extract valuation use case`
8. `refactor: unify single stock analysis use case`
9. `refactor: migrate stock selection pipeline`
10. `refactor: consolidate web entrypoints`
11. `chore: remove legacy path hacks and deprecated adapters`

---

## 10. 风险控制要求

### 禁止事项
在整个重构期间，禁止以下行为：

1. 一次性同时改多个 Feature 的核心逻辑
2. 未验证就删除旧实现
3. 在同一次提交中同时做“重命名 + 逻辑重写 + 接口变更”
4. 为了图快直接全局替换导入路径
5. 让 Web、skills、scanner 同时切新链路但没有回退方案

### 必须事项
1. 每个 Feature 都先列清边界
2. 每个 Feature 都先选一个入口试接
3. 每个 Feature 都做旧新结果对比
4. 每个 Feature 都保留回滚点
5. 每次结束后更新本文档的阶段状态

---

## 11. 建议优先级

如果只做最小可控起步，建议优先顺序如下：

### 第一批（最推荐）
1. Phase 0：建立分层骨架
2. Phase 1：统一配置与密钥
3. Phase 2.1：股票基础信息查询
4. Phase 2.2：spot 查询
5. Phase 3.1：核心技术指标计算
6. Phase 4.1：新闻查询

### 第二批
1. Phase 5：估值
2. Phase 6：单股综合分析
3. Phase 8：skill 接口统一

### 第三批
1. Phase 7：选股扫描
2. Phase 9：Web 入口统一
3. Phase 10：旧代码清理

---

## 12. 最终预期结果

完成全部阶段后，项目应达到以下状态：

1. **所有核心功能都通过 application/use_cases 暴露**
2. **Web、CLI、skills 不再直接依赖底层 stocklib 巨石逻辑**
3. **AkShare、MySQL、缓存、LLM 都收敛到 infrastructure**
4. **策略、评分、技术指标逻辑进入 domain**
5. **旧入口只保留最薄兼容层，最终可删除**
6. **不再需要大量 `sys.path.append(...)`**
7. **配置、密钥、安全项统一管理**
8. **每个 Feature 都能独立测试与回滚**

---

## 13. 当前执行建议

如果准备开始实施，建议严格从下面的第一步开始：

### 第一步
执行 **Phase 0 + Phase 1**，仅做：
- 建立 `src/stock_analyse` 分层骨架
- 统一配置读取
- 清理硬编码密钥
- 不触碰业务逻辑输出

这是收益最高、风险最低、后续所有 Feature 重构都依赖的一步。

### 第二步
从 **Phase 2.1 股票基础信息查询** 开始，作为第一个真正迁移的 Feature。

原因：
- 读操作为主
- 风险低
- 复用价值高
- 可快速验证
- 对后续 skills / web / analysis 都有帮助

---

## 14. 文档维护方式

每完成一个 Feature，更新本文档：

- 阶段状态：未开始 / 进行中 / 已完成
- 已迁移入口
- 已删除兼容层
- 遗留问题
- 下一步 Feature

建议在文档末尾追加一段执行记录，例如：

```md
## 执行记录
- [ ] Phase 0：未开始
- [ ] Phase 1：未开始
- [ ] Phase 2.1：未开始
- [ ] Phase 2.2：未开始
- [ ] Phase 3.1：未开始
```

---

## 15. 执行记录

- [ ] Phase 0：建立重构地基
- [ ] Phase 1：配置与安全 Feature
- [ ] Phase 2.1：股票基础信息查询
- [ ] Phase 2.2：实时行情 spot 查询
- [ ] Phase 2.3：历史行情查询
- [ ] Phase 2.4：财报查询
- [ ] Phase 3：技术分析 Feature
- [ ] Phase 4：新闻与情绪 Feature
- [ ] Phase 5：估值 Feature
- [ ] Phase 6：单股综合分析 Feature
- [ ] Phase 7：选股扫描 Feature
- [ ] Phase 8：skills 接口 Feature
- [ ] Phase 9：Web/API 接口 Feature
- [ ] Phase 10：收尾与清理
