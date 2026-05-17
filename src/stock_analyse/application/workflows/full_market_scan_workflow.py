from __future__ import annotations

import logging
import random
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from tqdm import tqdm

from stock_analyse.application.workflows.technical_analysis_workflow import TechnicalAnalysisWorkflow
from stock_analyse.domain.strategies.selection_strategy_service import SelectionStrategyService
from stock_analyse.domain.strategies.stock_select_strategy import StockSelectStrategy
from stock_analyse.infrastructure.config.settings import get_settings
from stock_analyse.infrastructure.persistence.stock_file_utils import StockFileUtils
from stock_analyse.infrastructure.services.futu_market_data_provider import FutuMarketDataProvider
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo


@dataclass
class FullMarketScanRuntime:
    """全市场扫描运行时依赖集合。

    用于封装一次全市场扫描批次中的技术分析器、选股器、文件输出和日志依赖，避免在扫描过程中重复组装对象。
    """

    market: str
    strategy_type: int
    analyzer: TechnicalAnalysisWorkflow
    selector: StockSelectStrategy
    file_utils: StockFileUtils
    logger: logging.Logger
    max_workers: int
    min_score: float


class FullMarketScanWorkflow:
    """全市场批量扫描工作流。

    用于选股、技术分析和高分结果落盘场景，负责按市场批量拉取股票列表、筛选候选并并发执行单票分析。
    """

    def __init__(self, max_workers: int = 20, min_score: float = 30) -> None:
        self.max_workers = max_workers
        self.min_score = min_score
        self.logger = logging.getLogger(__name__)

    def build_runtime(self, *, market: str, strategy_type: int) -> tuple[StockFileUtils, FullMarketScanRuntime]:
        selector = StockSelectStrategy(market=market, strategy_type=strategy_type)
        strategy_name = selector.get_strategy_name(strategy_type)
        file_utils = StockFileUtils(min_score=self.min_score, market=market, name=strategy_name)
        normalized_market = get_settings().market_data.normalize_market(market)
        configured_provider = get_settings().market_data.provider_for_market(normalized_market)
        runtime_max_workers = self.max_workers
        if configured_provider == 'futu' and normalized_market in {'H', 'HK', 'usa'}:
            runtime_max_workers = min(self.max_workers, 4)
        runtime = FullMarketScanRuntime(
            market=market,
            strategy_type=strategy_type,
            analyzer=TechnicalAnalysisWorkflow(market=market),
            selector=selector,
            file_utils=file_utils,
            logger=self.logger,
            max_workers=runtime_max_workers,
            min_score=self.min_score,
        )
        return file_utils, runtime

    def get_all_stocks(self, *, market: str) -> pd.DataFrame:
        try:
            stock = stockBorderInfo(market=market)
            normalized_market = get_settings().market_data.normalize_market(market)
            if normalized_market in {'SH', 'SZ'}:
                df_stock = stock.get_stock_spot()
                self.logger.info('Loaded A-share scan candidates from stock spot | market=%s | rows=%s', normalized_market, len(df_stock))
            else:
                df_stock = stock.get_stock_border_info()
                self.logger.info('Loaded enriched scan candidates | market=%s | rows=%s', normalized_market, len(df_stock))
            self.logger.info(f"\n开始分析 {len(df_stock)} 支股票...")
            return df_stock
        except Exception as exc:
            self.logger.error(f"获取股票列表失败：{exc}")
            traceback.print_exc()
            raise

    def _should_source_candidates_from_strategy(self, *, market: str) -> bool:
        normalized_market = get_settings().market_data.normalize_market(market)
        configured_provider = get_settings().market_data.provider_for_market(normalized_market)
        return configured_provider == 'futu' and normalized_market in {'H', 'usa'}

    def _get_scan_candidates(
        self,
        *,
        runtime: FullMarketScanRuntime,
        strategy_filter: str,
    ) -> pd.DataFrame:
        if self._should_source_candidates_from_strategy(market=runtime.market):
            runtime.logger.info('Use strategy-driven Futu prefilter candidates | market=%s | strategy=%s', runtime.market, runtime.strategy_type)
            df_selected = SelectionStrategyService().get_prefilter_candidates_or_raise(
                market=runtime.market,
                strategy_type=runtime.strategy_type,
                strategy_filter=strategy_filter,
            )
            runtime.logger.info('Strategy prefilter returned %s candidate stocks', len(df_selected))
            runtime.logger.info('Use throttled Futu scan runtime | market=%s | max_workers=%s', runtime.market, runtime.max_workers)
            if df_selected.empty:
                runtime.logger.info('Strategy prefilter produced no candidates | market=%s | strategy=%s', runtime.market, runtime.strategy_type)
                return df_selected
            
            # 标准化 Futu 返回的结果，确保有名为 '代码' 的列
            if '代码' not in df_selected.columns:
                for col in ['股票代码', 'code', 'stock_code']:
                    if col in df_selected.columns:
                        df_selected['代码'] = df_selected[col]
                        break
            return df_selected

        df_stocks_data = self.get_all_stocks(market=runtime.market)
        df_selected = runtime.selector.select_stock(
            df_stocks_data,
            strategy_type=runtime.strategy_type,
            strategy_filter=strategy_filter,
        )
        
        # 调试输出
        if df_selected.empty:
            runtime.logger.warning('select_stock returned empty dataframe. Original columns: %s', df_stocks_data.columns.tolist())
            # 如果是空的且没有列名，尝试打印原始数据的样本
            if not df_stocks_data.empty:
                runtime.logger.info('Original data sample (first 5 rows):\n%s', df_stocks_data.head(5).to_string())

        # 兼容多种列名获取代码集合
        possible_code_cols = ['代码', '股票代码', 'code', 'stock_code']
        code_col = next((col for col in possible_code_cols if col in df_selected.columns), None)
        
        if code_col is None:
            runtime.logger.error('Could not find code column in selected stocks. Columns available: %s', df_selected.columns.tolist())
            return pd.DataFrame()

        selected_codes = set(df_selected[code_col].astype(str))
        
        # 同样适配原始数据的列名
        base_code_col = next((col for col in possible_code_cols if col in df_stocks_data.columns), '代码')
        df_final = df_stocks_data[df_stocks_data[base_code_col].astype(str).isin(selected_codes)].copy()

        # 标准化结果，确保下游可以通过 '代码' 访问
        if '代码' not in df_final.columns and base_code_col != '代码':
            df_final['代码'] = df_final[base_code_col]
        
        return df_final

    def analyze_stock_safe(self, runtime: FullMarketScanRuntime, stock, max_retries: int = 3) -> Optional[dict]:
        stock_code = stock['代码']
        market = stock['market']
        for attempt in range(max_retries):
            try:
                result = runtime.analyzer.analyze_stock(stock, market)
                runtime.logger.debug(f"股票 {stock_code} 分析完成，结果：{result}")
                return result
            except ValueError as exc:
                runtime.logger.warning(f"跳过股票 {stock_code}: {exc}")
                return None
            except Exception as exc:
                if attempt == max_retries - 1:
                    runtime.logger.error(f"股票 {stock_code} 分析尝试 {max_retries} 次后失败：{exc}")
                    return None
                runtime.logger.warning(f"股票 {stock_code} 第 {attempt + 1} 次分析失败：{exc}")
                traceback.print_exc()
                time.sleep(random.uniform(2, 5))
        return None

    def process_batch(self, runtime: FullMarketScanRuntime, stock_codes: pd.DataFrame) -> list[dict]:
        results: list[dict] = []
        with ThreadPoolExecutor(max_workers=runtime.max_workers) as executor:
            futures = {executor.submit(self.analyze_stock_safe, runtime, row): index for index, row in stock_codes.iterrows()}
            for future in tqdm(futures, desc='分析进度', ncols=80):
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as exc:
                    stock = futures[future]
                    runtime.logger.error(f"处理股票 {stock} 时出错：{exc}")
        return results

    def scan_stock(self, runtime: FullMarketScanRuntime, *, batch_size: int, df_stocks_data: pd.DataFrame) -> list[dict]:
        all_stocks = df_stocks_data.copy()
        all_stocks['market'] = runtime.market
        total_stocks = len(all_stocks)
        runtime.logger.info(f"\n开始扫描 {total_stocks} 支股票……")
        results: list[dict] = []
        total_batches = (total_stocks + batch_size - 1) // batch_size
        green = '\033[92m'
        reset = '\033[0m'
        bar_format = f"{green}{{l_bar}}{{bar}}{{r_bar}}{reset}"
        with tqdm(total=total_batches, desc='批次处理进度', ncols=80, bar_format=bar_format) as pbar:
            for i in range(0, total_stocks, batch_size):
                batch_number = i // batch_size + 1
                runtime.logger.info(f"\r当前进度: 批次 {batch_number}/{total_batches}")
                batch = all_stocks.iloc[i:i + batch_size]
                batch_results = self.process_batch(runtime, batch)
                results.extend(batch_results)
                if i + batch_size < total_stocks:
                    time.sleep(random.uniform(3, 5))
                if results and ((len(results) % 100 == 0) or (i + batch_size >= total_stocks)):
                    runtime.file_utils.save_intermediate_results(results)
                pbar.update(1)
                pbar.set_description(f"批次处理进度 (当前批次: {batch_number}/{total_batches})")
        runtime.logger.info('\n扫描结束！')
        return results

    def run(
        self,
        *,
        market: str,
        strategy_type: int = 1,
        batch_size: int = 20,
        strategy_filter: str = 'avg',
        min_score: float | None = None,
    ):
        if min_score is not None and min_score != self.min_score:
            self.min_score = min_score
        file_utils, runtime = self.build_runtime(market=market, strategy_type=strategy_type)
        try:
            df_stocks_data = self._get_scan_candidates(runtime=runtime, strategy_filter=strategy_filter)
            results = self.scan_stock(runtime, batch_size=batch_size, df_stocks_data=df_stocks_data)
            if not results:
                return file_utils, []
            df_results = pd.DataFrame(results)
            formatted_results = file_utils.save_high_score_stocks(df_results)
            return file_utils, formatted_results
        except Exception as exc:
            runtime.logger.error(f"全盘扫描失败：{exc}")
            traceback.print_exc()
            raise
