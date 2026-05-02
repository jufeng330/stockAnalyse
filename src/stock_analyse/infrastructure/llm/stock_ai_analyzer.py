"""统一 AI 访问与 JSON 重整入口。

负责向兼容 OpenAI 的模型发送请求，并在结构化输出场景中执行本地解析与二次 JSON 重整。
"""

import datetime
import json
import os
import traceback
from pathlib import Path

import akshare as ak
import dashscope
import gradio as gr
import pandas as pd
from openai import OpenAI
from sqlalchemy import false
from sympy import factorial

from stock_analyse.infrastructure.config.settings import get_settings
from stock_analyse.infrastructure.data_sources.reports.annual_report_client import stockAnnualReport
from stock_analyse.infrastructure.services.company_data_service import stockCompanyInfo


def debug_log(message):
    print(f"[DEBUG] {message}")


class StockAiAnalyzer:
    """统一的模型请求与结构化输出处理器。

    负责构建模型配置、发起请求、保存原始响应，并在 JSON 场景中完成解析与重整。
    """

    def __init__(self, system_prompt=None, prompt_template=None, model=None, ai_platform=None, api_token=None):
        """根据运行配置初始化模型、平台和提示模板。"""
        settings = get_settings()
        self.model = "qwen-plus"
        resolved_platform = ai_platform or settings.ai.platform
        resolved_api_key = api_token or settings.ai.api_key or settings.ai.provider_keys.get(resolved_platform, '')
        resolved_base_url = settings.ai.resolve_api_base_url()
        dashscope.api_key = resolved_api_key
        self.api_key = resolved_api_key
        self.platform = resolved_platform
        self.max_tokens = settings.ai.max_tokens
        self.temperature = settings.ai.temperature

        debug_log(f"AI platform configured: {resolved_platform}")
        debug_log(f"API key configured: {'yes' if resolved_api_key else 'no'}")

        if model is not None:
            self.model = model
        else:
            self.model = settings.ai.model_name or "qwen-turbo"
        self.base_http_api_url = resolved_base_url or 'https://dashscope.aliyuncs.com/compatible-mode/v1/'
        if not resolved_base_url:
            if resolved_platform == 'qwen':
                self.base_http_api_url = 'https://dashscope.aliyuncs.com/compatible-mode/v1/'
            elif resolved_platform == 'byte':
                self.base_http_api_url = 'https://ark.cn-beijing.volces.com/api/v3/'
            elif resolved_platform == 'deepseek':
                self.base_http_api_url = 'https://api.deepseek.com/'
            elif resolved_platform == 'openai':
                self.base_http_api_url = 'https://api.openai.com/v1/'
            elif resolved_platform == 'kimi':
                self.base_http_api_url = 'https://api.moonshot.cn/v1'
        debug_log(
            f"Resolved AI config: platform={self.platform}, model={self.model}, base_url={self.base_http_api_url}, max_tokens={self.max_tokens}, temperature={self.temperature}"
        )
        if system_prompt is not None:
            self.instruction = system_prompt
        else:
            self.instruction = settings.ai.system_prompt
        if prompt_template is not None:
            self.prompt_template = prompt_template
        else:
            self.prompt_template = settings.ai.prompt_template or """当前股票主营业务介绍:
                {stock_zyjs_ths_df}

                当前股票所在的行业资金流数据:
                {single_industry_df}

                当前股票所在的概念板块的数据:
                {concept_info_df}

                当前股票基本数据:
                {stock_individual_info_em_df}

                当前股票历史行情数据和K线技术指标::
                {stock_zh_a_hist_df}

                当前股票最近的新闻:
                {stock_news_em_df}

                当前股票历史的资金流动:
                {stock_individual_fund_flow_df}

                当前股票的财务指标数据:
                {stock_financial_analysis_indicator_df}

                """
        project_root = Path(__file__).resolve().parents[4]
        self.data_dir = project_root / 'cache' / 'analyzer_result'
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _dump_openai_completion(self, stock_name: str, completion, current_date: datetime.datetime) -> None:
        timestamp_str = current_date.strftime("%Y%m%d%H%M%S")
        dump_path = self.data_dir / f"{stock_name or 'unknown'}_{self.model}_completion_{timestamp_str}.json"
        try:
            if hasattr(completion, 'model_dump_json'):
                dump_path.write_text(completion.model_dump_json(indent=2), encoding='utf-8')
                return
            if hasattr(completion, 'model_dump'):
                dump_path.write_text(
                    json.dumps(completion.model_dump(), ensure_ascii=False, indent=2, default=str),
                    encoding='utf-8',
                )
                return
            if hasattr(completion, 'to_dict'):
                dump_path.write_text(
                    json.dumps(completion.to_dict(), ensure_ascii=False, indent=2, default=str),
                    encoding='utf-8',
                )
                return
            dump_path.write_text(json.dumps(completion, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        except Exception as dump_error:
            debug_log(f"写入 completion dump 失败: {dump_error}")
            return
        debug_log(f"OpenAI completion 已保存到文件: {dump_path}")

    def _stream_chat_completion_text(self, client, request_kwargs: dict[str, object], stock_name: str, current_date: datetime.datetime) -> str:
        raw_chunks: list[dict[str, object]] = []
        text_parts: list[str] = []
        stream = client.chat.completions.create(
            **request_kwargs,
            stream=True,
        )
        for chunk in stream:
            if hasattr(chunk, 'model_dump'):
                raw_chunks.append(chunk.model_dump())
            else:
                raw_chunks.append(json.loads(json.dumps(chunk, ensure_ascii=False, default=str)))
            for choice in getattr(chunk, 'choices', []) or []:
                delta = getattr(choice, 'delta', None)
                if delta is None:
                    continue
                content = getattr(delta, 'content', None)
                if isinstance(content, str) and content:
                    text_parts.append(content)
                    continue
                reasoning_content = getattr(delta, 'reasoning_content', None)
                if isinstance(reasoning_content, str) and reasoning_content:
                    text_parts.append(reasoning_content)
        timestamp_str = current_date.strftime("%Y%m%d%H%M%S")
        dump_path = self.data_dir / f"{stock_name or 'unknown'}_{self.model}_stream_completion_{timestamp_str}.json"
        try:
            dump_path.write_text(
                json.dumps(raw_chunks, ensure_ascii=False, indent=2, default=str),
                encoding='utf-8',
            )
            debug_log(f"OpenAI stream completion 已保存到文件: {dump_path}")
        except Exception as dump_error:
            debug_log(f"写入 stream completion dump 失败: {dump_error}")
        return ''.join(text_parts).strip()

    def _summarize_stream_text(self, text: str) -> str:
        preview = (text or '').strip()[:500]
        return f"content={preview!r}, tool_calls=[]"

    def _run_stream_json_reformat(self, client, request_kwargs: dict, instruction: str, raw_text: str, stock_name: str, current_date: datetime.datetime):
        fallback_kwargs = dict(request_kwargs)
        fallback_kwargs.pop('tools', None)
        fallback_kwargs.pop('tool_choice', None)
        fallback_kwargs.pop('response_format', None)
        fallback_kwargs['messages'] = (
            {'role': 'system', 'content': self._build_json_reformat_instruction(instruction)},
            {'role': 'user', 'content': self._build_json_reformat_message(raw_text)},
        )
        content = self._stream_chat_completion_text(
            client,
            fallback_kwargs,
            f"{stock_name or 'unknown'}_json_reformat",
            current_date,
        )
        parsed = self._parse_json_content(content)
        if parsed is not None:
            return parsed
        raise ValueError(
            '当前 AI 服务在首轮生成和 JSON 重整模式下都未返回可解析内容。'
            f' 原始响应摘要: {self._summarize_stream_text(content)}'
        )

    def _extract_message_tool_calls(self, choice) -> list:
        tool_calls = getattr(choice, 'tool_calls', None) or []
        if tool_calls:
            return tool_calls
        function_call = getattr(choice, 'function_call', None)
        if function_call:
            return [{'function': function_call}]
        additional_kwargs = getattr(choice, 'additional_kwargs', None) or {}
        alt_tool_calls = additional_kwargs.get('tool_calls') or []
        if alt_tool_calls:
            return alt_tool_calls
        alt_function_call = additional_kwargs.get('function_call')
        if alt_function_call:
            return [{'function': alt_function_call}]
        return []
    def _extract_tool_call_arguments(self, tool_call) -> str:
        function_call = getattr(tool_call, 'function', None)
        if function_call is not None:
            return getattr(function_call, 'arguments', '') or ''
        if isinstance(tool_call, dict):
            function_dict = tool_call.get('function') or {}
            if isinstance(function_dict, dict):
                return function_dict.get('arguments', '') or ''
            return getattr(function_dict, 'arguments', '') or ''
        return ''

    def _summarize_choice(self, choice) -> str:
        if choice is None:
            return '<none>'
        content = getattr(choice, 'content', None)
        tool_calls = self._extract_message_tool_calls(choice)
        content_preview = content.strip()[:500] if isinstance(content, str) else repr(content)
        return f"content={content_preview}, tool_calls={json.dumps(tool_calls, ensure_ascii=False, default=str)[:1000]}"

    def _build_json_reformat_instruction(self, instruction: str) -> str:
        return (
            f"{instruction}\n"
            '你的唯一任务是把输入内容整理成一个合法 JSON 对象字符串。'
            '不要输出 markdown，不要输出解释，不要补充任何说明文字。'
        )

    def _build_json_reformat_message(self, raw_text: str) -> str:
        return (
            '请将下面内容整理为一个合法 JSON 对象字符串。\n'
            '要求：\n'
            '1. 只输出 JSON 对象本身。\n'
            '2. 去掉 markdown 代码块标记。\n'
            '3. 如果有中文说明文字，只保留能组成 JSON 对象的部分。\n'
            '4. 如果内容本身无法整理成 JSON 对象，请原样返回。\n\n'
            f'原始内容:\n{raw_text}'
        )

    def _parse_json_content(self, content: str):
        stripped = (content or '').strip()
        if not stripped:
            return None
        if stripped.startswith('```'):
            stripped = stripped.strip('`').strip()
            if stripped.startswith('json'):
                stripped = stripped[4:].strip()
        start = stripped.find('{')
        end = stripped.rfind('}')
        if start != -1 and end != -1 and end > start:
            candidate = stripped[start:end + 1]
            return json.loads(candidate)
        return None

    def _retry_json_reformat(self, client, request_kwargs: dict, instruction: str, raw_text: str, stock_name: str, current_date: datetime.datetime):
        return self._run_stream_json_reformat(client, request_kwargs, instruction, raw_text, stock_name, current_date)

    def aliyun_chat_api_call(self, symbol='', message='你好'):
        current_date = datetime.datetime.now()
        stock_name = symbol
        try:
            debug_log(f"{self.model}_api_call............................")
            if len(message) > 109024:
                debug_log(f'消息太长，长度:{len(message)} 截断消息... ')
            messages = [
                {"role": "system", "content": self.instruction},
                {"role": "user", "content": message},
            ]

            response = dashscope.Generation.call(
                model=self.model,
                messages=messages,
                result_format='message',
            )
            if response.status_code != 200:
                qwen_response = f"调用 API 失败,无法获取分析结果 : {response.status_code}, {response.message}"
                return qwen_response
            qwen_response = response["output"]["choices"][0]["message"]["content"]
            timestamp_str = current_date.strftime("%Y%m%d%H%M%S")
            qwen_file_name = self.data_dir / f"{stock_name}_{self.model}_response_{timestamp_str}.txt"

            with open(qwen_file_name, 'w', encoding='utf-8') as qwen_file:
                qwen_file.write(qwen_response)
            debug_log(f"qwen API 响应已保存到文件: {qwen_file_name}")
            return qwen_response
        except Exception as e:
            debug_log(f"发生异常: {e}")
            result = f"发生异常: {e}"
            return result

    def openai_api_call(
        self,
        symbol='',
        message='你好',
        instruction='请模拟中国A股的分析大师',
        *,
        tools=None,
        tool_choice=None,
        response_format=None,
        require_tool_call=False,
    ):
        """调用兼容 OpenAI 的模型接口，并优先返回可解析的 JSON 结果。"""
        current_date = datetime.datetime.now()
        stock_name = symbol
        raw_response = ''
        try:
            debug_log("openai_api_call............................")
            if len(message) > 109024:
                debug_log(f'消息太长，长度:{len(message)} 截断消息... ')

            client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_http_api_url,
            )

            request_kwargs = {
                'model': self.model,
                'messages': (
                    {'role': 'system', 'content': instruction},
                    {'role': 'user', 'content': message},
                ),
                'max_tokens': self.max_tokens,
                'temperature': self.temperature,
            }
            if tools:
                request_kwargs['tools'] = tools
            if tool_choice:
                request_kwargs['tool_choice'] = tool_choice
            if response_format:
                request_kwargs['response_format'] = response_format

            request_kwargs.pop('tools', None)
            request_kwargs.pop('tool_choice', None)
            request_kwargs.pop('response_format', None)
            raw_response = self._stream_chat_completion_text(client, request_kwargs, stock_name, current_date)
            if raw_response:
                debug_log(
                    f"OpenAI 兼容响应摘要: symbol={stock_name}, model={self.model}, preview={raw_response[:500]!r}"
                )
                parsed = self._parse_json_content(raw_response)
                if parsed is not None:
                    return parsed
                return self._retry_json_reformat(
                    client,
                    request_kwargs,
                    instruction,
                    raw_response,
                    stock_name,
                    current_date,
                )

            raise ValueError(
                '当前 AI 服务未返回文本内容，无法进入 JSON 解析。'
                f' 原始响应摘要: {self._summarize_stream_text(raw_response)}'
            )
        except Exception as e:
            debug_log(f"发生异常: {e}; raw_response={raw_response}")
            traceback.print_exc()
            raise

    def process_prompt(self, stock_zyjs_ths_df, stock_individual_info_em_df, stock_zh_a_hist_df, stock_news_em_df,
                       stock_individual_fund_flow_df, technical_indicators_df,
                       stock_financial_analysis_indicator_df, single_industry_df, concept_info_df):
        prompt_template = self.prompt_template
        prompt_filled = prompt_template.format(stock_zyjs_ths_df=stock_zyjs_ths_df,
                                               stock_individual_info_em_df=stock_individual_info_em_df,
                                               stock_zh_a_hist_df=stock_zh_a_hist_df,
                                               stock_news_em_df=stock_news_em_df,
                                               stock_individual_fund_flow_df=stock_individual_fund_flow_df,
                                               technical_indicators_df=technical_indicators_df,
                                               stock_financial_analysis_indicator_df=stock_financial_analysis_indicator_df,
                                               single_industry_df=single_industry_df,
                                               concept_info_df=concept_info_df
                                               )
        return prompt_filled

    def get_stock_summary(self, market, symbol):
        stock_service = stockCompanyInfo(market, symbol)
        stock_zyjs_ths_df = stock_service.get_stock_individual_info()
        debug_log(f"个股信息查询: {stock_zyjs_ths_df}")
        return stock_zyjs_ths_df.to_string(index=False)

    def stock_indicator_analyse(self, market, symbol, start_date, end_date):
        stock_service = stockCompanyInfo(market, symbol)
        stock_name = stock_service.get_stock_name()
        debug_log(f"创建 stockCompanyInfo 实例: {market}, {symbol}, {stock_name}")

        stock_zyjs_ths_df = stock_service.get_stock_zyjs()
        debug_log(f"获取主营业务介绍: {stock_zyjs_ths_df}")

        stock_individual_info_em_df, list_date, industry = stock_service.get_stock_individual_info_em()
        debug_log(f"获取个股信息: {stock_individual_info_em_df}")

        stock_sector_fund_flow_rank_df = stock_service.get_stock_fund_flow()
        if '名称' in stock_sector_fund_flow_rank_df.columns:
            single_industry_df = stock_sector_fund_flow_rank_df[stock_sector_fund_flow_rank_df['名称'] == industry]
        else:
            single_industry_df = pd.DataFrame()

        concept_info_df = stock_service.get_stock_industry_by_code(code=symbol)
        stock_zh_a_hist_df = stock_service.get_stock_history_data(start_date_str=start_date, end_date_str=end_date)
        technical_indicators_df = stock_zh_a_hist_df
        stock_news_em_df = stock_service.get_stock_news()
        stock_individual_fund_flow_df = stock_service.get_stock_individual_fund_flow()
        stock_financial_analysis_indicator_df = stock_service.get_stock_financial_analysis_indicator()

        user_message = self.generate_stock_indicate_message(concept_info_df, single_industry_df,
                                                            stock_financial_analysis_indicator_df,
                                                            stock_individual_fund_flow_df, stock_individual_info_em_df,
                                                            stock_news_em_df, stock_zh_a_hist_df, stock_zyjs_ths_df,
                                                            technical_indicators_df)

        timestamp_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        file_name = self.data_dir / f"{stock_name}_indicator_{market}_{self.model}_request_{timestamp_str}.txt"
        with open(file_name, 'w', encoding='utf-8') as file:
            is_mark_down = True
            user_message_view = self.generate_stock_indicate_message(concept_info_df, single_industry_df,
                                                                     stock_financial_analysis_indicator_df,
                                                                     stock_individual_fund_flow_df,
                                                                     stock_individual_info_em_df,
                                                                     stock_news_em_df, stock_zh_a_hist_df, stock_zyjs_ths_df,
                                                                     technical_indicators_df, is_mark_down)
            file.write(user_message_view)
        debug_log(f"{stock_name}_已保存到文件: {file_name}")

        result_qwen = self.openai_api_call(symbol=symbol, message=user_message, instruction=self.instruction)
        debug_log(f"Qwen API 响应 {len(result_qwen)}: {result_qwen}")

        file_name = self.data_dir / f"{stock_name}_indicator_{market}_{self.model}_{timestamp_str}.txt"
        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(result_qwen)
        debug_log(f"{stock_name}_已保存到文件: {file_name}")

        return result_qwen

    def generate_stock_indicate_message(self, concept_info_df, single_industry_df,
                                        stock_financial_analysis_indicator_df, stock_individual_fund_flow_df,
                                        stock_individual_info_em_df, stock_news_em_df, stock_zh_a_hist_df,
                                        stock_zyjs_ths_df, technical_indicators_df, is_mark_down=false):

        if is_mark_down:
            concept_info_df = concept_info_df if concept_info_df is not None else ''
            single_industry_df = single_industry_df.to_markdown(index=False) if single_industry_df is not None else ''
            stock_financial_analysis_indicator_df = stock_financial_analysis_indicator_df.to_markdown(index=False)
            stock_individual_fund_flow_df = stock_individual_fund_flow_df.to_markdown(index=False)
            stock_individual_info_em_df = stock_individual_info_em_df.to_markdown(index=False) if stock_individual_info_em_df is not None else ''
            stock_news_em_df = stock_news_em_df.to_markdown(index=False)
            stock_zh_a_hist_df = stock_zh_a_hist_df.to_markdown(index=False) if stock_zh_a_hist_df is not None else ''
            stock_zyjs_ths_df = stock_zyjs_ths_df.to_markdown(index=False) if stock_zyjs_ths_df is not None else ''
        else:
            concept_info_df = concept_info_df.to_string(index=False) if concept_info_df is not None else ''
            single_industry_df = single_industry_df.to_string(index=False) if single_industry_df is not None else ''
            stock_financial_analysis_indicator_df = stock_financial_analysis_indicator_df.to_string(index=False)
            stock_individual_fund_flow_df = stock_individual_fund_flow_df.to_string(index=False)
            stock_individual_info_em_df = stock_individual_info_em_df.to_string(index=False) if stock_individual_info_em_df is not None else ''
            stock_news_em_df = stock_news_em_df.to_string(index=False)
            stock_zh_a_hist_df = stock_zh_a_hist_df.to_string(index=False) if stock_zh_a_hist_df is not None else ''
            stock_zyjs_ths_df = stock_zyjs_ths_df.to_string(index=False) if stock_zyjs_ths_df is not None else ''
        technical_indicators_df = technical_indicators_df.to_markdown(index=False) if technical_indicators_df is not None else ''

        finally_prompt = self.process_prompt(stock_zyjs_ths_df, stock_individual_info_em_df, stock_zh_a_hist_df,
                                             stock_news_em_df,
                                             stock_individual_fund_flow_df, technical_indicators_df,
                                             stock_financial_analysis_indicator_df, single_industry_df,
                                             concept_info_df)
        debug_log(f"构建最终提示: {finally_prompt}")
        user_message = (
            f"{finally_prompt}\n\n"
            f"请基于以上收集到的实时的真实数据，发挥你的A股分析专业知识，对未来3天该股票的价格走势做出深度预测。\n"
            f"在预测中请全面考虑主营业务、基本数据、所在行业数据、所在概念板块数据、历史行情、最近新闻以及资金流动等多方面因素。\n"
            f"给出具体的涨跌百分比数据分析总结。\n\n"
            f"以下是具体问题，请详尽回答：\n\n"
            f"1. 对最近这个股票的资金流动情况以及所在行业的资金流情况和所在概念板块的资金情况分别进行深入分析，"
            f"请详解这三个维度的资金流入或者流出的主要原因，并评估是否属于短期现象和未来的影响。\n"
            f"相关数据：\n"
            f"## 个股资金流：\n{stock_individual_fund_flow_df}\n\n"
            f"## 行业资金流：\n{single_industry_df}\n\n"
            f"## 概念板块资金流：\n{concept_info_df}\n\n"
            f"2. 基于最近财务指标数据，深刻评估公司未来业绩是否有望积极改善，可以关注盈利能力、负债情况等财务指标。"
            f"同时分析未来财务状况。\n"
            f"## 财务指标数据：\n{stock_financial_analysis_indicator_df}\n\n"
            f"3. 是否存在与行业或公司相关的积极或者消极的消息，可能对股票价格产生什么影响？分析新闻对市场情绪的具体影响，"
            f"并评估消息的可靠性和长期影响。\n"
            f"## 相关新闻：\n{stock_news_em_df}\n\n"
            f"4. 基于技术分析指标，如均线、MACD、RSI、CCI等，请提供更为具体的未来走势预测。"
            f"关注指标的交叉和趋势，并解读当下可能的买卖信号。\n"
            f"## 技术指标数据：\n{technical_indicators_df}\n\n"
            f"5. 在综合以上分析的基础上，向投资者推荐在未来3天内采取何种具体操作？"
            f"从不同的投资者角度明确给出买入、卖出、持有或补仓或减仓的建议，并说明理由，附上相应的止盈/止损策略。"
            f"记住给出的策略需要精确给我写出止盈位的价格，充分利用利润点，或者精确写出止损位的价格，规避亏损风险。\n\n"
            f"## 历史行情参考：\n{stock_zh_a_hist_df}\n\n"
            f"## 个股基本信息：\n{stock_individual_info_em_df}\n\n"
            f"## 质押数据参考：\n{stock_zyjs_ths_df}\n\n"
            f"你可以一步一步的去思考，期待你深刻的分析，将有力指导我的投资决策。"
        )
        debug_log(f"构建用户消息: {user_message}")
        return user_message

    def stock_report_analyse(self, market, symbol, concept='科技板块'):
        try:
            debug_log(f"{self.model}_api_call............................")
            report_service = stockAnnualReport()
            stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = report_service.get_stock_report(stock_code=symbol, market=market)
            stock_service = stockCompanyInfo(market, symbol)
            stock_financial_indicator_df = stock_service.get_stock_financial_analysis_indicator()

            current_date = datetime.datetime.now()
            current_date_str = current_date.strftime("%Y-%m-%d")
            previous_year = current_date - datetime.timedelta(days=30)
            previous_str = previous_year.strftime("%Y-%m-%d")
            stock_price_df = stock_service.get_stock_history_data(start_date_str=previous_str, end_date_str=current_date_str)

            prompt_template = self.generate_report_prompt_message(stock_financial_indicator_df, stock_lrb_em_df,
                                                                  stock_price_df, stock_xjll_em_df, stock_zcfz_em_df)

            text = self.openai_api_call(symbol=symbol, message=prompt_template, instruction=self.instruction)

            stock_name = stock_service.get_stock_name()
            timestamp_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            file_name = self.data_dir / f"{stock_name}_report_{market}_{self.model}__{timestamp_str}.txt"
            with open(file_name, 'w', encoding='utf-8') as file:
                file.write(self.instruction)
                is_mark_down = True
                prompt_template_markdown = self.generate_report_prompt_message(stock_financial_indicator_df, stock_lrb_em_df,
                                                                              stock_price_df, stock_xjll_em_df,
                                                                              stock_zcfz_em_df, is_mark_down)
                file.write(prompt_template_markdown)
                file.write("\n\n\n\n\n\nAI的分析结果:\n\n ")
                file.write(text)
            debug_log(f"{stock_name}_已保存到文件: {file_name}")
            return text
        except Exception as e:
            debug_log(f"发生异常: {e}")
            result = f"{self.model} 分析发生异常: {e}"
            traceback.print_exc()
            return result

    def generate_report_prompt_message(self, stock_financial_indicator_df, stock_lrb_em_df, stock_price_df,
                                       stock_xjll_em_df, stock_zcfz_em_df, is_mark_down=False):
        if is_mark_down is True:
            stock_financial_indicator_df = stock_financial_indicator_df.to_markdown(index=False) if stock_financial_indicator_df is not None else ''
            stock_zcfz_em_df_str = stock_zcfz_em_df.to_markdown(index=False) if stock_zcfz_em_df is not None else ''
            stock_lrb_em_df_str = stock_lrb_em_df.to_markdown(index=False) if stock_lrb_em_df is not None else ''
            stock_xjll_em_df_str = stock_xjll_em_df.to_markdown(index=False) if stock_xjll_em_df is not None else ''
            stock_price_df_str = stock_price_df.to_markdown(index=False) if stock_price_df is not None else ''
        else:
            stock_financial_indicator_df = stock_financial_indicator_df.to_string(index=False) if stock_financial_indicator_df is not None else ''
            stock_zcfz_em_df_str = stock_zcfz_em_df.to_string(index=False) if stock_zcfz_em_df is not None else ''
            stock_lrb_em_df_str = stock_lrb_em_df.to_string(index=False) if stock_lrb_em_df is not None else ''
            stock_xjll_em_df_str = stock_xjll_em_df.to_string(index=False) if stock_xjll_em_df is not None else ''
            stock_price_df_str = stock_price_df.to_string(index=False) if stock_price_df is not None else ''

        self.instruction = """你作为股票分析专家,请详细公司财务报表，揭示公司财务健康状况。采用资产负债表相关指标
                            资产负债率：
                            流动比率：
                            速动比率：
                            利润表相关指标
                            毛利率：
                            净利率：
                            净资产收益率（ROE）：
                            现金流量表相关指标
                            经营活动现金流量净额：
                            自由现金流量：
                            市盈率法（P/E）
                            市净率法（P/B）
                            现金流折现法（DCF）等等科学方法，评估公司的财务健康状况和公司当前股票的估值状况，提供财务投资建议"""
        self.prompt_template = """当前股票财务介绍:
                    资产负债表:
                            {stock_zcfz_em_df}
                    利润表
                            {stock_lrb_em_df}
                    现金流量表
                            {stock_xjll_em_df}
                    财务指标
                            {stock_financial_indicator_df}
                    股票历史成绩数据
                            {stock_price_df}

                     请基于以上收集到的实时的真实数据，发挥你的股票分析专业知识，做出如下评估结果
                      1、给出公司财务健康报告和财务风险
                      2、公司股票估值苹果结果，给出估值结果
                      3、公司股票的投资建议。
                      数据支持尽可能详细以便判断结果准确性\n
                    """
        prompt_template = self.prompt_template.format(stock_zcfz_em_df=stock_zcfz_em_df_str,
                                                      stock_lrb_em_df=stock_lrb_em_df_str,
                                                      stock_xjll_em_df=stock_xjll_em_df_str,
                                                      stock_financial_indicator_df=stock_financial_indicator_df,
                                                      stock_price_df=stock_price_df_str)
        return prompt_template

    def stock_sentiment_analyse(self, market, symbol, stock_name, start_date, end_date, concept):
        instruction = '请模拟中国A股的分析大师'
        content = ''
        stock_code = ''
        stock_code = ''
        prompt = f"考虑以下的新闻内容和最近的股票价格走势，请给出未来5天股票价格走势预测的涨跌百分比,并作为短线投资者的给出以下建议：买入，卖出，持有，补仓：\n\n{content}\n\n分析结果："
        +f'综合分析和预测结果： {stock_code} - {stock_name}'
        +f'根据提供的新闻内容、技术指标（MACD、RSI、KDJ）以及股价走势数据，我将对东尼电子 {stock_code} - {stock_name}未来5天的股价走势进行预测，并给出短线投资建议。'

        text = ''
        return text

    def test(self):
        text = self.stock_indicator_analyse(market='SH', symbol='000681', start_date='20250101', end_date='20250501')
        print(text)
