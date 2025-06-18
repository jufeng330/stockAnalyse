import sys
import os
import traceback
import os
from openai import OpenAI
from sqlalchemy import false
from sympy import factorial
# 添加 stock_analyse 目录到 Python 模块搜索路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import threading
import pandas as pd
import datetime
import os
import dashscope
from dotenv import load_dotenv
import gradio as gr
import akshare as ak
import sys
import os
from stocklib.stock_company import stockCompanyInfo
from stocklib.stock_annual_report import stockAnnualReport

# 添加调试日志函数
def debug_log(message):
    print(f"[DEBUG] {message}")

class StockAiAnalyzer:
    def __init__(self,system_prompt=None,prompt_template=None,model=None,ai_platform=None,api_token = None):
        load_dotenv()
        # 加载环境变量中的 OpenAI API 密钥
        OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
        DASHSCOPE_API_KEY = os.getenv('DASHSCOPE_API_KEY')
        dashscope.api_key = DASHSCOPE_API_KEY

        debug_log(f"OpenAI API Key: {OPENAI_API_KEY}")
        debug_log(f"DashScope API Key: {DASHSCOPE_API_KEY}")

        self.model="qwen-plus"
        if(api_token is not None):
            dashscope.api_key = api_token
            self.api_key = api_token

        if(model is not None):
            self.model = model
        else:
            self.model = "qwen-plus"
        if ai_platform is not None:
            if ai_platform == 'qwen':
                self.base_http_api_url = 'https://dashscope.aliyuncs.com/compatible-mode/v1/'
            elif ai_platform == 'byte':
                self.base_http_api_url = 'https://ark.cn-beijing.volces.com/api/v3/'
            elif ai_platform == 'deepseek':
                self.base_http_api_url = 'https://api.deepseek.com/'
            elif ai_platform == 'openai':
                self.base_http_api_url = 'https://api.openai.com/v1/'
            elif ai_platform == 'kimi':
                self.base_http_api_url = 'https://api.moonshot.cn/v1'
        if system_prompt is not None:
            self.instruction = system_prompt
        else:
            self.instruction = "你作为A股分析专家,请详细分析市场趋势、行业前景，揭示潜在投资机会,请确保提供充分的数据支持和专业见解。"
        if(prompt_template is not None):
            self.prompt_template = prompt_template
        else:
            self.prompt_template ="""当前股票主营业务介绍:
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
        self.data_dir = os.path.join(os.path.dirname(__file__), 'result')

    def aliyun_chat_api_call(self,symbol='', message='你好'):
        current_date = datetime.datetime.now()
        stock_name = symbol
        try:
            debug_log(f"{self.model}_api_call............................")
            if (len(message) > 109024):
                debug_log(f'消息太长，长度:{len(message)} 截断消息... ')
                message = message[:108024]
            messages = [
                {"role": "system", "content": self.instruction},
                {"role": "user", "content": message}
            ]

            response = dashscope.Generation.call(
                model=self.model,
                messages=messages,
                result_format='message',  # set the result is message format.
            )
            if response.status_code != 200:
                qwen_response = (f"调用 API 失败,无法获取分析结果 : {response.status_code}, {response.message}")
                return qwen_response
            qwen_response = response["output"]["choices"][0]["message"]["content"]
            timestamp_str = current_date.strftime("%Y%m%d%H%M%S")
            qwen_file_name = os.path.join(self.data_dir, f"{stock_name}_{self.model}_response_{timestamp_str}.txt")

            with open(qwen_file_name, 'w', encoding='utf-8') as qwen_file:
                qwen_file.write(qwen_response)
            debug_log(f"qwen API 响应已保存到文件: {qwen_file_name}")
            return qwen_response
        except Exception as e:
            debug_log(f"发生异常: {e}")
            result = f"发生异常: {e}"
            return result
    def openai_api_call(self,symbol='', message='你好',instruction = '请模拟中国A股的分析大师'):
        current_date = datetime.datetime.now()
        stock_name = symbol
        json = ''
        try:
            debug_log(f"openai_api_call............................")
            if (len(message) > 109024):
                debug_log(f'消息太长，长度:{len(message)} 截断消息... ')
                message = message[:108024]

            client = OpenAI(
                # 若没有配置环境变量，请用百炼API Key将下行替换为：api_key="sk-xxx",
                api_key=self.api_key,
                base_url=self.base_http_api_url
            )

            completion = client.chat.completions.create(
                model=self.model,  # 模型名称为qwen - plus
                messages=(
                    {'role': 'system', 'content': instruction},
                    {'role': 'user', 'content': message}
                ),
                stream=True
            )

            full_response = ""
            for chunk in completion:
                if chunk.choices[0].delta.content is not None:
                    full_response += chunk.choices[0].delta.content

            print(full_response)
            json = full_response
            return full_response
        except Exception as e:
            debug_log(f"发生异常: {e}")
            result = f"发生异常: {e} reuslt:{json}"
            return result



    def process_prompt(self,stock_zyjs_ths_df, stock_individual_info_em_df, stock_zh_a_hist_df, stock_news_em_df,
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

    # 个股信息查询
    def get_stock_summary(self,market, symbol):
        stock_service = stockCompanyInfo(market, symbol)
        stock_zyjs_ths_df = stock_service.get_stock_individual_info()
        debug_log(f"个股信息查询: {stock_zyjs_ths_df}")
        return stock_zyjs_ths_df.to_string(index=False)
#    公司股票数据分析
    def stock_indicator_analyse(self, market, symbol,start_date,end_date):


        stock_service = stockCompanyInfo(market, symbol)
        stock_name = stock_service.get_stock_name()
        debug_log(f"创建 stockCompanyInfo 实例: {market}, {symbol}, {stock_name}")

        # 主营业务介绍 TODO 根据主营业务网络搜索相关事件报道
        stock_zyjs_ths_df = stock_service.get_stock_zyjs()
        debug_log(f"获取主营业务介绍: {stock_zyjs_ths_df}")

        # 个股信息查询
        stock_individual_info_em_df,list_date,industry = stock_service.get_stock_individual_info_em()
        debug_log(f"获取个股信息: {stock_individual_info_em_df}")

        # 获取当前个股所在行业板块情况
        stock_sector_fund_flow_rank_df = stock_service.get_stock_fund_flow()
        single_industry_df = stock_sector_fund_flow_rank_df[stock_sector_fund_flow_rank_df['名称'] == industry]


        df_concept_border = stock_service.get_stock_board_all_concept_name()
        # 获取概念板块的数据情况
        concept_info_df = stock_service.get_stock_board_concept_name(symbol=symbol, df_stock_board=df_concept_border)

        # 个股历史数据查询
        stock_zh_a_hist_df = stock_service.get_stock_history_data(start_date_str=start_date, end_date_str=end_date)
        # 个股技术指标计算
        technical_indicators_df = stock_zh_a_hist_df

        # 个股新闻
        stock_news_em_df = stock_service.get_stock_news()

        # 历史的个股资金流
        stock_individual_fund_flow_df = stock_service.get_stock_individual_fund_flow()

        # 财务指标
        stock_financial_analysis_indicator_df = stock_service.get_stock_financial_analysis_indicator()
        # 构建最终prompt

        user_message = self.generate_stock_indicate_message(concept_info_df, single_industry_df,
                                                            stock_financial_analysis_indicator_df,
                                                            stock_individual_fund_flow_df, stock_individual_info_em_df,
                                                            stock_news_em_df, stock_zh_a_hist_df, stock_zyjs_ths_df,
                                                            technical_indicators_df)

        # 获取当前时间戳字符串
        timestamp_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        file_name = os.path.join(self.data_dir, f"{stock_name}_indicator_{market}_{self.model}_request_{timestamp_str}.txt")
        # 修改这一行，确保文件名合法
        with open(file_name, 'w', encoding='utf-8') as file:
            is_mark_down = True
            user_message_view = self.generate_stock_indicate_message(concept_info_df, single_industry_df,
                                                                stock_financial_analysis_indicator_df,
                                                                stock_individual_fund_flow_df,
                                                                stock_individual_info_em_df,
                                                                stock_news_em_df, stock_zh_a_hist_df, stock_zyjs_ths_df,
                                                                technical_indicators_df,is_mark_down)
            file.write(user_message_view)
        debug_log(f"{stock_name}_已保存到文件: {file_name}")

        # 创建一个列表来存储结果
        result = [None, None]

       #  result_qwen = self.aliyun_chat_api_call(symbol=symbol,message=user_message)
        result_qwen = self.openai_api_call(symbol=symbol, message=user_message,instruction=self.instruction)
        debug_log(f"Qwen API 响应 {len(result_qwen)}: {result_qwen}")

        file_name = os.path.join(self.data_dir, f"{stock_name}_indicator_{market}_{self.model}_{timestamp_str}.txt")
        # 修改这一行，确保文件名合法
        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(result_qwen)
        debug_log(f"{stock_name}_已保存到文件: {file_name}")

        return result_qwen

    def generate_stock_indicate_message(self, concept_info_df, single_industry_df,
                                        stock_financial_analysis_indicator_df, stock_individual_fund_flow_df,
                                        stock_individual_info_em_df, stock_news_em_df, stock_zh_a_hist_df,
                                        stock_zyjs_ths_df, technical_indicators_df,is_mark_down = false):

        if is_mark_down :
            concept_info_df = concept_info_df.to_markdown(index=False) if concept_info_df is not None else ''
            single_industry_df = single_industry_df.to_markdown(index=False) if single_industry_df is not None else ''
            stock_financial_analysis_indicator_df = stock_financial_analysis_indicator_df.to_markdown(
                index=False)

            stock_individual_fund_flow_df = stock_individual_fund_flow_df.to_markdown(index=False)
            stock_individual_info_em_df = stock_individual_info_em_df.to_markdown(index=False) if stock_individual_info_em_df is not None else ''
            stock_news_em_df = stock_news_em_df.to_markdown(index=False)

            stock_zh_a_hist_df = stock_zh_a_hist_df.to_markdown(index=False) if stock_zh_a_hist_df is not None else ''


            stock_zyjs_ths_df = stock_zyjs_ths_df.to_markdown(index=False) if stock_zyjs_ths_df is not None else ''
        else:
            concept_info_df = concept_info_df.to_string(index=False) if concept_info_df is not None else ''
            single_industry_df = single_industry_df.to_string(index=False) if single_industry_df is not None else ''
            stock_financial_analysis_indicator_df = stock_financial_analysis_indicator_df.to_string(
                index=False)

            stock_individual_fund_flow_df = stock_individual_fund_flow_df.to_string(index=False)
            stock_individual_info_em_df = stock_individual_info_em_df.to_string(
                index=False) if stock_individual_info_em_df is not None else ''
            stock_news_em_df = stock_news_em_df.to_string(index=False)

            stock_zh_a_hist_df = stock_zh_a_hist_df.to_string(index=False) if stock_zh_a_hist_df is not None else ''

            stock_zyjs_ths_df = stock_zyjs_ths_df.to_string(index=False) if stock_zyjs_ths_df is not None else ''
        technical_indicators_df = technical_indicators_df.to_markdown(
            index=False) if technical_indicators_df is not None else ''





        finally_prompt = self.process_prompt(stock_zyjs_ths_df, stock_individual_info_em_df, stock_zh_a_hist_df,
                                             stock_news_em_df,
                                             stock_individual_fund_flow_df, technical_indicators_df
                                             , stock_financial_analysis_indicator_df, single_industry_df,
                                             concept_info_df)
        debug_log(f"构建最终提示: {finally_prompt}")
        user_message = (
            f"{finally_prompt}\n"
            f"请基于以上收集到的实时的真实数据，发挥你的A股分析专业知识，对未来3天该股票的价格走势做出深度预测。\n"
            f"在预测中请全面考虑主营业务、基本数据、所在行业数据、所在概念板块数据、历史行情、最近新闻以及资金流动等多方面因素。\n"
            f"给出具体的涨跌百分比数据分析总结。\n\n"
            f"以下是具体问题，请详尽回答：\n\n"
            f"1. 对最近这个股票的资金流动情况以及所在行业的资金流情况和所在概念板块的资金情况分别进行深入分析，"
            f"请详解这三个维度的资金流入或者流出的主要原因，并评估是否属于短期现象和未来的影响。\n\n"
            f"2. 基于最近财务指标数据，深刻评估公司未来业绩是否有望积极改善，可以关注盈利能力、负债情况等财务指标。"
            f"同时分析未来财务状况。\n\n"
            f"3. 是否存在与行业或公司相关的积极或者消极的消息，可能对股票价格产生什么影响？分析新闻对市场情绪的具体影响，"
            f"并评估消息的可靠性和长期影响。\n\n"
            f"4. 基于技术分析指标，如均线、MACD、RSI、CCI等，请提供更为具体的未来走势预测。"
            f"关注指标的交叉和趋势，并解读当下可能的买卖信号。\n\n"
            f"5. 在综合以上分析的基础上，向投资者推荐在未来3天内采取何种具体操作？"
            f"从不同的投资者角度明确给出买入、卖出、持有或补仓或减仓的建议，并说明理由，附上相应的止盈/止损策略。"
            f"记住给出的策略需要精确给我写出止盈位的价格，充分利用利润点，或者精确写出止损位的价格，规避亏损风险。\n\n"
            f"你可以一步一步的去思考，期待你深刻的分析，将有力指导我的投资决策。"
        )
        debug_log(f"构建用户消息: {user_message}")
        return user_message

    # 公司股票财务分析
    def stock_report_analyse(self, market, symbol,  concept='科技板块'):



        try:
            debug_log(f"{self.model}_api_call............................")
            report_service = stockAnnualReport()
            stock_zcfz_em_df, stock_lrb_em_df, stock_xjll_em_df = report_service.get_stock_report(stock_code=symbol,
                                                                                                  market=market)
            stock_service = stockCompanyInfo(market, symbol)
            stock_financial_indicator_df = stock_service.get_stock_financial_analysis_indicator()

            current_date = datetime.datetime.now()
            current_date_str = current_date.strftime("%Y-%m-%d")
            previous_year = current_date - datetime.timedelta(days=30)
            previous_str = previous_year.strftime("%Y-%m-%d")
            stock_price_df =  stock_service.get_stock_history_data(start_date_str=previous_str, end_date_str=current_date_str)

            prompt_template = self.generate_report_prompt_message(stock_financial_indicator_df, stock_lrb_em_df,
                                                                  stock_price_df, stock_xjll_em_df, stock_zcfz_em_df)

            text = self.openai_api_call(symbol=symbol, message=prompt_template, instruction=self.instruction)

            stock_name = stock_service.get_stock_name()
            timestamp_str = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            file_name = os.path.join(self.data_dir, f"{stock_name}_report_{market}_{self.model}__{timestamp_str}.txt")
            # 修改这一行，确保文件名合法
            with open(file_name, 'w', encoding='utf-8') as file:
                file.write(self.instruction)
                is_mark_down = True
                prompt_template_markdown = self.generate_report_prompt_message(stock_financial_indicator_df, stock_lrb_em_df,
                                                                      stock_price_df, stock_xjll_em_df,
                                                                      stock_zcfz_em_df,is_mark_down)
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
                                       stock_xjll_em_df, stock_zcfz_em_df,is_mark_down = False):
        # 使用条件表达式处理可能为None的DataFrame
        if is_mark_down == True:
            stock_financial_indicator_df = stock_financial_indicator_df.to_markdown(
                index=False) if stock_financial_indicator_df is not None else ''
            stock_zcfz_em_df_str = stock_zcfz_em_df.to_markdown(index=False) if stock_zcfz_em_df is not None else ''
            stock_lrb_em_df_str = stock_lrb_em_df.to_markdown(index=False) if stock_lrb_em_df is not None else ''
            stock_xjll_em_df_str = stock_xjll_em_df.to_markdown(index=False) if stock_xjll_em_df is not None else ''
            stock_price_df_str = stock_price_df.to_markdown(index=False) if stock_price_df is not None else ''
        else:
            stock_financial_indicator_df = stock_financial_indicator_df.to_string(
                index=False) if stock_financial_indicator_df is not None else ''
            stock_zcfz_em_df_str = stock_zcfz_em_df.to_string(index=False) if stock_zcfz_em_df is not None else ''
            stock_lrb_em_df_str = stock_lrb_em_df.to_string(index=False) if stock_lrb_em_df is not None else ''
            stock_xjll_em_df_str = stock_xjll_em_df.to_string(index=False) if stock_xjll_em_df is not None else ''
            stock_price_df_str = stock_price_df.to_string(index=False) if stock_price_df is not None else ''

            # 修正了原句末尾的句号
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

    # 公司股票情绪分析
    def stock_sentiment_analyse(self, market, symbol, stock_name,
                             start_date,
                             end_date, concept):
        instruction = '请模拟中国A股的分析大师'
        content = ''
        stock_code = ''
        stock_code=''
        prompt = f"考虑以下的新闻内容和最近的股票价格走势，请给出未来5天股票价格走势预测的涨跌百分比,并作为短线投资者的给出以下建议：买入，卖出，持有，补仓：\n\n{content}\n\n分析结果："
        +f'综合分析和预测结果： {stock_code} - {stock_name}'
        +f'根据提供的新闻内容、技术指标（MACD、RSI、KDJ）以及股价走势数据，我将对东尼电子 {stock_code} - {stock_name}未来5天的股价走势进行预测，并给出短线投资建议。'

        text = ''

        return text

    def test (self):

        text = self.stock_indicator_analyse(market='SH', symbol='000681',start_date='20250101',end_date='20250501')
        print(text)