import os
import sys
import logging
import traceback
import akshare as ak
from datetime import datetime, timedelta
from .stock_company import stockCompanyInfo

class StockSentimentAnalysis:
    """股票分析引擎，计算各类技术指标"""

    def __init__(self,market='SH',symbol="601919"):
        """
        初始化股票分析引擎

        Args:
            params: 技术指标配置参数
        """
        self._setup_logging()
        self.market = market
        self.symbol = symbol
        self.news_cache = {}
        self.stock_service = stockCompanyInfo(marker=self.market,symbol=self.symbol)


    def _setup_logging(self) -> None:
        """配置日志记录"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)


    def get_sentiment_analysis(self):
        self.logger.info("正在获取新闻数据和分析市场情绪...", 'info')
        try:
            comprehensive_news_data = self.get_comprehensive_news_data(self.symbol, days=30)
            sentiment_analysis = self.calculate_advanced_sentiment_analysis(comprehensive_news_data)
            sentiment_score = self.calculate_sentiment_score(sentiment_analysis)
            self.logger.info(f"✓ 情绪分析完成，得分: {sentiment_score:.1f}", 'success')
            return sentiment_score, sentiment_analysis
        except Exception as e:
            self.logger.warning(f"获取行业新闻失败: {e}")
            return -1,'{}'



    def get_comprehensive_news_data(self, stock_code, days=15):
        """获取综合新闻数据（修正版本）"""
        cache_key = f"{stock_code}_{days}"
        if cache_key in self.news_cache:
            cache_time, data = self.news_cache[cache_key]
            if datetime.now() - cache_time < self.news_cache_duration:
                self.logger.info(f"使用缓存的新闻数据: {stock_code}")
                return data

        self.logger.info(f"开始获取 {stock_code} 的综合新闻数据（最近{days}天）...")

        try:
            # stock_name = self.get_stock_name(stock_code)
            stock_name =  self.stock_service.get_stock_name()
            all_news_data = {
                'company_news': [],
                'announcements': [],
                'research_reports': [],
                'industry_news': [],
                'market_sentiment': {},
                'news_summary': {}
            }

            # 1. 公司新闻
            try:
                self.logger.info("正在获取公司新闻...")
                company_news = ak.stock_news_em(symbol=stock_code)
                if not company_news.empty:
                    processed_news = []
                    for _, row in company_news.head(50).iterrows():  # 增加获取数量
                        news_item = {
                            'title': str(row.get(row.index[0], '')),
                            'content': str(row.get(row.index[1], '')) if len(row.index) > 1 else '',
                            'date': str(row.get(row.index[2], '')) if len(row.index) > 2 else datetime.now().strftime(
                                '%Y-%m-%d'),
                            'source': 'eastmoney',
                            'url': str(row.get(row.index[3], '')) if len(row.index) > 3 else '',
                            'relevance_score': 1.0
                        }
                        processed_news.append(news_item)

                    all_news_data['company_news'] = processed_news
                    self.logger.info(f"✓ 获取公司新闻 {len(processed_news)} 条")
            except Exception as e:
                self.logger.warning(f"获取公司新闻失败: {e}")

            # 2. 公司公告
            try:
                self.logger.info("正在获取公司公告...")
                announcements = ak.stock_zh_a_alerts_cls(symbol=stock_code)
                if not announcements.empty:
                    processed_announcements = []
                    for _, row in announcements.head(30).iterrows():  # 增加获取数量
                        announcement = {
                            'title': str(row.get(row.index[0], '')),
                            'content': str(row.get(row.index[1], '')) if len(row.index) > 1 else '',
                            'date': str(row.get(row.index[2], '')) if len(row.index) > 2 else datetime.now().strftime(
                                '%Y-%m-%d'),
                            'type': str(row.get(row.index[3], '')) if len(row.index) > 3 else '公告',
                            'relevance_score': 1.0
                        }
                        processed_announcements.append(announcement)

                    all_news_data['announcements'] = processed_announcements
                    self.logger.info(f"✓ 获取公司公告 {len(processed_announcements)} 条")
            except Exception as e:
                self.logger.warning(f"获取公司公告失败: {e}")

            # 3. 研究报告
            try:
                self.logger.info("正在获取研究报告...")
                research_reports = ak.stock_research_report_em(symbol=stock_code)
                if not research_reports.empty:
                    processed_reports = []
                    for _, row in research_reports.head(20).iterrows():  # 增加获取数量
                        report = {
                            'title': str(row.get(row.index[0], '')),
                            'institution': str(row.get(row.index[1], '')) if len(row.index) > 1 else '',
                            'rating': str(row.get(row.index[2], '')) if len(row.index) > 2 else '',
                            'target_price': str(row.get(row.index[3], '')) if len(row.index) > 3 else '',
                            'date': str(row.get(row.index[4], '')) if len(row.index) > 4 else datetime.now().strftime(
                                '%Y-%m-%d'),
                            'relevance_score': 0.9
                        }
                        processed_reports.append(report)

                    all_news_data['research_reports'] = processed_reports
                    self.logger.info(f"✓ 获取研究报告 {len(processed_reports)} 条")
            except Exception as e:
                self.logger.warning(f"获取研究报告失败: {e}")

            # 4. 行业新闻
            try:
                self.logger.info("正在获取行业新闻...")
                industry_news = self._get_comprehensive_industry_news(stock_code, days)
                all_news_data['industry_news'] = industry_news
                self.logger.info(f"✓ 获取行业新闻 {len(industry_news)} 条")
            except Exception as e:
                self.logger.warning(f"获取行业新闻失败: {e}")

            # 5. 新闻摘要统计
            try:
                total_news = (len(all_news_data['company_news']) +
                              len(all_news_data['announcements']) +
                              len(all_news_data['research_reports']) +
                              len(all_news_data['industry_news']))

                all_news_data['news_summary'] = {
                    'total_news_count': total_news,
                    'company_news_count': len(all_news_data['company_news']),
                    'announcements_count': len(all_news_data['announcements']),
                    'research_reports_count': len(all_news_data['research_reports']),
                    'industry_news_count': len(all_news_data['industry_news']),
                    'data_freshness': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

            except Exception as e:
                self.logger.warning(f"生成新闻摘要失败: {e}")

            # 缓存数据
            self.news_cache[cache_key] = (datetime.now(), all_news_data)

            self.logger.info(
                f"✓ 综合新闻数据获取完成，总计 {all_news_data['news_summary'].get('total_news_count', 0)} 条")
            return all_news_data

        except Exception as e:
            self.logger.error(f"获取综合新闻数据失败: {str(e)}")
            return {
                'company_news': [],
                'announcements': [],
                'research_reports': [],
                'industry_news': [],
                'market_sentiment': {},
                'news_summary': {'total_news_count': 0}
            }

    def calculate_advanced_sentiment_analysis(self, comprehensive_news_data):
        """计算高级情绪分析（修正版本）"""
        self.logger.info("开始高级情绪分析...")

        try:
            # 准备所有新闻文本
            all_texts = []

            # 收集所有新闻文本
            for news in comprehensive_news_data.get('company_news', []):
                text = f"{news.get('title', '')} {news.get('content', '')}"
                all_texts.append({'text': text, 'type': 'company_news', 'weight': 1.0})

            for announcement in comprehensive_news_data.get('announcements', []):
                text = f"{announcement.get('title', '')} {announcement.get('content', '')}"
                all_texts.append({'text': text, 'type': 'announcement', 'weight': 1.2})  # 公告权重更高

            for report in comprehensive_news_data.get('research_reports', []):
                text = f"{report.get('title', '')} {report.get('rating', '')}"
                all_texts.append({'text': text, 'type': 'research_report', 'weight': 0.9})

            for news in comprehensive_news_data.get('industry_news', []):
                text = f"{news.get('title', '')} {news.get('content', '')}"
                all_texts.append({'text': text, 'type': 'industry_news', 'weight': 0.7})

            if not all_texts:
                return {
                    'overall_sentiment': 0.0,
                    'sentiment_by_type': {},
                    'sentiment_trend': '中性',
                    'confidence_score': 0.0,
                    'total_analyzed': 0
                }

            # 扩展的情绪词典
            positive_words = {
                '上涨', '涨停', '利好', '突破', '增长', '盈利', '收益', '回升', '强势', '看好',
                '买入', '推荐', '优秀', '领先', '创新', '发展', '机会', '潜力', '稳定', '改善',
                '提升', '超预期', '积极', '乐观', '向好', '受益', '龙头', '热点', '爆发', '翻倍',
                '业绩', '增收', '扩张', '合作', '签约', '中标', '获得', '成功', '完成', '达成'
            }

            negative_words = {
                '下跌', '跌停', '利空', '破位', '下滑', '亏损', '风险', '回调', '弱势', '看空',
                '卖出', '减持', '较差', '落后', '滞后', '困难', '危机', '担忧', '悲观', '恶化',
                '下降', '低于预期', '消极', '压力', '套牢', '被套', '暴跌', '崩盘', '踩雷', '退市',
                '违规', '处罚', '调查', '停牌', '亏损', '债务', '违约', '诉讼', '纠纷', '问题'
            }

            # 分析每类新闻的情绪
            sentiment_by_type = {}
            overall_scores = []

            for text_data in all_texts:
                try:
                    text = text_data['text']
                    text_type = text_data['type']
                    weight = text_data['weight']

                    if not text.strip():
                        continue

                    positive_count = sum(1 for word in positive_words if word in text)
                    negative_count = sum(1 for word in negative_words if word in text)

                    # 计算情绪得分
                    total_sentiment_words = positive_count + negative_count
                    if total_sentiment_words > 0:
                        sentiment_score = (positive_count - negative_count) / total_sentiment_words
                    else:
                        sentiment_score = 0.0

                    # 应用权重
                    weighted_score = sentiment_score * weight
                    overall_scores.append(weighted_score)

                    # 按类型统计
                    if text_type not in sentiment_by_type:
                        sentiment_by_type[text_type] = []
                    sentiment_by_type[text_type].append(weighted_score)

                except Exception as e:
                    continue

            # 计算总体情绪
            overall_sentiment = sum(overall_scores) / len(overall_scores) if overall_scores else 0.0

            # 计算各类型平均情绪
            avg_sentiment_by_type = {}
            for text_type, scores in sentiment_by_type.items():
                avg_sentiment_by_type[text_type] = sum(scores) / len(scores) if scores else 0.0

            # 判断情绪趋势
            if overall_sentiment > 0.3:
                sentiment_trend = '非常积极'
            elif overall_sentiment > 0.1:
                sentiment_trend = '偏向积极'
            elif overall_sentiment > -0.1:
                sentiment_trend = '相对中性'
            elif overall_sentiment > -0.3:
                sentiment_trend = '偏向消极'
            else:
                sentiment_trend = '非常消极'

            # 计算置信度
            confidence_score = min(len(all_texts) / 50, 1.0)  # 基于新闻数量的置信度

            result = {
                'overall_sentiment': overall_sentiment,
                'sentiment_by_type': avg_sentiment_by_type,
                'sentiment_trend': sentiment_trend,
                'confidence_score': confidence_score,
                'total_analyzed': len(all_texts),
                'type_distribution': {k: len(v) for k, v in sentiment_by_type.items()},
                'positive_ratio': len([s for s in overall_scores if s > 0]) / len(overall_scores) if overall_scores else 0,
                'negative_ratio': len([s for s in overall_scores if s < 0]) / len(overall_scores) if overall_scores else 0
            }

            self.logger.info(f"✓ 高级情绪分析完成: {sentiment_trend} (得分: {overall_sentiment:.3f})")
            return result

        except Exception as e:
            self.logger.error(f"高级情绪分析失败: {e}")
            return {
                'overall_sentiment': 0.0,
                'sentiment_by_type': {},
                'sentiment_trend': '分析失败',
                'confidence_score': 0.0,
                'total_analyzed': 0
            }

    def calculate_sentiment_score(self, sentiment_analysis):
        """计算情绪分析得分"""
        try:
            overall_sentiment = sentiment_analysis.get('overall_sentiment', 0.0)
            confidence_score = sentiment_analysis.get('confidence_score', 0.0)
            total_analyzed = sentiment_analysis.get('total_analyzed', 0)

            # 基础得分：将情绪得分从[-1,1]映射到[0,100]
            base_score = (overall_sentiment + 1) * 50

            # 置信度调整
            confidence_adjustment = confidence_score * 10

            # 新闻数量调整
            news_adjustment = min(total_analyzed / 100, 1.0) * 10

            final_score = base_score + confidence_adjustment + news_adjustment
            final_score = max(0, min(100, final_score))

            return final_score

        except Exception as e:
            self.logger.error(f"情绪得分计算失败: {e}")
            return 50


