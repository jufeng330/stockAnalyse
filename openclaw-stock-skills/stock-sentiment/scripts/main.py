#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Stock Sentiment Skill - 股票情绪分析 Skill
提供股票新闻数据获取、市场情绪分析、情绪评分计算等情绪分析功能
"""

from stocklib import StockSentimentAnalysis, stockNewsData, stockCompanyInfo


def get_sentiment_analysis(market: str, symbol: str, days: int = 7) -> dict:
    """
    获取股票情绪分析

    Args:
        market: 市场代码 (SH/SZ/H/usa)
        symbol: 股票代码
        days: 获取最近几天的新闻

    Returns:
        dict: 情绪分析结果
    """
    try:
        sentiment = StockSentimentAnalysis(market=market, symbol=symbol)
        score, analysis = sentiment.get_sentiment_analysis()

        # 判断情绪状态
        if score > 50:
            state = "强烈乐观"
        elif score > 20:
            state = "乐观"
        elif score > -20:
            state = "中性"
        elif score > -50:
            state = "悲观"
        else:
            state = "强烈悲观"

        return {
            "股票代码": symbol,
            "情绪得分": score,
            "情绪状态": state,
            "整体情绪": analysis.get('overall_sentiment', ''),
            "情绪趋势": analysis.get('sentiment_trend', ''),
            "置信度": analysis.get('confidence', 0) * 100,
            "正向比例": analysis.get('positive_ratio', 0) * 100,
            "负向比例": analysis.get('negative_ratio', 0) * 100
        }
    except Exception as e:
        return {"error": str(e)}


def get_stock_news(market: str, symbol: str) -> dict:
    """
    获取个股新闻

    Args:
        market: 市场代码
        symbol: 股票代码

    Returns:
        dict: 新闻数据
    """
    try:
        stock = stockCompanyInfo(market=market, symbol=symbol)
        df_news = stock.get_stock_news()

        return {
            "股票代码": symbol,
            "新闻数量": len(df_news),
            "新闻列表": df_news.to_dict('records') if df_news is not None else []
        }
    except Exception as e:
        return {"error": str(e)}


def get_comprehensive_news_data(market: str, symbol: str, days: int = 7) -> dict:
    """
    获取综合新闻数据

    Args:
        market: 市场代码
        symbol: 股票代码
        days: 获取最近几天的新闻

    Returns:
        dict: 综合新闻数据
    """
    try:
        sentiment = StockSentimentAnalysis(market=market, symbol=symbol)
        news_data = sentiment.get_comprehensive_news_data(stock_code=symbol, days=days)

        return {
            "股票代码": symbol,
            "新闻天数": days,
            "新闻总数": news_data.get('total_count', 0),
            "公司新闻数量": len(news_data.get('company_news', [])),
            "公告数量": len(news_data.get('announcements', [])),
            "研究报告数量": len(news_data.get('research_reports', [])),
            "行业新闻数量": len(news_data.get('industry_news', []))
        }
    except Exception as e:
        return {"error": str(e)}


def analyze_sentiment_distribution(market: str, symbol: str, days: int = 7) -> dict:
    """
    分析新闻情绪分布

    Args:
        market: 市场代码
        symbol: 股票代码
        days: 获取最近几天的新闻

    Returns:
        dict: 情绪分布分析结果
    """
    try:
        sentiment = StockSentimentAnalysis(market=market, symbol=symbol)

        # 获取综合新闻数据
        news_data = sentiment.get_comprehensive_news_data(stock_code=symbol, days=days)

        # 计算高级情绪分析
        sentiment_analysis = sentiment.calculate_advanced_sentiment_analysis(news_data)

        return {
            "股票代码": symbol,
            "新闻总数": sentiment_analysis.get('total_count', 0),
            "正向新闻数量": sentiment_analysis.get('positive_count', 0),
            "负向新闻数量": sentiment_analysis.get('negative_count', 0),
            "中性新闻数量": sentiment_analysis.get('neutral_count', 0),
            "综合情绪得分": sentiment_analysis.get('sentiment_score', 0),
            "情绪趋势": sentiment_analysis.get('sentiment_trend', ''),
            "分析置信度": sentiment_analysis.get('confidence', 0) * 100,
            "新闻类型分布": sentiment_analysis.get('news_distribution', {})
        }
    except Exception as e:
        return {"error": str(e)}


def multi_stock_sentiment_comparison(stocks: list, market: str, days: int = 7) -> dict:
    """
    多股票情绪对比

    Args:
        stocks: 股票代码列表
        market: 市场代码
        days: 获取最近几天的新闻

    Returns:
        dict: 多股票情绪对比结果
    """
    try:
        results = []

        for symbol in stocks:
            try:
                sentiment = StockSentimentAnalysis(market=market, symbol=symbol)
                score, analysis = sentiment.get_sentiment_analysis()

                # 获取综合新闻数据
                news_data = sentiment.get_comprehensive_news_data(stock_code=symbol, days=days)

                # 计算高级情绪分析
                sentiment_analysis = sentiment.calculate_advanced_sentiment_analysis(news_data)

                # 判断情绪状态
                if score > 50:
                    state = "强烈乐观"
                elif score > 20:
                    state = "乐观"
                elif score > -20:
                    state = "中性"
                elif score > -50:
                    state = "悲观"
                else:
                    state = "强烈悲观"

                results.append({
                    "股票代码": symbol,
                    "情绪得分": score,
                    "情绪状态": state,
                    "整体情绪": analysis.get('overall_sentiment', ''),
                    "情绪趋势": analysis.get('sentiment_trend', ''),
                    "置信度(%)": analysis.get('confidence', 0) * 100,
                    "正向比例(%)": analysis.get('positive_ratio', 0) * 100,
                    "负向比例(%)": analysis.get('negative_ratio', 0) * 100,
                    "新闻数量": news_data.get('total_count', 0)
                })
            except Exception as e:
                results.append({
                    "股票代码": symbol,
                    "错误": str(e)
                })

        return {"比较结果": results}
    except Exception as e:
        return {"error": str(e)}


def analyze_sentiment_trend(market: str, symbol: str, days: int = 7) -> dict:
    """
    分析情绪趋势

    Args:
        market: 市场代码
        symbol: 股票代码
        days: 获取最近几天的新闻

    Returns:
        dict: 情绪趋势分析结果
    """
    try:
        sentiment = StockSentimentAnalysis(market=market, symbol=symbol)

        # 获取情绪分析
        score, analysis = sentiment.get_sentiment_analysis()

        # 获取综合新闻数据
        news_data = sentiment.get_comprehensive_news_data(stock_code=symbol, days=days)

        # 计算高级情绪分析
        sentiment_analysis = sentiment.calculate_advanced_sentiment_analysis(news_data)

        # 判断情绪状态
        if sentiment_analysis.get('sentiment_score', 0) > 50:
            state = "强烈乐观"
            interpretation = "市场对这只股票高度看好，情绪高涨"
        elif sentiment_analysis.get('sentiment_score', 0) > 20:
            state = "乐观"
            interpretation = "市场对这只股票整体看好"
        elif sentiment_analysis.get('sentiment_score', 0) > -20:
            state = "中性"
            interpretation = "市场对这只股票没有明显偏好"
        elif sentiment_analysis.get('sentiment_score', 0) > -50:
            state = "悲观"
            interpretation = "市场对这只股票存在担忧"
        else:
            state = "强烈悲观"
            interpretation = "市场对这只股票高度悲观，风险较高"

        # 情绪趋势解读
        trend = sentiment_analysis.get('sentiment_trend', '')
        if trend == "上升":
            trend_interpretation = "情绪在改善，市场信心增强"
        elif trend == "下降":
            trend_interpretation = "情绪在恶化，市场信心减弱"
        else:
            trend_interpretation = "情绪相对稳定"

        # 置信度判断
        confidence = sentiment_analysis.get('confidence', 0)
        if confidence > 0.7:
            reliability = "高（新闻数量充足，情绪判断可信）"
        elif confidence > 0.5:
            reliability = "中等（新闻数量一般，情绪判断有一定参考价值）"
        else:
            reliability = "低（新闻数量不足，情绪判断可能不准确）"

        return {
            "股票代码": symbol,
            "当前情绪状态": state,
            "情绪得分": sentiment_analysis.get('sentiment_score', 0),
            "状态解读": interpretation,
            "情绪趋势": trend,
            "趋势解读": trend_interpretation,
            "分析可靠性": reliability,
            "置信度": confidence * 100,
            "正向新闻数量": sentiment_analysis.get('positive_count', 0),
            "负向新闻数量": sentiment_analysis.get('negative_count', 0),
            "中性新闻数量": sentiment_analysis.get('neutral_count', 0)
        }
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    # 测试代码
    print("=== Stock Sentiment Skill 测试 ===")

    # 测试获取情绪分析
    print("\n1. 获取情绪分析:")
    sentiment = get_sentiment_analysis('SH', '601318', days=7)
    if 'error' not in sentiment:
        print(f"情绪得分: {sentiment['情绪得分']}")
        print(f"情绪状态: {sentiment['情绪状态']}")
        print(f"整体情绪: {sentiment['整体情绪']}")
        print(f"置信度: {sentiment['置信度']:.1f}%")
    else:
        print(f"错误: {sentiment['error']}")

    # 测试获取新闻
    print("\n2. 获取个股新闻:")
    news = get_stock_news('SH', '601318')
    if 'error' not in news:
        print(f"新闻数量: {news['新闻数量']}")
    else:
        print(f"错误: {news['error']}")

    # 测试分析情绪分布
    print("\n3. 分析情绪分布:")
    distribution = analyze_sentiment_distribution('SH', '601318', days=7)
    if 'error' not in distribution:
        print(f"新闻总数: {distribution['新闻总数']}")
        print(f"正向新闻: {distribution['正向新闻数量']}")
        print(f"负向新闻: {distribution['负向新闻数量']}")
        print(f"综合情绪得分: {distribution['综合情绪得分']}")
    else:
        print(f"错误: {distribution['error']}")

    # 测试多股票情绪对比
    print("\n4. 多股票情绪对比:")
    comparison = multi_stock_sentiment_comparison(['601318', '600519', '600036'], 'SH', days=7)
    if 'error' not in comparison:
        for result in comparison['比较结果']:
            if '错误' not in result:
                print(f"{result['股票代码']}: 得分={result['情绪得分']:.2f}, "
                      f"状态={result['情绪状态']}")
    else:
        print(f"错误: {comparison['error']}")
