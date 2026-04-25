from __future__ import annotations


class TechnicalIndicatorService:
    @staticmethod
    def signal_from_value(value) -> str:
        if value == 1:
            return "buy"
        if value == -1:
            return "sell"
        return "neutral"

    @staticmethod
    def rsi_signal(value) -> str:
        if value is None:
            return "neutral"
        if value > 70:
            return "overbought"
        if value < 30:
            return "oversold"
        return "neutral"

    @staticmethod
    def build_indicator_data(symbol: str, indicator: str, signal: str, last_price, **indicator_values) -> dict:
        return {
            "symbol": symbol,
            "indicator": indicator,
            "indicator_values": indicator_values,
            "signal": signal,
            "last_price": last_price,
            **indicator_values,
        }

    @staticmethod
    def score_results(results: dict) -> int:
        score = 0
        for result in results.values():
            signal = result.get("signal")
            if signal in {"buy", "oversold"}:
                score += 15
            elif signal in {"sell", "overbought"}:
                score -= 10
        return max(0, score)

    @staticmethod
    def recommendation_from_score(score: float) -> str:
        if score >= 50:
            return "强烈推荐买入"
        if score >= 30:
            return "建议买入"
        if score >= 10:
            return "建议持有"
        return "建议观望"

    @staticmethod
    def williams_signal(value) -> str:
        if value is None:
            return "neutral"
        if value < -80:
            return "oversold"
        if value > -20:
            return "overbought"
        return "neutral"

    @staticmethod
    def overall_signal(results: dict) -> str:
        buy_signals = sum(1 for result in results.values() if result.get("signal") in {"buy", "oversold"})
        sell_signals = sum(1 for result in results.values() if result.get("signal") in {"sell", "overbought"})
        if buy_signals > sell_signals:
            return "buy"
        if sell_signals > buy_signals:
            return "sell"
        return "neutral"

    @classmethod
    def summarize(cls, results: dict) -> dict:
        buy_signals = sum(1 for result in results.values() if result.get("signal") in {"buy", "oversold"})
        sell_signals = sum(1 for result in results.values() if result.get("signal") in {"sell", "overbought"})
        score = cls.score_results(results)
        return {
            "buy_signals": buy_signals,
            "sell_signals": sell_signals,
            "neutral_signals": len(results) - buy_signals - sell_signals,
            "signal": cls.overall_signal(results),
            "score": score,
            "recommendation": cls.recommendation_from_score(score),
        }
