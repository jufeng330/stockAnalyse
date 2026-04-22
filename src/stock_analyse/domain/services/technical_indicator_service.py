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
    def summarize(results: dict) -> dict:
        buy_signals = sum(1 for result in results.values() if result.get("signal") in {"buy", "oversold"})
        sell_signals = sum(1 for result in results.values() if result.get("signal") in {"sell", "overbought"})
        return {
            "buy_signals": buy_signals,
            "sell_signals": sell_signals,
            "neutral_signals": len(results) - buy_signals - sell_signals,
        }
