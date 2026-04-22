from __future__ import annotations

from stocklib.stock_annual_report import stockAnnualReport


class ReportGateway:
    def get_stock_report(self, market: str, symbol: str, years: int = 5):
        report = stockAnnualReport()
        return report.get_stock_report(stock_code=symbol, market=market, years=years)
