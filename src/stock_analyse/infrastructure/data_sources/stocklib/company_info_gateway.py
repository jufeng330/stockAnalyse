from __future__ import annotations

from stocklib.stock_company import stockCompanyInfo


class CompanyInfoGateway:
    def get_stock_info_parts(self, market: str, symbol: str) -> tuple[object, str, str, str, str, str]:
        stock = stockCompanyInfo(marker=market, symbol=symbol)
        info_df = stock.get_stock_individual_info()
        name = stock.get_stock_name()
        _, list_date, industry = stock.get_stock_individual_info_em()
        concept = stock.get_stock_concept_by_code(symbol)
        sector = stock.get_stock_industry_by_code(symbol)
        return info_df, name, list_date, industry, concept, sector

    def get_stock_history_data(self, market: str, symbol: str, start_date: str | None = None, end_date: str | None = None):
        stock = stockCompanyInfo(marker=market, symbol=symbol)
        return stock.get_stock_history_data(start_date_str=start_date, end_date_str=end_date)
