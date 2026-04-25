from __future__ import annotations

import unittest

from stock_analyse.application.services.ai_stock_data_facade import AIStockDataFacade


class StubCompanyService:
    def __init__(self) -> None:
        self.symbol = '600000'

    def get_stock_individual_info(self):
        return {'name': '浦发银行'}

    def get_stock_name(self):
        return '浦发银行'

    def get_stock_zyjs(self):
        return {'business': '银行'}

    def get_stock_industry_by_code(self, code):
        return '银行'

    def get_stock_concept_by_code(self, code):
        return '中字头'

    def get_stock_financial_analysis_indicator(self):
        return None

    def get_stock_individual_fund_flow(self):
        return None


class StubSpotRow:
    def to_dict(self):
        return {'股票代码': '600000', '名称': '浦发银行'}


class StubFilteredSpot:
    empty = False

    class _Iloc:
        def __getitem__(self, index):
            return StubSpotRow()

    iloc = _Iloc()


class StubSpotFrame:
    columns = ['股票代码']
    empty = False

    def __getitem__(self, key):
        if key == '股票代码':
            return self
        return StubFilteredSpot()

    def __eq__(self, other):
        return 'comparison'


class StubMarketService:
    def get_stock_spot(self):
        return StubSpotFrame()


class StubWorkflow:
    def run(self, *, stock_code: str, market: str):
        return 88, {'score': 88, 'stock_code': stock_code}


class StubReportService:
    def get_stock_report(self, stock_code='600000', market='SH'):
        return None, None, None


class AIStockDataFacadeTest(unittest.TestCase):
    def test_build_snapshot_uses_project_services(self):
        facade = AIStockDataFacade(
            technical_analysis_workflow=StubWorkflow(),
            company_service_factory=lambda market, stock_code: StubCompanyService(),
            market_service_factory=lambda market: StubMarketService(),
            report_service=StubReportService(),
        )

        snapshot = facade.build_snapshot(stock_code='600000', market='SH', include_sentiment=False)

        self.assertEqual(snapshot['stock_code'], '600000')
        self.assertEqual(snapshot['company_name'], '浦发银行')
        self.assertEqual(snapshot['industry'], '银行')
        self.assertEqual(snapshot['technical']['score'], 88)
        self.assertEqual(snapshot['reports']['balance_sheet'], [])


if __name__ == '__main__':
    unittest.main()
