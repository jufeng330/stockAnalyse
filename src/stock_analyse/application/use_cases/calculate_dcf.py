from __future__ import annotations

from stock_analyse.domain.services.valuation_service import ValuationService
from stock_analyse.infrastructure.services.valuation_gateway import ValuationGateway



def execute(market: str, symbol: str, discount_rate: float = 0.1, growth_rate: float = 0.03, gateway: ValuationGateway | None = None, service: ValuationService | None = None) -> dict:
    try:
        gateway = gateway or ValuationGateway()
        service = service or ValuationService()
        zcfz, lrb, xjll = gateway.get_stock_report(market=market, symbol=symbol, years=5)
        if zcfz is None or lrb is None or xjll is None:
            return {"success": False, "data": {}, "message": "无法获取财务报表"}

        dcf_value = gateway.calculate_dcf(
            market=market,
            symbol=symbol,
            cashflow_df=xjll,
            profit_df=lrb,
            discount_rate=discount_rate,
            growth_rate=growth_rate,
        )

        if '资产-总股本' in zcfz.columns:
            total_shares = zcfz['资产-总股本'].iloc[0]
        elif '实收资本(或股本)' in zcfz.columns:
            total_shares = zcfz['实收资本(或股本)'].iloc[0]
        else:
            total_shares = 0

        total_shares = float(total_shares or 0)
        dcf_value = float(dcf_value or 0)

        if total_shares > 1000000:
            total_shares = total_shares / 10000
        if abs(dcf_value) > 1000000:
            dcf_value = dcf_value / 10000
        if dcf_value < 0:
            dcf_value = 0
        if total_shares < 0:
            total_shares = 0

        value_per_share = service.value_per_share(dcf_value, total_shares)

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "market": market,
                "dcf_value": round(dcf_value, 2),
                "total_shares": round(total_shares, 2),
                "value_per_share": round(value_per_share, 2),
                "discount_rate": discount_rate,
                "growth_rate": growth_rate,
            },
            "message": f"DCF估值: {round(value_per_share, 2)} 元/股",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"计算失败: {exc}"}
