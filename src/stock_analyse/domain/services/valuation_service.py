from __future__ import annotations


class ValuationService:
    @staticmethod
    def value_per_share(dcf_value: float, total_shares: float) -> float:
        if not total_shares or total_shares <= 0:
            return 0
        return dcf_value / total_shares

    @staticmethod
    def midpoint(lower: float, upper: float) -> float:
        return (lower + upper) / 2

    @staticmethod
    def margin_of_safety(current_price: float, target_price: float) -> float:
        if current_price <= 0 or target_price <= 0:
            return 0
        return (target_price - current_price) / current_price * 100

    @staticmethod
    def compare_status(current_price: float, price_range: dict) -> tuple[str, str]:
        if current_price < price_range['conservative']:
            return '严重低估', '当前价格低于保守估值，具备较高安全边际'
        if current_price < price_range['normal']:
            return '轻度低估', '当前价格低于正常估值，具备一定安全边际'
        if current_price < price_range['optimistic']:
            return '合理估值', '当前价格在合理区间内'
        return '高估', '当前价格高于乐观估值，注意风险'
