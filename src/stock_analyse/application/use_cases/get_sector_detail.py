from __future__ import annotations

from stock_analyse.infrastructure.data_sources.concepts.ths_concept_client import stockConceptData


def execute(market: str, name: str, gateway: stockConceptData | None = None) -> dict:
    try:
        gateway = gateway or stockConceptData()
        board_df = gateway.stock_board_concept_name_ths()
        df = gateway.stock_board_concept_info_ths(symbol=name, stock_board_ths_map_df=board_df)
        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "无板块详情"}

        info = dict(zip(df['项目'], df['值']))
        return {
            "success": True,
            "data": {
                "sector_name": name,
                "market": market,
                "info": info,
            },
            "message": "获取成功",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"获取失败: {exc}"}
