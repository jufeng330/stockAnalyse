from __future__ import annotations

from stock_analyse.infrastructure.data_sources.concepts.ths_concept_client import stockConceptData


def execute(market: str, name: str, gateway: stockConceptData | None = None) -> dict:
    try:
        gateway = gateway or stockConceptData()
        board_df = gateway.stock_board_concept_name_ths()
        df = gateway.stock_board_concept_cons_ths(symbol=name, stock_board_ths_map_df=board_df)
        if df is None or df.empty:
            return {"success": False, "data": {}, "message": "暂不支持行业板块成分股查询"}

        records = df.to_dict(orient='records')
        return {
            "success": True,
            "data": {
                "sector_name": name,
                "market": market,
                "type": "concept",
                "count": len(records),
                "stocks": records,
            },
            "message": f"共 {len(records)} 只成分股",
        }
    except Exception as exc:
        return {"success": False, "data": {}, "message": f"获取失败: {exc}"}
