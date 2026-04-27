from __future__ import annotations

from pathlib import Path

from stock_analyse.infrastructure.persistence.trading_decision.watch_stock_repository import WatchStockRepository


def test_watch_stock_repository_crud_and_summary(tmp_path: Path):
    repository = WatchStockRepository(tmp_path / 'trading_decision.sqlite3')

    created = repository.create(
        {
            'stock_code': '300750',
            'stock_name': '宁德时代',
            'market': 'A股',
            'industry': '新能源',
            'asset_type': '成长型',
            'source': 'manual',
            'note': '首次加入',
            'current_price': 182.4,
            'pe': 21.8,
            'current_stage': 'B阶段',
            'current_price_zone': '合理偏低区',
            'suggested_action': '适合做第一笔决策',
            'last_conclusion_summary': '景气仍在',
            'last_analysis_at': '2026-04-26 10:00',
        }
    )

    fetched = repository.get_by_id(created['id'])
    assert fetched is not None
    assert fetched['stock_code'] == '300750'

    updated = repository.update(
        created['id'],
        {
            'note': '更新备注',
            'suggested_action': '计划跟踪',
            'current_price': 190.12,
        },
    )
    assert updated is not None
    assert updated['note'] == '更新备注'
    assert updated['suggested_action'] == '计划跟踪'
    assert updated['current_price'] == 190.12

    listed = repository.list({'page': 1, 'page_size': 20})
    assert listed.pagination['total'] == 1
    assert listed.summary['watch_count'] == 1
    assert listed.summary['decision_ready_count'] == 1
    assert listed.summary['analysis_completed_count'] == 1
    assert listed.summary['planned_count'] == 1

    archived = repository.archive(created['id'])
    assert archived is not None
    assert archived['status'] == 'archived'

    listed_after_archive = repository.list({'page': 1, 'page_size': 20})
    assert listed_after_archive.pagination['total'] == 0
    assert listed_after_archive.summary['watch_count'] == 0


def test_watch_stock_repository_filters(tmp_path: Path):
    repository = WatchStockRepository(tmp_path / 'filters.sqlite3')
    repository.create(
        {
            'stock_code': '601088',
            'stock_name': '中国神华',
            'market': 'A股',
            'industry': '煤炭',
            'asset_type': '红利型',
            'current_stage': 'C阶段',
            'current_price_zone': '合理区',
        }
    )
    repository.create(
        {
            'stock_code': 'SMCI',
            'stock_name': 'Super Micro',
            'market': '美股',
            'industry': '服务器',
            'asset_type': '成长型',
            'current_stage': 'A阶段',
            'current_price_zone': '高波动区',
        }
    )

    by_market = repository.list({'market': 'A股', 'page': 1, 'page_size': 20})
    assert len(by_market.items) == 1
    assert by_market.items[0]['stock_code'] == '601088'

    by_keyword = repository.list({'keyword': 'Micro', 'page': 1, 'page_size': 20})
    assert len(by_keyword.items) == 1
    assert by_keyword.items[0]['stock_code'] == 'SMCI'

    by_stage_zone = repository.list({'stage': 'C阶段', 'price_zone': '合理区', 'page': 1, 'page_size': 20})
    assert len(by_stage_zone.items) == 1
    assert by_stage_zone.items[0]['stock_code'] == '601088'
