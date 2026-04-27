from __future__ import annotations

import pandas as pd

from stock_analyse.interfaces.web.streaming.streaming_analyzer import StreamingAnalyzer


class StubSseManager:
    def __init__(self):
        self.messages = []

    def send_to_client(self, client_id, event, data):
        self.messages.append({'client_id': client_id, 'event': event, 'data': data})


def test_send_final_result_serializes_dataframe_and_series_without_truthiness_error():
    manager = StubSseManager()
    analyzer = StreamingAnalyzer('client-1', manager)

    analyzer.send_final_result(
        {
            'success': True,
            'data': {
                'table': pd.DataFrame([
                    {'代码': '300750', '最新价': 182.4},
                    {'代码': '600519', '最新价': 1688.0},
                ]),
                'row': pd.Series({'动作': 'hold', '置信度': 0.79}),
            },
        }
    )

    assert len(manager.messages) == 1
    payload = manager.messages[0]
    assert payload['client_id'] == 'client-1'
    assert payload['event'] == 'final_result'
    assert payload['data']['data']['table'] == [
        {'代码': '300750', '最新价': 182.4},
        {'代码': '600519', '最新价': 1688.0},
    ]
    assert payload['data']['data']['row'] == {'动作': 'hold', '置信度': 0.79}
