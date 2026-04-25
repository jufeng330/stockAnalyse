from __future__ import annotations

import json
import math
from datetime import date, datetime, time

import numpy as np
import pandas as pd


class StreamingAnalyzer:
    """流式分析器"""

    def __init__(self, client_id, sse_manager):
        self.client_id = client_id
        self.sse_manager = sse_manager

    def send_log(self, message, log_type='info'):
        self.sse_manager.send_to_client(self.client_id, 'log', {'message': message, 'type': log_type})

    def send_progress(self, element_id, percent, message=None, current_stock=None):
        self.sse_manager.send_to_client(
            self.client_id,
            'progress',
            {'element_id': element_id, 'percent': percent, 'message': message, 'current_stock': current_stock},
        )

    def send_scores(self, scores, animate=True):
        self.sse_manager.send_to_client(self.client_id, 'scores_update', {'scores': scores, 'animate': animate})

    def send_data_quality(self, data_quality):
        self.sse_manager.send_to_client(self.client_id, 'data_quality_update', data_quality)

    def send_partial_result(self, data):
        self.sse_manager.send_to_client(self.client_id, 'partial_result', self.clean_data_for_json(data))

    def send_final_result(self, result):
        self.sse_manager.send_to_client(self.client_id, 'final_result', self.clean_data_for_json(result))

    def send_history_result(self, result):
        self.sse_manager.send_to_client(self.client_id, 'history_result', self.clean_data_for_json(result))

    def send_select_result(self, result):
        self.sse_manager.send_to_client(self.client_id, 'select_result', self.clean_data_for_json(result))

    def send_batch_result(self, results):
        self.sse_manager.send_to_client(self.client_id, 'batch_result', self.clean_data_for_json(results))

    def send_completion(self, message=None):
        self.sse_manager.send_to_client(self.client_id, 'analysis_complete', {'message': message or '分析完成'})

    def send_error(self, error_message):
        self.sse_manager.send_to_client(self.client_id, 'analysis_error', {'error': error_message})

    def send_ai_stream(self, content):
        self.sse_manager.send_to_client(self.client_id, 'ai_stream', {'content': content})

    def clean_data_for_json(self, obj):
        if isinstance(obj, dict):
            return {key: self.clean_data_for_json(value) for key, value in obj.items()}
        if isinstance(obj, list):
            return [self.clean_data_for_json(item) for item in obj]
        if isinstance(obj, tuple):
            return [self.clean_data_for_json(item) for item in obj]
        if isinstance(obj, (int, float)):
            if math.isnan(obj) or math.isinf(obj):
                return None
            return obj
        if isinstance(obj, np.ndarray):
            return self.clean_data_for_json(obj.tolist())
        if isinstance(obj, (np.integer, np.floating)):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return obj.item()
        if isinstance(obj, (datetime, date)):
            return obj.isoformat() if hasattr(obj, 'isoformat') else str(obj)
        if isinstance(obj, time):
            return obj.isoformat()
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if isinstance(obj, pd.NaT.__class__):
            return None
        if pd.isna(obj):
            return None
        if hasattr(obj, 'to_dict'):
            try:
                return self.clean_data_for_json(obj.to_dict())
            except Exception:
                return str(obj)
        if hasattr(obj, 'item'):
            try:
                return self.clean_data_for_json(obj.item())
            except Exception:
                return str(obj)
        if obj is None or isinstance(obj, (str, bool)):
            return obj
        try:
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            return str(obj)
