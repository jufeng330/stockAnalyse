
import logging
import json
import threading
import time
from datetime import datetime, timedelta
import os
import sys
import math
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import asyncio
from functools import wraps
import hashlib
import secrets
import uuid
from queue import Queue, Empty

class StreamingAnalyzer:
    """流式分析器"""

    def __init__(self, client_id,sse_manager):
        self.client_id = client_id
        self.sse_manager = sse_manager

    def send_log(self, message, log_type='info'):
        """发送日志消息"""
        self.sse_manager.send_to_client(self.client_id, 'log', {
            'message': message,
            'type': log_type
        })

    def send_progress(self, element_id, percent, message=None, current_stock=None):
        """发送进度更新"""
        self.sse_manager.send_to_client(self.client_id, 'progress', {
            'element_id': element_id,
            'percent': percent,
            'message': message,
            'current_stock': current_stock
        })

    def send_scores(self, scores, animate=True):
        """发送评分更新"""
        self.sse_manager.send_to_client(self.client_id, 'scores_update', {
            'scores': scores,
            'animate': animate
        })

    def send_data_quality(self, data_quality):
        """发送数据质量指标"""
        self.sse_manager.send_to_client(self.client_id, 'data_quality_update', data_quality)

    def send_partial_result(self, data):
        """发送部分结果"""
        cleaned_data = self.clean_data_for_json(data)
        self.sse_manager.send_to_client(self.client_id, 'partial_result', cleaned_data)

    def send_final_result(self, result):
        """发送最终结果"""
        cleaned_result = self.clean_data_for_json(result)
        self.sse_manager.send_to_client(self.client_id, 'final_result', cleaned_result)

    def send_history_result(self, result):
        """发送最终结果"""
        cleaned_result = self.clean_data_for_json(result)
        self.sse_manager.send_to_client(self.client_id, 'history_result', cleaned_result)

    def send_select_result(self, result):
        """发送最终结果"""
        cleaned_result = self.clean_data_for_json(result)
        self.sse_manager.send_to_client(self.client_id, 'select_result', cleaned_result)

    def send_batch_result(self, results):
        """发送批量结果"""
        cleaned_results = self.clean_data_for_json(results)
        self.sse_manager.send_to_client(self.client_id, 'batch_result', cleaned_results)

    def send_completion(self, message=None):
        """发送完成信号"""
        self.sse_manager.send_to_client(self.client_id, 'analysis_complete', {
            'message': message or '分析完成'
        })

    def send_error(self, error_message):
        """发送错误信息"""
        self.sse_manager.send_to_client(self.client_id, 'analysis_error', {
            'error': error_message
        })

    def send_ai_stream(self, content):
        """发送AI流式内容"""
        self.sse_manager.send_to_client(self.client_id, 'ai_stream', {
            'content': content
        })

    def clean_data_for_json(self,obj):
        """清理数据中的NaN、Infinity、日期等无效值，使其能够正确序列化为JSON"""
        import pandas as pd
        from datetime import datetime, date, time

        if isinstance(obj, dict):
            return {key: self.clean_data_for_json(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.clean_data_for_json(item) for item in obj]
        elif isinstance(obj, tuple):
            return [self.clean_data_for_json(item) for item in obj]
        elif isinstance(obj, (int, float)):
            if math.isnan(obj):
                return None
            elif math.isinf(obj):
                return None
            else:
                return obj
        elif isinstance(obj, np.ndarray):
            return self.clean_data_for_json(obj.tolist())
        elif isinstance(obj, (np.integer, np.floating)):
            if np.isnan(obj):
                return None
            elif np.isinf(obj):
                return None
            else:
                return obj.item()
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat() if hasattr(obj, 'isoformat') else str(obj)
        elif isinstance(obj, time):
            return obj.isoformat()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, pd.NaT.__class__):
            return None
        elif pd.isna(obj):
            return None
        elif hasattr(obj, 'to_dict'):  # DataFrame或Series
            try:
                return self.clean_data_for_json(obj.to_dict())
            except:
                return str(obj)
        elif hasattr(obj, 'item'):  # numpy标量
            try:
                return self.clean_data_for_json(obj.item())
            except:
                return str(obj)
        elif obj is None:
            return None
        elif isinstance(obj, (str, bool)):
            return obj
        else:
            # 对于其他不可序列化的对象，转换为字符串
            try:
                # 尝试直接序列化测试
                json.dumps(obj)
                return obj
            except (TypeError, ValueError):
                return str(obj)

