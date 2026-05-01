"""SSE 连接管理组件。

负责维护浏览器连接，并把 AI 主链路中的事件安全地序列化后推送给指定客户端。
"""

from __future__ import annotations

import json
import logging
import math
import threading
from datetime import date, datetime, time

import numpy as np
import pandas as pd


class SSEManager:
    """管理 SSE 客户端与消息分发。

    负责注册连接、移除失效连接，并在发送前把复杂对象清洗为可序列化数据。
    """

    def __init__(self):
        """初始化连接池、线程锁与日志器。"""
        self.clients = {}
        self.lock = threading.Lock()
        self.logger = logging.getLogger(__name__)

    def add_client(self, client_id, queue):
        with self.lock:
            self.clients[client_id] = queue
            self.logger.info(f'SSE客户端连接: {client_id}')

    def remove_client(self, client_id):
        with self.lock:
            if client_id in self.clients:
                del self.clients[client_id]
                self.logger.info(f'SSE客户端断开: {client_id}')

    def send_to_client(self, client_id, event_type, data):
        with self.lock:
            if client_id in self.clients:
                try:
                    cleaned_data = self.clean_data_for_json(data)
                    message = {
                        'event': event_type,
                        'data': cleaned_data,
                        'timestamp': datetime.now().isoformat(),
                    }
                    self.clients[client_id].put(message, block=False)
                    return True
                except Exception as exc:
                    self.logger.error(f'SSE消息发送失败: {exc}')
                    return False
            return False

    def broadcast(self, event_type, data):
        with self.lock:
            cleaned_data = self.clean_data_for_json(data)
            message = {
                'event': event_type,
                'data': cleaned_data,
                'timestamp': datetime.now().isoformat(),
            }
            dead_clients = []
            for client_id, queue in self.clients.items():
                try:
                    queue.put(message, block=False)
                except Exception as exc:
                    self.logger.error(f'SSE广播失败给客户端 {client_id}: {exc}')
                    dead_clients.append(client_id)
            for client_id in dead_clients:
                del self.clients[client_id]

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
