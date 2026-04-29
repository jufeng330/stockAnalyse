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


class SSEManager:
    """SSE连接管理器"""

    def __init__(self):
        self.clients = {}
        self.lock = threading.Lock()
        self.logger = logging.getLogger(__name__)

    def add_client(self, client_id, queue):
        """添加SSE客户端"""
        with self.lock:
            self.clients[client_id] = queue
            self.logger.info(f"SSE客户端连接: {client_id}")

    def remove_client(self, client_id):
        """移除SSE客户端"""
        with self.lock:
            if client_id in self.clients:
                del self.clients[client_id]
                self.logger.info(f"SSE客户端断开: {client_id}")

    def send_to_client(self, client_id, event_type, data):
        """向特定客户端发送消息"""
        with self.lock:
            if client_id in self.clients:
                try:
                    # 清理数据确保JSON可序列化
                    cleaned_data = self.clean_data_for_json(data)
                    message = {
                        'event': event_type,
                        'data': cleaned_data,
                        'timestamp': datetime.now().isoformat()
                    }
                    self.clients[client_id].put(message, block=False)
                    return True
                except Exception as e:
                    self.logger.error(f"SSE消息发送失败: {e}")
                    return False
            return False

    def broadcast(self, event_type, data):
        """广播消息给所有客户端"""
        with self.lock:
            # 清理数据确保JSON可序列化
            cleaned_data = self.clean_data_for_json(data)
            message = {
                'event': event_type,
                'data': cleaned_data,
                'timestamp': datetime.now().isoformat()
            }

            dead_clients = []
            for client_id, queue in self.clients.items():
                try:
                    queue.put(message, block=False)
                except Exception as e:
                    self.logger.error(f"SSE广播失败给客户端 {client_id}: {e}")
                    dead_clients.append(client_id)

            # 清理死连接
            for client_id in dead_clients:
                del self.clients[client_id]

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