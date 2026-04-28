from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor

import matplotlib
from flask import Flask

from stock_analyse.infrastructure.config.settings import get_settings
from stock_analyse.interfaces.web.routes import (
    register_analysis_routes,
    register_auth_routes,
    register_history_routes,
    register_misc_routes,
    register_trading_decision_routes,
)
from stock_analyse.interfaces.web.services.stock_analyzer_service import StockAnalyzerService
from stock_analyse.interfaces.web.services.trading_decision_service import TradingDecisionService
from stock_analyse.interfaces.web.streaming.sse_manager import SSEManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

matplotlib.use('Agg')


class WebAppContext:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.sse_manager = SSEManager()
        self.analyzer = StockAnalyzerService()
        self.analysis_tasks: dict = {}
        self.task_results: dict = {}
        self.task_lock = threading.Lock()
        self.sse_clients: dict = {}
        self.sse_lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.trading_decision_service = TradingDecisionService()

    def check_auth_config(self) -> tuple[bool, dict]:
        if not self.analyzer:
            return False, {}
        web_auth_config = self.settings.as_service_config().get('web_auth', {})
        return web_auth_config.get('enabled', False), web_auth_config


web_app_context = WebAppContext()


def create_app() -> Flask:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..'))
    app = Flask(
        __name__,
        template_folder=os.path.join(project_root, 'templates'),
        static_folder=os.path.join(project_root, 'static'),
    )
    app.secret_key = web_app_context.settings.web.flask_secret_key
    app.extensions['stock_analyse.context'] = web_app_context
    register_misc_routes(app)
    register_auth_routes(app)
    register_history_routes(app)
    register_analysis_routes(app)
    register_trading_decision_routes(app)
    return app

# app.py

from flask import Flask

app = Flask(__name__)


# ====== 新增：主入口 ======
if __name__ == '__main__':
    app = create_app()
    app.run(
        host='0.0.0.0',   # 允许外部访问（可选）
        port=38080,        # 默认端口
        debug=True,        # 开启调试模式（开发时方便，生产环境务必关闭！）
        use_reloader=False
    )