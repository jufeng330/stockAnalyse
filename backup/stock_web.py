import logging

from stock_analyse.interfaces.web.app import create_app, web_app_context
from stock_analyse.interfaces.web.routes.analysis import register_analysis_routes
from stock_analyse.interfaces.web.routes.auth import check_auth_config, register_auth_routes, require_auth
from stock_analyse.interfaces.web.routes.history import register_history_routes
from stock_analyse.interfaces.web.routes.misc import register_misc_routes

logger = logging.getLogger(__name__)

app = create_app()
settings = web_app_context.settings
analyzer = web_app_context.analyzer

register_analysis_routes(app)
register_history_routes(app)
register_auth_routes(app)
register_misc_routes(app)

task_lock = web_app_context.task_lock
analysis_tasks = web_app_context.analysis_tasks

# analysis-related routes are registered from src/stock_analyse/interfaces/web/routes/analysis.py


# history routes are registered from src/stock_analyse/interfaces/web/routes/history.py


# misc page routes are registered from src/stock_analyse/interfaces/web/routes/misc.py


# auth routes are registered from src/stock_analyse/interfaces/web/routes/auth.py

# streaming and async analysis endpoints are registered from src/stock_analyse/interfaces/web/routes/analysis.py


# auth config helper is provided by src/stock_analyse/interfaces/web/routes/auth.py


if __name__ == '__main__':
    app.run(debug=True,host='0.0.0.0',port = 38080)
