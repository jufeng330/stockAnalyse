from __future__ import annotations

import logging
from datetime import datetime

from functools import wraps

from flask import current_app, redirect, render_template, request, session, url_for

logger = logging.getLogger(__name__)



def check_auth_config():
    context = current_app.extensions['stock_analyse.context']
    return context.check_auth_config()



def require_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_enabled, auth_config = check_auth_config()
        if not auth_enabled:
            return f(*args, **kwargs)
        if session.get('authenticated'):
            login_time = session.get('login_time')
            if login_time:
                session_timeout = auth_config.get('session_timeout', 3600)
                if (datetime.now() - datetime.fromisoformat(login_time)).total_seconds() < session_timeout:
                    return f(*args, **kwargs)
                session.pop('authenticated', None)
                session.pop('login_time', None)
        return redirect(url_for('login'))

    return decorated_function


def register_auth_routes(app):
    @app.route('/login', methods=['GET', 'POST'])
    def login():
        auth_enabled, auth_config = check_auth_config()
        if not auth_enabled:
            return redirect(url_for('index'))

        if request.method == 'POST':
            password = request.form.get('password', '')
            config_password = auth_config.get('password', '')
            if not config_password:
                return render_template(
                    'login.html',
                    error='系统未设置访问密码，请联系管理员配置',
                    session_timeout=auth_config.get('session_timeout', 3600) // 60,
                )

            if password == config_password:
                session['authenticated'] = True
                session['login_time'] = datetime.now().isoformat()
                logger.info('用户登录成功')
                return redirect(url_for('index'))

            logger.warning('用户登录失败：密码错误')
            return render_template(
                'login.html',
                error='密码错误，请重试',
                session_timeout=auth_config.get('session_timeout', 3600) // 60,
            )

        return render_template(
            'login.html',
            session_timeout=auth_config.get('session_timeout', 3600) // 60,
        )
