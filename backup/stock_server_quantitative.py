# Legacy web entrypoint retained for compatibility; the primary Flask bootstrap now lives in stock_web.py.
from stock_web import app


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
