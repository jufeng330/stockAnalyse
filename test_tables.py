from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine('mysql+pymysql://root:aloo.1234-qwer@192.168.1.12:3306/stock_info')

for tbl in ['stock_border_SH', 'stock_border_H', 'stock_border_usa', 'financial_indicate_H', 'financial_indicate_usa']:
    try:
        with engine.connect() as con:
            res = con.execute(text(f"SELECT count(*) FROM {tbl}"))
            count = res.scalar()
            print(f"{tbl} count: {count}")
    except Exception as e:
        print(f"Error checking {tbl}: {e}")

