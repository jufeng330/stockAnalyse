import pandas as pd
from sqlalchemy import create_engine
from stock_analyse.infrastructure.services.futu_market_data_provider import FutuMarketDataProvider
import logging

logging.basicConfig(level=logging.INFO)

engine = create_engine('mysql+pymysql://root:aloo.1234-qwer@192.168.1.12:3306/stock_info')

def load_futu_spot(market, table_name, start_batch):
    print(f"Fetching {market} spot data from batch {start_batch}...")
    provider = FutuMarketDataProvider(market)
    
    with engine.connect() as con:
        res = con.execute(text(f"SELECT DISTINCT code FROM stock_industry_{market}"))
        stock_codes = [row[0] for row in res]
    
    print(f"Found {len(stock_codes)} stocks for {market}")
    
    from stock_analyse.infrastructure.data_sources.futu.futu_quote_client import FutuQuoteClient
    client = FutuQuoteClient()
    
    import time
    
    all_data = []
    batch_size = 300
    for i in range(start_batch * batch_size, len(stock_codes), batch_size):
        batch_codes = stock_codes[i:i+batch_size]
        print(f"Fetching snapshot batch {i//batch_size + 1}/{(len(stock_codes)+batch_size-1)//batch_size}")
        
        retry = 0
        while retry < 3:
            try:
                df = client.get_market_snapshot(batch_codes, skip_unsupported=True)
                if df is not None and not df.empty:
                    df = provider._map_snapshot_frame(df, market)
                    all_data.append(df)
                break
            except Exception as e:
                print(f"Error fetching snapshot: {e}")
                time.sleep(5)
                retry += 1
        time.sleep(0.6)
        
    if all_data:
        full_df = pd.concat(all_data, ignore_index=True)
        col_mapping = {
            '代码': 'code',
            '名称': 'name',
            '最新价': 'latest_price',
            '涨跌幅': 'change_rate',
            '涨跌额': 'change_amount',
            '今开': 'open_price',
            '最高': 'high_price',
            '最低': 'low_price',
            '昨收': 'prev_close',
            '成交量': 'volume',
            '成交额': 'turnover',
            '换手率': 'turnover_rate',
            '市盈率-动态': 'pe_dynamic',
            '总市值': 'total_market_value',
            '市净率': 'pb'
        }
        
        mapped_df = pd.DataFrame()
        for cn, en in col_mapping.items():
            if cn in full_df.columns:
                mapped_df[en] = pd.to_numeric(full_df[cn], errors='coerce') if en not in ['code', 'name'] else full_df[cn]
                
        mapped_df.drop_duplicates(subset=['code'], inplace=True)
        
        mapped_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"Appended {len(mapped_df)} rows to {table_name}")
    else:
        print(f"No {market} spot data retrieved.")
        
from sqlalchemy import text
# The last log was:
# Fetching snapshot batch 21/21 for usa
# So we resume from batch 20 (index 20)
load_futu_spot('usa', 'stock_border_usa', 20)
