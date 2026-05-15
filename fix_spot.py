import pandas as pd
from sqlalchemy import create_engine
from stock_analyse.infrastructure.services.futu_market_data_provider import FutuMarketDataProvider
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo
import logging

logging.basicConfig(level=logging.INFO)

engine = create_engine('mysql+pymysql://root:aloo.1234-qwer@192.168.1.12:3306/stock_info')

# 1. Fetch SH using AkShare (Futu doesn't support SH directly as per code error)
def load_sh_spot():
    print("Fetching SH spot data...")
    market_service = stockBorderInfo(market='SH')
    sh_df = market_service.get_stock_spot()
    if sh_df is not None and not sh_df.empty:
        # Rename columns to match db
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
        
        # Only keep columns we have mapping for
        sh_df_mapped = pd.DataFrame()
        for cn, en in col_mapping.items():
            if cn in sh_df.columns:
                sh_df_mapped[en] = pd.to_numeric(sh_df[cn], errors='coerce') if en not in ['code', 'name'] else sh_df[cn]
        
        if 'code' not in sh_df_mapped.columns and '股票代码' in sh_df.columns:
             sh_df_mapped['code'] = sh_df['股票代码']
             
        sh_df_mapped.drop_duplicates(subset=['code'], inplace=True)
        
        with engine.connect() as con:
            con.execute(text("TRUNCATE TABLE stock_border_SH"))
            
        sh_df_mapped.to_sql(name='stock_border_SH', con=engine, if_exists='append', index=False)
        print(f"Saved {len(sh_df_mapped)} rows to stock_border_SH")
    else:
        print("No SH spot data retrieved.")

def load_futu_spot(market, table_name):
    print(f"Fetching {market} spot data...")
    provider = FutuMarketDataProvider(market)
    
    # We will get stock list from DB (plate lists) since futu provider spot gets from cache or seed
    with engine.connect() as con:
        res = con.execute(text(f"SELECT DISTINCT code FROM stock_industry_{market}"))
        stock_codes = [row[0] for row in res]
    
    print(f"Found {len(stock_codes)} stocks for {market}")
    
    # Batched snapshot requests (max 400 per request according to futu docs)
    # The limit is 60 requests per 30 seconds
    from stock_analyse.infrastructure.data_sources.futu.futu_quote_client import FutuQuoteClient
    client = FutuQuoteClient()
    
    import time
    
    all_data = []
    batch_size = 300
    for i in range(0, len(stock_codes), batch_size):
        batch_codes = stock_codes[i:i+batch_size]
        print(f"Fetching snapshot batch {i//batch_size + 1}/{(len(stock_codes)+batch_size-1)//batch_size}")
        
        retry = 0
        while retry < 3:
            try:
                df = client.get_market_snapshot(batch_codes, skip_unsupported=True)
                if df is not None and not df.empty:
                    # Rename columns to match the database mapping
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
        
        with engine.connect() as con:
            con.execute(text(f"TRUNCATE TABLE {table_name}"))
            
        mapped_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"Saved {len(mapped_df)} rows to {table_name}")
    else:
        print(f"No {market} spot data retrieved.")
        
from sqlalchemy import text
load_sh_spot()
load_futu_spot('H', 'stock_border_H')
load_futu_spot('usa', 'stock_border_usa')

