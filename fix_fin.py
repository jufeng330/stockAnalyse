import pandas as pd
from sqlalchemy import create_engine
from stock_analyse.infrastructure.services.futu_market_data_provider import FutuMarketDataProvider
import logging

logging.basicConfig(level=logging.INFO)

engine = create_engine('mysql+pymysql://root:aloo.1234-qwer@192.168.1.12:3306/stock_info')

def load_futu_financial(market, table_name):
    print(f"Fetching {market} financial data...")
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
    for i in range(0, len(stock_codes), batch_size):
        batch_codes = stock_codes[i:i+batch_size]
        print(f"Fetching snapshot batch {i//batch_size + 1}/{(len(stock_codes)+batch_size-1)//batch_size}")
        
        retry = 0
        while retry < 3:
            try:
                # We need market_snapshot which includes financial data
                df = client.get_market_snapshot(batch_codes, skip_unsupported=True)
                if df is not None and not df.empty:
                    df = provider._map_snapshot_detail_frame(df, market, None)
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
            '今开': 'open_price',
            '最高': 'high_price',
            '最低': 'low_price',
            '昨收': 'prev_close',
            '成交量': 'volume',
            '成交额': 'turnover',
            '换手率': 'turnover_rate',
            '每手股数': 'lot_size',
            '振幅': 'amplitude',
            '均价': 'avg_price',
            '量比': 'volume_ratio',
            '52周最高': 'week52_high',
            '52周最低': 'week52_low',
            '历史最高': 'historical_high',
            '历史最低': 'historical_low',
            '已发行股份': 'issued_shares',
            '流通股本': 'circulating_shares',
            '总市值': 'total_market_value',
            '流通市值': 'circulating_market_value',
            '净资产': 'net_assets',
            '净利润': 'net_profit',
            '每股收益': 'eps',
            '每股净资产': 'naps',
            '收益率': 'yield_rate',
            '市盈率': 'pe',
            '市净率': 'pb',
            '市盈率-TTM': 'pe_ttm',
            '股息-TTM': 'dividend_ttm',
            '股息率-TTM': 'dividend_ratio_ttm',
            '上一财年股息': 'dividend_lfy',
            '上一财年股息率': 'dividend_lfy_ratio',
            
            # Optional specific to US market
            '盘前价格': 'pre_price',
            '盘前成交量': 'pre_volume',
            '盘前成交额': 'pre_turnover',
            '盘前涨跌额': 'pre_change_amount',
            '盘前涨跌幅': 'pre_change_rate',
            '盘后价格': 'post_price',
            '盘后成交量': 'post_volume',
            '盘后成交额': 'post_turnover',
            '盘后涨跌额': 'post_change_amount',
            '盘后涨跌幅': 'post_change_rate'
        }
        
        mapped_df = pd.DataFrame()
        for cn, en in col_mapping.items():
            if cn in full_df.columns:
                mapped_df[en] = pd.to_numeric(full_df[cn], errors='coerce') if en not in ['code', 'name'] else full_df[cn]
                
        if 'code' not in mapped_df.columns and '股票代码' in full_df.columns:
             mapped_df['code'] = full_df['股票代码']
             
        mapped_df.drop_duplicates(subset=['code'], inplace=True)
        
        with engine.connect() as con:
            con.execute(text(f"TRUNCATE TABLE {table_name}"))
            
        mapped_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"Saved {len(mapped_df)} rows to {table_name}")
    else:
        print(f"No {market} financial data retrieved.")
        
from sqlalchemy import text
load_futu_financial('H', 'financial_indicate_H')
load_futu_financial('usa', 'financial_indicate_usa')

