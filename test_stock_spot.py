import pandas as pd
from stock_analyse.infrastructure.services.futu_market_data_provider import FutuMarketDataProvider
from stock_analyse.infrastructure.services.market_data_service import stockBorderInfo

futu_sh = FutuMarketDataProvider('SH')
sh_df = futu_sh.get_stock_spot('SH')
print(f"SH Spot Data Rows: {len(sh_df) if sh_df is not None and not sh_df.empty else 0}")

futu_h = FutuMarketDataProvider('H')
h_df = futu_h.get_stock_spot('H')
print(f"H Spot Data Rows: {len(h_df) if h_df is not None and not h_df.empty else 0}")

futu_usa = FutuMarketDataProvider('usa')
usa_df = futu_usa.get_stock_spot('usa')
print(f"usa Spot Data Rows: {len(usa_df) if usa_df is not None and not usa_df.empty else 0}")

if sh_df is not None and not sh_df.empty:
    print("\nSH columns:", sh_df.columns.tolist())
