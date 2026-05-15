from stock_analyse.infrastructure.data_sources.futu.futu_quote_client import FutuQuoteClient
from futu import Market, Plate

client = FutuQuoteClient()
quote_ctx, ret_ok = client._open_quote_context()

try:
    ret, data = quote_ctx.get_plate_stock('US.LIST2003')
    print("Return code:", ret)
    print("Data type:", type(data))
    if not data.empty:
        print("Data columns:", data.columns.tolist())
        print(data.head())
finally:
    quote_ctx.close()
