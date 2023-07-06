import datetime
import logging
import pandas as pd
import pyotp
import yaml
import pandas_ta as ta
import time
from api_helper import ShoonyaApiPy
import requests
# logging.basicConfig(level=logging.DEBUG)
# Set up logging
logging.basicConfig(filename='app.log', filemode='w',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S',
                    level=logging.DEBUG)

pd.set_option('display.max_columns', None)
# Set display option to show full rows
pd.set_option('display.max_colwidth', None)

# Get the current time
now = datetime.datetime.now()

# Store the current minute
current_minute = now.minute
# Load credentials from YAML file
with open('cred.yml') as f:
    cred = yaml.load(f, Loader=yaml.FullLoader)
symbols = cred.get('symbols', [])
quantity =cred.get('quantity')
exchange = str(cred.get('exchange'))
start_time_str = cred.get('start_time')  # fetch the start time string from the yaml file

# check if start_time_str is not None and is properly formatted
if not start_time_str or len(start_time_str.split(":")) != 3:
    raise ValueError("No properly formatted start_time provided in the YAML file.")

def get_time(time_string):
    data = time.strptime(time_string, '%d-%m-%Y %H:%M:%S')
    return time.mktime(data)

def buy_order(api, symbol, quantity,exchange):
    try:
        ret = api.place_order(buy_or_sell='B', product_type='M',
                              exchange=exchange, tradingsymbol=symbol,
                              discloseqty=0,
                              quantity=quantity, price_type='MKT',
                              retention='DAY', remarks='supertrend buy')
        # print(ret)
        logging.info(ret)

    except Exception as e:
        print(e)

def sell_order(api, symbol, quantity,exchange):
    try:
        ret = api.place_order(buy_or_sell='S', product_type='M',
                              exchange=exchange, tradingsymbol=symbol,
                              discloseqty=0,
                              quantity=quantity, price_type='MKT',
                              retention='DAY', remarks='supertrend sell')
        # print(ret)
        logging.info(ret)
    except Exception as e:
        print(e)

def stoploss_buy(api, symbol, quantity, price,exchange):
    try:
        ret = api.place_order(buy_or_sell='B', product_type='M',
                              exchange=exchange, tradingsymbol=symbol,
                              discloseqty=0,
                              quantity=quantity, price_type='SL-MKT',
                              price=price,
                              retention='DAY', remarks='stoploss buy')
        # print(ret)
        logging.info(ret)

    except Exception as e:
        print(e)


def get_data(api, symbol, token,exchange):
    try:

        now = datetime.datetime.now().strftime("%d-%m-%Y")

        # Combine the current date with the start time from the YAML file
        start_time = now + " " + start_time_str
        end_time = int(time.time())


        start_secs = get_time(start_time)

        ret = api.get_time_price_series(exchange=exchange, token=token,
                                        starttime=start_secs, endtime=end_time, interval=1)

        df = pd.DataFrame.from_dict(ret)
        df = df.drop(['stat', 'ssboe', 'intvwap', 'intoi', 'oi', 'v'], axis=1)
        df.rename(columns={
            'into': 'open',
            'inth': 'high',
            'intl': 'low',
            'intc': 'close',
            'intv': 'volume'
        }, inplace=True)
        df['high'] = pd.to_numeric(df['high'])
        df['low'] = pd.to_numeric(df['low'])
        df['close'] = pd.to_numeric(df['close'])
        df['open'] = pd.to_numeric(df['open'])
        df['volume'] = pd.to_numeric(df['volume'])

        # Reverse the DataFrame
        df = df[::-1].reset_index(drop=True)

        df.ta.supertrend(length=10, multiplier=3, column="close", append=True)
        return df
    except Exception as e:
        logging.error(f"Request timed out: {e}")
        print("network failed, retrying again.")

def findsymbol(api, symbol,exchange):
    ret = api.searchscrip(exchange=exchange, searchtext=symbol)
    if ret is not None:
        symbols = ret['values']
        for s in symbols:
            if s['tsym'] == symbol:
                return str(s['token'])

    return None


# Initialize a dictionary to track the last sell order price for each symbol
last_sell_price = {symbol: None for symbol in symbols}
# Initialize a dictionary to track the stop-loss buy order price for each symbol
stoploss_price = {symbol: None for symbol in symbols}

def check_signals(api, df, symbol, quantity,exchange):
    try:

        """Check the last row for sell signals, place a sell order if it's below the last executed sell price and place a stop-loss buy order"""
        last_row = df.iloc[-1]
        prev_row = df.iloc[-2]

        # If the last close price crossed the stop-loss buy order price
        if stoploss_price[symbol] is not None and last_row['close'] > stoploss_price[symbol]:
            # Reset the last sell price for the symbol
            last_sell_price[symbol] = None

        if last_row['SUPERTd_10_3.0'] < 0 and prev_row['SUPERTd_10_3.0'] >= 0:
            # If there's no previous sell order or the new sell price is below the last sell price
            if last_sell_price[symbol] is None or last_row['close'] < last_sell_price[symbol]:
                # Place sell order
                sell_order(api, symbol, quantity,exchange)
                print(f"[{datetime.datetime.now()}] Selling {symbol} at {last_row['close']} (Stop-Loss: {last_row['SUPERTs_10_3.0']})")

                # Place stoploss buy order
                stoploss_price[symbol] = last_row['SUPERTs_10_3.0']
                stoploss_buy(api, symbol, quantity, stoploss_price[symbol],exchange)
                print(f"[{datetime.datetime.now()}] Stoploss Buy Order for {symbol} placed at {stoploss_price[symbol]}")

                # Update the last sell price for the symbol
                last_sell_price[symbol] = last_row['close']
    except Exception as e :
        logging.error(f"Request timed out: {e}")
        print("network failed, retrying again.")


api = ShoonyaApiPy()

# Login and create a session
ret = api.login(userid=cred['user'], password=cred['pwd'], twoFA=pyotp.TOTP(cred['factor2']).now(),
                vendor_code=cred['vc'], api_secret=cred['apikey'], imei=cred['imei'])

session_token = ret['susertoken']
api.set_session(userid=cred['user'], password=cred['pwd'], usertoken=session_token)

# Find symbols and store their tokens
symbol_tokens = {}
for symbol in symbols:
    token = findsymbol(api, symbol,exchange)
    if token:
        symbol_tokens[symbol] = token

while True:
    # Get the current time
    now = datetime.datetime.now()
    # Check if the minute has changed
    if now.minute != current_minute:
        # Update the current minute
        current_minute = now.minute

        for symbol, token in symbol_tokens.items():
            # Fetch new data and calculate the Supertrend
            df = get_data(api, symbol, token,exchange)
            check_signals(api, df, symbol, quantity,exchange)
            df.to_csv(f'super_{symbol}.csv', index=False)
