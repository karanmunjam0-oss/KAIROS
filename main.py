bplist00ÑXUTI-DataÑ_public.utf8-plain-text_Èimport os
import time
import threading
import requests
import pandas as pd
from flask import Flask
from tvDatafeed import TvDatafeed, Interval

# Start time note kar lo (GitHub ke 6-hour limit se pehle safely restart karne ke liye)
start_time = time.time()
MAX_RUN_SECONDS = 5 * 3600  # 5 Ghante baad safe exit

TELEGRAM_BOT_TOKEN = "8664477223:AAEmCFyAXEvO4G21VENtANNMA0XjP4BBHF4"
TELEGRAM_CHAT_ID   = "5615073966"

SYMBOLS = [
    ("XAUUSD (OANDA)", "XAUUSD", "OANDA"),
    ("XAUUSD (FOREX.com)", "XAUUSD", "FOREXCOM"),
    ("BTCUSD (BINANCE)", "BTCUSDT", "BINANCE"),
    ("EURUSD", "EURUSD", "OANDA"),
    ("USDJPY", "USDJPY", "OANDA"),
    ("GBPJPY", "GBPJPY", "OANDA"),
    ("GBPUSD", "GBPUSD", "OANDA"),
    ("USOIL", "WTICOUSD", "OANDA"),
    ("USDCAD", "USDCAD", "OANDA")
]

TIMEFRAMES = {
    "5m": Interval.in_5_minute,
    "15m": Interval.in_15_minute,
    "1h": Interval.in_1_hour,
    "4h": Interval.in_4_hour,
    "1D": Interval.in_daily
}

ZONE_BODY_PCT = 1.0
MAX_BARS_LIMIT = 1500
SCAN_INTERVAL_SECONDS = 30

tv = TvDatafeed()
sent_alerts = set()

app = Flask(__name__)

@app.route("/")
def home():
    return "Top-G SMC Scanner is Live and Running 24/7!"

@app.route("/health")
def health():
    return "OK", 200

def send_telegram_alert(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        print(f"Telegram API Error: {e}")

def calc_high_zone_bot(c_high, c_open, c_close, c_low):
    body_top = max(c_open, c_close)
    body_bot = min(c_open, c_close)
    b_size   = body_top - body_bot
    z_bot    = body_top - (b_size * (ZONE_BODY_PCT * 0.01))
    if (c_high - z_bot) <= 1e-5:
        z_bot = c_high - ((c_high - c_low) * 0.15)
    return z_bot

def calc_low_zone_top(c_low, c_open, c_close, c_high):
    body_bot = min(c_open, c_close)
    body_top = max(c_open, c_close)
    b_size   = body_top - body_bot
    z_top    = body_bot + (b_size * (ZONE_BODY_PCT * 0.01))
    if (z_top - c_low) <= 1e-5:
        z_top = c_low + ((c_high - c_low) * 0.15)
    return z_top

def fetch_tv_ohlc(symbol, exchange, tv_interval):
    global tv
    try:
        df = tv.get_hist(symbol=symbol, exchange=exchange, interval=tv_interval, n_bars=1500)
        if df is None or df.empty:
            tv = TvDatafeed()
            df = tv.get_hist(symbol=symbol, exchange=exchange, interval=tv_interval, n_bars=1500)
        if df is None or df.empty:
            return None
        return df[["open", "high", "low", "close"]].dropna()
    except Exception as e:
        print(f"Data Fetch Error [{exchange}:{symbol}]: {e}")
        try:
            tv = TvDatafeed()
        except Exception:
            pass
        return None

def run_smc_engine(pair_label, symbol, exchange, tf_name, tv_interval):
    df = fetch_tv_ohlc(symbol, exchange, tv_interval)
    if df is None or len(df) < 50:
        return

    opens  = df["open"].values
    highs  = df["high"].values
    lows   = df["low"].values
    closes = df["close"].values
    n      = len(df)
    last_bar_index = n - 1

    decimals = 4 if ("EUR" in symbol or "GBP" in symbol or "CAD" in symbol) and "JPY" not in symbol else (3 if "JPY" in symbol else 2)

    trend = 1
    conf_high, conf_high_bar, conf_h_open, conf_h_close, conf_h_low = float(highs[0]), 0, float(opens[0]), float(closes[0]), float(lows[0])
    conf_low, conf_low_bar, conf_l_open, conf_l_close, conf_l_high  = float(lows[0]), 0, float(opens[0]), float(closes[0]), float(highs[0])

    qml_high, qml_h_bar, qml_h_open, qml_h_close, qml_h_low = None, None, None, None, None
    ext_high, ext_h_bar, ext_h_open, ext_h_close, ext_h_low = float(highs[0]), 0, float(opens[0]), float(closes[0]), float(lows[0])

    has_ch = False
    pb_low, pb_l_bar, pb_l_open, pb_l_close, pb_l_high = float(lows[0]), 0, float(opens[0]), float(closes[0]), float(highs[0])

    active_zones = []

    def create_zone(left_b, top_p, bot_p, txt, is_bull_zone, current_bar):
        if left_b is not None and top_p is not None and bot_p is not None and top_p > bot_p:
            if (last_bar_index - current_bar) <= MAX_BARS_LIMIT and (current_bar - left_b) <= MAX_BARS_LIMIT:
                active_zones.append({
                    "left": int(left_b), "top": float(top_p), "bot": float(bot_p),
                    "name": str(txt), "is_bull": bool(is_bull_zone), "created_bar": int(current_bar)
                })

    for i in range(1, n):
        o, h, l, c     = float(opens[i]), float(highs[i]), float(lows[i]), float(closes[i])
        o1, h1, l1, c1 = float(opens[i-1]), float(highs[i-1]), float(lows[i-1]), float(closes[i-1])

        bull_retrace = (c1 < o1) and (c < o) and (c < l1)

        if len(active_zones) > 0:
            for idx in range(len(active_zones) - 1, -1, -1):
                z = active_zones[idx]
                if i > z["created_bar"]:
                    if (i - z["left"]) > MAX_BARS_LIMIT:
                        active_zones.pop(idx)
                    else:
                        tapped = (l <= z["top"] and h >= z["bot"]) or (z["is_bull"] and l <= z["top"]) or (not z["is_bull"] and h >= z["bot"])
                        if tapped:
                            if i >= last_bar_index - 1:
                                alert_key = f"{pair_label}_{tf_name}_{z['name']}_{z['left']}_{round(z['top'], decimals)}"
                                if alert_key not in sent_alerts:
                                    sent_alerts.add(alert_key)
                                    side_str = "BUY ZONE" if z["is_bull"] else "SELL ZONE"
                                    alert_msg = (
                                        f"<b>TOP-G SMC LEVEL TAPPED</b>\n\n"
                                        f"<b>Pair:</b> {pair_label}\n"
                                        f"<b>Timeframe:</b> {tf_name}\n"
                                        f"<b>Level:</b> {z['name']} ({side_str})\n"
                                        f"<b>Zone:</b> {z['bot']:.{decimals}f} - {z['top']:.{decimals}f}\n"
                                        f"<b>Tap Price:</b> {c:.{decimals}f}"
                                    )
                                    send_telegram_alert(alert_msg)
                            active_zones.pop(idx)

        if trend == 1:
            if h >= ext_high:
                ext_high = h
            if not has_ch and bull_retrace:
                has_ch = True
                conf_high = ext_high
                pb_low = l if l <= l1 else l1
            if has_ch and l < pb_low:
                pb_low = l
            if has_ch and c > conf_high:
                conf_low = pb_low
                tjl1_bot = calc_high_zone_bot(conf_high, o1, c1, l1)
                create_zone(i-1, conf_high, tjl1_bot, "BUY TJL1", True, i)
                has_ch = False
                ext_high = h
            if c1 < conf_low and c < conf_low:
                trend = -1
                ext_low = l
        else:
            if l <= ext_low:
                ext_low = l
            if c1 > conf_high and c > conf_high:
                trend = 1
                ext_high = h

def scanner_loop():
    send_telegram_alert("<b>Top-G SMC Live Scanner Active (24/7 on GitHub Actions)</b>")
    while True:
        # Check timer: agar 5 ghante poore ho gaye hain toh safe exit lo taaki 'failed' error na aaye
        if time.time() - start_time > MAX_RUN_SECONDS:
            print("Max run time reached. Exiting cleanly.")
            os._exit(0)

        for pair_label, sym, exch in SYMBOLS:
            for tf_name, tv_int in TIMEFRAMES.items():
                try:
                    run_smc_engine(pair_label, sym, exch, tf_name, tv_int)
                except Exception as err:
                    pass
                time.sleep(0.5)
        time.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    threading.Thread(target=scanner_loop, daemon=True).start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
     0                           ü