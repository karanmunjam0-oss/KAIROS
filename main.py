bplist00—XUTI-Data—_public.utf8-plain-text_Y_name: Top-G 24-7 Scanner

on:
  workflow_dispatch:
  push:
    branches: [ main, master ]
  schedule:
    - cron: '*/30 * * * *'

permissions:
  actions: write
  contents: read

concurrency:
  group: topg-24-7-live-scanner
  cancel-in-progress: false

jobs:
  run-scanner:
    runs-on: ubuntu-latest
    timeout-minutes: 350
    steps:
      - name: Checkout Repository
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      - name: Install Required Libraries
        run: |
          pip install --upgrade pip
          pip install yfinance pandas requests
          pip install --no-cache-dir git+https://github.com/rongardF/tvdatafeed.git || true

      - name: Create and Run Top-G SMC Scanner (5.5 Hours Continuous)
        run: |
          cat << 'EOF' > scanner.py
          import sys
          import time
          import requests
          import pandas as pd
          import yfinance as yf

          try:
              from tvDatafeed import TvDatafeed, Interval
              tv = TvDatafeed()
              HAS_TV = True
          except Exception:
              tv = None
              HAS_TV = False

          START_TIME = time.time()
          # 5 Hours 30 Minutes (330 mins) lagataar chalega
          MAX_RUN_SECONDS = 330 * 60

          TELEGRAM_BOT_TOKEN = "8664477223:AAEmCFyAXEvO4G21VENtANNMA0XjP4BBHF4"
          TELEGRAM_CHAT_ID   = "5615073966"

          SYMBOLS = [
              ("XAUUSD (OANDA)", "XAUUSD", "OANDA", "GC=F"),
              ("XAUUSD (FOREX.com)", "XAUUSD", "FOREXCOM", "GC=F"),
              ("BTCUSD (BINANCE)", "BTCUSDT", "BINANCE", "BTC-USD"),
              ("EURUSD", "EURUSD", "OANDA", "EURUSD=X"),
              ("USDJPY", "USDJPY", "OANDA", "JPY=X"),
              ("GBPJPY", "GBPJPY", "OANDA", "GBPJPY=X"),
              ("GBPUSD", "GBPUSD", "OANDA", "GBPUSD=X"),
              ("USOIL", "WTICOUSD", "OANDA", "CL=F"),
              ("USDCAD", "USDCAD", "OANDA", "CAD=X")
          ]

          TIMEFRAMES = ["5m", "15m", "1h", "4h", "1D"]
          ZONE_BODY_PCT = 1.0
          MAX_BARS_LIMIT = 1500
          SCAN_INTERVAL_SECONDS = 30

          sent_alerts = set()

          def send_telegram_alert(message):
              url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
              payload = {
                  "chat_id": TELEGRAM_CHAT_ID,
                  "text": message,
                  "parse_mode": "HTML"
              }
              try:
                  r = requests.post(url, data=payload, timeout=10)
                  print(f"Telegram Status: {r.status_code}")
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

          def fetch_ohlc(sym, exch, yf_ticker, tf_name):
              global tv
              if HAS_TV and tv is not None:
                  try:
                      tv_map = {
                          "5m": Interval.in_5_minute,
                          "15m": Interval.in_15_minute,
                          "1h": Interval.in_1_hour,
                          "4h": Interval.in_4_hour,
                          "1D": Interval.in_daily
                      }
                      df = tv.get_hist(symbol=sym, exchange=exch, interval=tv_map[tf_name], n_bars=1000)
                      if df is not None and not df.empty:
                          df = df[["open", "high", "low", "close"]].dropna()
                          df.columns = ["Open", "High", "Low", "Close"]
                          return df
                  except Exception:
                      pass

              try:
                  if tf_name == "5m":
                      period, interval, resample_4h = "1mo", "5m", False
                  elif tf_name == "15m":
                      period, interval, resample_4h = "1mo", "15m", False
                  elif tf_name == "1h":
                      period, interval, resample_4h = "3mo", "1h", False
                  elif tf_name == "4h":
                      period, interval, resample_4h = "6mo", "1h", True
                  else:
                      period, interval, resample_4h = "2y", "1d", False

                  df = yf.download(yf_ticker, period=period, interval=interval, progress=False)
                  if df is None or df.empty:
                      return None
                  if isinstance(df.columns, pd.MultiIndex):
                      df.columns = df.columns.get_level_values(0)
                  df = df[["Open", "High", "Low", "Close"]].dropna()
                  if resample_4h:
                      df = df.resample("4h").agg({
                          "Open": "first",
                          "High": "max",
                          "Low": "min",
                          "Close": "last"
                      }).dropna()
                  return df
              except Exception as e:
                  print(f"Fetch Error [{sym} - {tf_name}]: {e}")
                  return None

          def run_smc_engine(pair_label, sym, exch, yf_ticker, tf_name):
              df = fetch_ohlc(sym, exch, yf_ticker, tf_name)
              if df is None or len(df) < 50:
                  return

              opens  = df["Open"].values
              highs  = df["High"].values
              lows   = df["Low"].values
              closes = df["Close"].values
              n      = len(df)
              last_bar_index = n - 1

              decimals = 4 if ("EUR" in sym or "GBP" in sym or "CAD" in sym) and "JPY" not in sym else (3 if "JPY" in sym else 2)

              trend = 1
              conf_high, conf_high_bar, conf_h_open, conf_h_close, conf_h_low = float(highs[0]), 0, float(opens[0]), float(closes[0]), float(lows[0])
              conf_low, conf_low_bar, conf_l_open, conf_l_close, conf_l_high  = float(lows[0]), 0, float(opens[0]), float(closes[0]), float(highs[0])

              qml_high, qml_h_bar, qml_h_open, qml_h_close, qml_h_low = None, None, None, None, None
              qml_low, qml_l_bar, qml_l_open, qml_l_close, qml_l_high = None, None, None, None, None

              ext_high, ext_h_bar, ext_h_open, ext_h_close, ext_h_low = float(highs[0]), 0, float(opens[0]), float(closes[0]), float(lows[0])
              ext_low, ext_l_bar, ext_l_open, ext_l_close, ext_l_high = float(lows[0]), 0, float(opens[0]), float(closes[0]), float(highs[0])

              has_ch = False
              pb_low, pb_l_bar, pb_l_open, pb_l_close, pb_l_high = float(lows[0]), 0, float(opens[0]), float(closes[0]), float(highs[0])

              has_cl = False
              pb_high, pb_h_bar, pb_h_open, pb_h_close, pb_h_low = float(highs[0]), 0, float(opens[0]), float(closes[0]), float(lows[0])

              has_int_step = False
              int_ch, int_ch_bar, int_ch_open, int_ch_close, int_ch_low = None, None, None, None, None
              int_cl, int_cl_bar, int_cl_open, int_cl_close, int_cl_high = None, None, None, None, None
              int_pb_high, int_pb_h_bar, int_pb_h_open, int_pb_h_close, int_pb_h_low = None, None, None, None, None
              int_pb_low, int_pb_l_bar, int_pb_l_open, int_pb_l_close, int_pb_l_high = None, None, None, None, None

              active_zones = []

              def create_zone(left_b, top_p, bot_p, txt, is_bull_zone, current_bar):
                  if left_b is not None and top_p is not None and bot_p is not None and top_p > bot_p:
                      if (last_bar_index - current_bar) <= MAX_BARS_LIMIT and (current_bar - left_b) <= MAX_BARS_LIMIT:
                          active_zones.append({
                              "left": int(left_b),
                              "top": float(top_p),
                              "bot": float(bot_p),
                              "name": str(txt),
                              "is_bull": bool(is_bull_zone),
                              "created_bar": int(current_bar)
                          })

              for i in range(1, n):
                  o, h, l, c     = float(opens[i]), float(highs[i]), float(lows[i]), float(closes[i])
                  o1, h1, l1, c1 = float(opens[i-1]), float(highs[i-1]), float(lows[i-1]), float(closes[i-1])

                  bull_retrace = (c1 < o1) and (c < o) and (c < l1)
                  bear_retrace = (c1 > o1) and (c > o) and (c > h1)

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
                                              trend_str = "BULLISH" if trend == 1 else "BEARISH"
                                              alert_msg = (
                                                  f"<b>TOP-G SMC LEVEL TAPPED</b>\n\n"
                                                  f"<b>Pair:</b> {pair_label}\n"
                                                  f"<b>Timeframe:</b> {tf_name}\n"
                                                  f"<b>Structure:</b> {trend_str}\n"
                                                  f"<b>Level:</b> {z['name']} ({side_str})\n"
                                                  f"<b>Zone:</b> {z['bot']:.{decimals}f} - {z['top']:.{decimals}f}\n"
                                                  f"<b>Tap Price:</b> {c:.{decimals}f}"
                                              )
                                              print(f"[TAP ALERT] {pair_label} ({tf_name}) -> {z['name']} at {c:.{decimals}f}")
                                              send_telegram_alert(alert_msg)
                                      active_zones.pop(idx)

                  if trend == 1:
                      if h >= ext_high:
                          ext_high, ext_h_bar, ext_h_open, ext_h_close, ext_h_low = h, i, o, c, l

                      if not has_ch and bull_retrace and ext_h_bar > conf_low_bar:
                          has_ch = True
                          has_int_step = False
                          conf_high, conf_high_bar, conf_h_open, conf_h_close, conf_h_low = ext_high, ext_h_bar, ext_h_open, ext_h_close, ext_h_low
                          if l <= l1:
                              pb_low, pb_l_bar, pb_l_open, pb_l_close, pb_l_high = l, i, o, c, h
                          else:
                              pb_low, pb_l_bar, pb_l_open, pb_l_close, pb_l_high = l1, i-1, o1, c1, h1
                          int_cl, int_cl_bar, int_cl_open, int_cl_close, int_cl_high = pb_low, pb_l_bar, pb_l_open, pb_l_close, pb_l_high
                          int_pb_high, int_pb_h_bar, int_pb_h_open, int_pb_h_close, int_pb_h_low = h, i, o, c, l

                      if has_ch:
                          if l < pb_low:
                              pb_low, pb_l_bar, pb_l_open, pb_l_close, pb_l_high = l, i, o, c, h
                          if h > int_pb_high and i > int_cl_bar:
                              int_pb_high, int_pb_h_bar, int_pb_h_open, int_pb_h_close, int_pb_h_low = h, i, o, c, l
                          if bull_retrace and int_pb_h_bar > int_cl_bar and int_pb_high < conf_high:
                              int_ch, int_ch_bar, int_ch_open, int_ch_close, int_ch_low = int_pb_high, int_pb_h_bar, int_pb_h_open, int_pb_h_close, int_pb_h_low
                              has_int_step = True
                          if has_int_step and int_cl is not None and c < int_cl and c > conf_low:
                              iss_ch_bot = calc_high_zone_bot(int_ch, int_ch_open, int_ch_close, int_ch_low)
                              create_zone(int_ch_bar, int_ch, iss_ch_bot, "ISS 4", False, i)
                              iss_cl_top = calc_low_zone_top(int_cl, int_cl_open, int_cl_close, int_cl_high)
                              create_zone(int_cl_bar, iss_cl_top, int_cl, "ISS 4", False, i)
                              int_cl, int_cl_bar, int_cl_open, int_cl_close, int_cl_high = l, i, o, c, h
                              int_pb_high, int_pb_h_bar, int_pb_h_open, int_pb_h_close, int_pb_h_low = h, i, o, c, l
                              has_int_step = False
                          elif not has_int_step and l < int_cl:
                              int_cl, int_cl_bar, int_cl_open, int_cl_close, int_cl_high = l, i, o, c, h
                              int_pb_high, int_pb_h_bar, int_pb_h_open, int_pb_h_close, int_pb_h_low = h, i, o, c, l

                      if has_ch and c > conf_high:
                          qml_high, qml_h_bar, qml_h_open, qml_h_close, qml_h_low = conf_high, conf_high_bar, conf_h_open, conf_h_close, conf_h_low
                          conf_low, conf_low_bar, conf_l_open, conf_l_close, conf_l_high = pb_low, pb_l_bar, pb_l_open, pb_l_close, pb_l_high
                          tjl1_bot = calc_high_zone_bot(conf_high, conf_h_open, conf_h_close, conf_h_low)
                          create_zone(conf_high_bar, conf_high, tjl1_bot, "BUY TJL1", True, i)
                          tjl2_top = calc_low_zone_top(conf_low, conf_l_open, conf_l_close, conf_l_high)
                          create_zone(conf_low_bar, tjl2_top, conf_low, "BUY TJL2", True, i)
                          has_ch = False
                          has_int_step = False
                          ext_high, ext_h_bar, ext_h_open, ext_h_close, ext_h_low = h, i, o, c, l

                      if c1 < conf_low and c < conf_low:
                          sbr_top = calc_low_zone_top(conf_low, conf_l_open, conf_l_close, conf_l_high)
                          create_zone(conf_low_bar, sbr_top, conf_low, "SBR", False, i)
                          if qml_h_bar is not None and qml_h_bar != ext_h_bar:
                              qml_bot = calc_high_zone_bot(qml_high, qml_h_open, qml_h_close, qml_h_low)
                              is_best_qml = (qml_high < ext_high) and (qml_high > conf_low)
                              create_zone(qml_h_bar, qml_high, qml_bot, "D-CHOCH SELL QML" if is_best_qml else "SELL QML", False, i)
                          if ext_h_bar is not None:
                              dt_bot = calc_high_zone_bot(ext_high, ext_h_open, ext_h_close, ext_h_low)
                              create_zone(ext_h_bar, ext_high, dt_bot, "DT", False, i)
                          trend = -1
                          conf_high, conf_high_bar, conf_h_open, conf_h_close, conf_h_low = ext_high, ext_h_bar, ext_h_open, ext_h_close, ext_h_low
                          has_cl = False
                          has_int_step = False
                          if l <= l1:
                              ext_low, ext_l_bar, ext_l_open, ext_l_close, ext_l_high = l, i, o, c, h
                          else:
                              ext_low, ext_l_bar, ext_l_open, ext_l_close, ext_l_high = l1, i-1, o1, c1, h1

                  elif trend == -1:
                      if l <= ext_low:
                          ext_low, ext_l_bar, ext_l_open, ext_l_close, ext_l_high = l, i, o, c, h

                      if not has_cl and bear_retrace and ext_l_bar > conf_high_bar:
                          has_cl = True
                          has_int_step = False
                          conf_low, conf_low_bar, conf_l_open, conf_l_close, conf_l_high = ext_low, ext_l_bar, ext_l_open, ext_l_close, ext_l_high
                          if h >= h1:
                              pb_high, pb_h_bar, pb_h_open, pb_h_close, pb_h_low = h, i, o, c, l
                          else:
                              pb_high, pb_h_bar, pb_h_open, pb_h_close, pb_h_low = h1, i-1, o1, c1, l1
                          int_ch, int_ch_bar, int_ch_open, int_ch_close, int_ch_low = pb_high, pb_h_bar, pb_h_open, pb_h_close, pb_h_low
                          int_pb_low, int_pb_l_bar, int_pb_l_open, int_pb_l_close, int_pb_l_high = l, i, o, c, h

                      if has_cl:
                          if h > pb_high:
                              pb_high, pb_h_bar, pb_h_open, pb_h_close, pb_h_low = h, i, o, c, l
                          if l < int_pb_low and i > int_ch_bar:
                              int_pb_low, int_pb_l_bar, int_pb_l_open, int_pb_l_close, int_pb_l_high = l, i, o, c, h
                          if bear_retrace and int_pb_l_bar > int_ch_bar and int_pb_low > conf_low:
                              int_cl, int_cl_bar, int_cl_open, int_cl_close, int_cl_high = int_pb_low, int_pb_l_bar, int_pb_l_open, int_pb_l_close, int_pb_l_high
                              has_int_step = True
                          if has_int_step and int_ch is not None and c > int_ch and c < conf_high:
                              iss_ch_bot = calc_high_zone_bot(int_ch, int_ch_open, int_ch_close, int_ch_low)
                              create_zone(int_ch_bar, int_ch, iss_ch_bot, "ISS 4", True, i)
                              iss_cl_top = calc_low_zone_top(int_cl, int_cl_open, int_cl_close, int_cl_high)
                              create_zone(int_cl_bar, iss_cl_top, int_cl, "ISS 4", True, i)
                              int_ch, int_ch_bar, int_ch_open, int_ch_close, int_ch_low = h, i, o, c, l
                              int_pb_low, int_pb_l_bar, int_pb_l_open, int_pb_l_close, int_pb_l_high = l, i, o, c, h
                              has_int_step = False
                          elif not has_int_step and h > int_ch:
                              int_ch, int_ch_bar, int_ch_open, int_ch_close, int_ch_low = h, i, o, c, l
                              int_pb_low, int_pb_l_bar, int_pb_l_open, int_pb_l_close, int_pb_l_high = l, i, o, c, h

                      if has_cl and c < conf_low:
                          qml_low, qml_l_bar, qml_l_open, qml_l_close, qml_l_high = conf_low, conf_low_bar, conf_l_open, conf_l_close, conf_l_high
                          conf_high, conf_high_bar, conf_h_open, conf_h_close, conf_h_low = pb_high, pb_h_bar, pb_h_open, pb_h_close, pb_h_low
                          tjl1_top = calc_low_zone_top(conf_low, conf_l_open, conf_l_close, conf_l_high)
                          create_zone(conf_low_bar, tjl1_top, conf_low, "SELL TJL1", False, i)
                          tjl2_bot = calc_high_zone_bot(conf_high, conf_h_open, conf_h_close, conf_h_low)
                          create_zone(conf_high_bar, conf_high, tjl2_bot, "SELL TJL2", False, i)
                          has_cl = False
                          has_int_step = False
                          ext_low, ext_l_bar, ext_l_open, ext_l_close, ext_l_high = l, i, o, c, h

                      if c1 > conf_high and c > conf_high:
                          rbs_bot = calc_high_zone_bot(conf_high, conf_h_open, conf_h_close, conf_h_low)
                          create_zone(conf_high_bar, conf_high, rbs_bot, "RBS", True, i)
                          if qml_l_bar is not None and qml_l_bar != ext_l_bar:
                              qml_top = calc_low_zone_top(qml_low, qml_l_open, qml_l_close, qml_l_high)
                              is_best_qml = (qml_low > ext_low) and (qml_low < conf_high)
                              create_zone(qml_l_bar, qml_top, qml_low, "D-CHOCH BUY QML" if is_best_qml else "BUY QML", True, i)
                          if ext_l_bar is not None:
                              db_top = calc_low_zone_top(ext_low, ext_l_open, ext_l_close, ext_l_high)
                              create_zone(ext_l_bar, db_top, ext_low, "DB", True, i)
                          trend = 1
                          conf_low, conf_low_bar, conf_l_open, conf_l_close, conf_l_high = ext_low, ext_l_bar, ext_l_open, ext_l_close, ext_l_high
                          has_ch = False
                          has_int_step = False
                          if h >= h1:
                              ext_high, ext_h_bar, ext_h_open, ext_h_close, ext_h_low = h, i, o, c, l
                          else:
                              ext_high, ext_h_bar, ext_h_open, ext_h_close, ext_h_low = h1, i-1, o1, c1, l1

          if __name__ == "__main__":
              print("Top-G SMC Scanner Started (5.5 Hours Non-Stop Cycle)...")
              send_telegram_alert(
                  "<b>Top-G SMC Scanner Active (24/7 Non-Stop Mode)</b>\n"
                  "<b>Pairs:</b> XAUUSD (OANDA & FOREX.com), BTCUSD (BINANCE), EURUSD, USDJPY, GBPJPY, GBPUSD, USOIL, USDCAD\n"
                  "<b>Timeframes:</b> 5m, 15m, 1h, 4h, 1D"
              )
              while True:
                  if time.time() - START_TIME > MAX_RUN_SECONDS:
                      print("5.5 Hours completed. Triggering instant restart...")
                      sys.exit(0)
                  for pair_label, sym, exch, yf_ticker in SYMBOLS:
                      for tf_name in TIMEFRAMES:
                          try:
                              run_smc_engine(pair_label, sym, exch, yf_ticker, tf_name)
                          except Exception as err:
                              print(f"Error [{pair_label} - {tf_name}]: {err}")
                          time.sleep(0.3)
                  time.sleep(SCAN_INTERVAL_SECONDS)
          EOF
          python scanner.py

      - name: Instant Auto-Restart Next Run (Zero Gap)
        if: always()
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          gh workflow run "Top-G 24-7 Scanner" --ref ${{ github.ref_name }} || true
     0                           Yì