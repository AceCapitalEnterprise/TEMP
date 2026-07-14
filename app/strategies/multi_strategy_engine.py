# -*- coding: utf-8 -*-
"""
================================================================================
MULTI-STRATEGY COMBINATION BACKTEST  --  every C(6,3)=20 triples
================================================================================
Author : Rahul's Trading Desk - Ace Capital Enterprise
Date   : 2026

WHAT THIS DOES
--------------
Takes the SIX strategy backtests you have:

    1. MB  : Multibagger v5            (daily entry + 15-min exit)
    2. BO  : Breakout v8               (daily entry + 15-min exit)
    3. SQ  : Institutional Squeeze 4.3 (daily entry + 15-min exit)
    4. WF  : Wyckoff Flag              (daily entry + 15-min exit)
    5. RD  : RSI Dual-Stage v6.11      (daily entry + 15-min exit)
    6. NM  : NSE Momentum-Trend v5     (WEEKLY rebalance, daily-only)

...and runs EVERY possible combination of THREE strategies (20 in total) as a
single COMBINED PORTFOLIO of 20 stocks (slots shared across the 3 strategies).

WHAT IS PRESERVED EXACTLY (per your instruction: "logic and concept should not
change strictly")
-----------------------------------------------------------------------------
For each strategy the following are lifted verbatim from your original files:
  * indicator computation
  * entry/setup conditions, scoring, ranking
  * the entry FILL price convention (close / buy-stop / next-open / etc.)
  * regime filters, blackout, gray-zone, blocked-months, ready-date gates
  * the EXACT exit rules (stops, trails, SMA breaches, ATR/BE/EMA, time-stop,
    zombie, regime-aware ATR trail, weekly trend-break/overbought/bear)
  * exit FILL price convention of each strategy
  * scale-out (Squeeze 40%) and overbought-half (NSE 50%) partial exits

WHAT NECESSARILY CHANGES (and WHY) -- the only deviations, all at the PORTFOLIO
layer, never inside a strategy's signal/exit logic:
-----------------------------------------------------------------------------
  A) POSITION SIZING is standardised to EQUAL WEIGHT = (current equity / 20)
     per slot.  Three strategies cannot each size against the full capital in
     one shared 20-slot book, so each original's bespoke sizing formula
     (risk-%, ATR-vol-target, 5%/10% notional, DD-throttle, top-quartile boost)
     is replaced by one common equal-weight rule.  Every other part of each
     strategy is untouched.
  B) SLOT BUDGET: the 20 slots are split across the 3 strategies (default as
     even as possible, e.g. 7/7/6).  This rescales each strategy's max-position
     count (e.g. MB's top-20 -> its budget) but not its ranking/gating logic.
  C) ONE SYMBOL is held by AT MOST ONE strategy at a time across the whole
     book (no double exposure).  First strategy (in combo order) to claim a
     symbol on a given day wins it.
  D) RD scale-ins (Stage-2-on-Stage-1 +2% add) are a *sizing* feature and are
     therefore OMITTED under equal-weight slots; RD's Stage-1/Stage-2 ENTRY and
     its regime-aware ATR-trail EXIT are fully preserved.
  E) NM expected a pre-computed SMA_66 column in its CSV; here SMA_66 is
     computed as a 66-period SMA of daily close (the obvious reconstruction).
     NM keeps its WEEKLY rebalance cadence and weekly exit checks.

DATA CONTRACT (unchanged from your files)
-----------------------------------------
  INTRADAY_ZIP : zip of 15-min OHLCV for Nifty 500 stocks
                 cols (case-insensitive): symbol,date,time|datetime,o,h,l,c,vol
  NIFTY_CSV    : daily Nifty index (Date, Close, ...). Optional -> synthetic.

  Daily bars for ALL strategies are RESAMPLED from the 15-min file
  (O=first,H=max,L=min,C=last,V=sum) exactly as your variants do.

NOTE ON DATA HORIZON
--------------------
The long-lookback indicators (SMA-200/198, 252-day rolling return, 400-day
universe history, etc.) need years of daily bars.  If "current_month" really is
a single month, those indicators stay NaN and the corresponding strategies emit
few/zero signals -- identical behaviour to your standalone files.  Point
INTRADAY_ZIP at a multi-year 15-min archive for meaningful results.

OUTPUTS  (under OUTPUT_DIR)
---------------------------
  combo_<NN>_<A>_<B>_<C>_trades.csv      per-combination trade log
  combo_<NN>_<A>_<B>_<C>_equity.csv      per-combination daily equity
  ALL_COMBINATIONS_SUMMARY.csv           ranked summary of all 20 combos

DEPENDENCIES:  pip install pandas numpy tabulate
================================================================================
"""

import os
import sys
import time
import zipfile
import warnings
import itertools
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set

import numpy as np
import pandas as pd

try:
    from tabulate import tabulate
except ImportError:
    def tabulate(rows, headers=(), tablefmt="simple", **kw):
        out = []
        if headers:
            out.append("  ".join(str(h) for h in headers))
        for r in rows:
            out.append("  ".join(str(x) for x in r))
        return "\n".join(out)

warnings.filterwarnings("ignore")

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


# =============================================================================
# GLOBAL CONFIG  --  EDIT THESE
# =============================================================================
INTRADAY_ZIP   = os.environ.get("INTRADAY_ZIP", "Nifty500_current_month.csv.zip")
NIFTY_CSV      = os.environ.get("NIFTY_CSV", "nifty_data.csv")
OUTPUT_DIR     = os.environ.get("OUTPUT_DIR", "combo_backtest_results")

TOTAL_CAPITAL  = float(os.environ.get("TOTAL_CAPITAL", 5_000_000))   # default Rs 30 lakh
TOTAL_SLOTS    = int(os.environ.get("TOTAL_SLOTS", 20))              # 20-stock book
START_DATE     = "2015-01-01"
END_DATE       = "2030-12-31"
BLACKLIST      = {"GPIL"}           # symbol blacklist (from MB original)

# Which strategies participate in the combinations. Any 3-subset is run.
# Keys MUST match the StrategyBase.code of each plugin below.
STRATEGY_CODES = ["MB", "BO", "SQ", "WF", "RD", "NM"]

SEP = "=" * 80


# =============================================================================
# SHARED DATA LAYER
# =============================================================================
def _canon(col: str) -> Optional[str]:
    lc = str(col).strip().lower()
    if lc in ("symbol", "ticker", "stock", "scrip", "instrument", "name"):
        return "symbol"
    if lc == "date":  return "date"
    if lc == "time":  return "time"
    if lc in ("datetime", "date_time", "timestamp", "ts"): return "datetime"
    if lc == "open":  return "open"
    if lc == "high":  return "high"
    if lc == "low":   return "low"
    if lc == "close": return "close"
    if lc in ("volume", "vol"): return "volume"
    return None


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Dedup-aware canonicalisation (keeps the most-populated duplicate)."""
    seen, keep = {}, []
    for col in df.columns:
        c = _canon(col)
        if c is None:
            keep.append((col, col)); continue
        if c in seen:
            prev = seen[c]
            try:
                if df[col].notna().sum() > df[prev].notna().sum():
                    keep = [(o, n) for o, n in keep if n != c]
                    keep.append((col, c)); seen[c] = col
            except Exception:
                pass
        else:
            seen[c] = col; keep.append((col, c))
    return pd.DataFrame({n: df[o].values for o, n in keep})


def load_intraday_zip(zip_path: str, blacklist: Set[str]) -> pd.DataFrame:
    """Read 15-min zip -> long DataFrame [symbol,datetime,open,high,low,close,volume]."""
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Zip not found: {zip_path}")
    print(f"  Reading 15-min archive: {zip_path}", flush=True)
    t0 = time.time()
    frames = []
    opener = (zipfile.ZipFile(zip_path) if zip_path.lower().endswith(".zip")
              else None)
    members = []
    if opener is not None:
        members = [n for n in opener.namelist()
                   if n.lower().endswith(".csv") and "__MACOSX" not in n]
        if not members:
            raise ValueError("No CSV inside the zip.")
    else:
        members = [zip_path]
    print(f"  CSV members: {len(members)}", flush=True)

    for i, member in enumerate(members, 1):
        if opener is not None:
            with opener.open(member) as f:
                df = pd.read_csv(f, low_memory=False)
        else:
            df = pd.read_csv(member, low_memory=False)
        df = _normalize_columns(df)
        if i == 1:
            print(f"  Canonical columns: {list(df.columns)}", flush=True)

        if "symbol" not in df.columns:
            base = os.path.splitext(os.path.basename(member))[0]
            df["symbol"] = base.upper()

        def _ser(name):
            o = df[name]
            return o.iloc[:, 0] if isinstance(o, pd.DataFrame) else o

        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(_ser("datetime"), errors="coerce")
        elif "date" in df.columns and "time" in df.columns:
            df["datetime"] = pd.to_datetime(
                _ser("date").astype(str) + " " + _ser("time").astype(str),
                errors="coerce")
        elif "date" in df.columns:
            df["datetime"] = pd.to_datetime(_ser("date"), errors="coerce")
        else:
            raise ValueError(f"No datetime info in {member}: {list(df.columns)}")

        need = ["symbol", "datetime", "open", "high", "low", "close"]
        miss = [c for c in need if c not in df.columns]
        if miss:
            raise ValueError(f"Missing {miss} in {member}: {list(df.columns)}")
        if "volume" not in df.columns:
            df["volume"] = 0.0
        if getattr(df["datetime"].dt, "tz", None) is not None:
            df["datetime"] = df["datetime"].dt.tz_localize(None)

        df = df.dropna(subset=["datetime", "open", "high", "low", "close"])
        df = df[["symbol", "datetime", "open", "high", "low", "close", "volume"]]
        if blacklist:
            df = df[~df["symbol"].isin(blacklist)]
        if not df.empty:
            frames.append(df)
        if i % 50 == 0 or i == len(members):
            print(f"    {i}/{len(members)} CSVs read", flush=True)
    if opener is not None:
        opener.close()
    if not frames:
        raise ValueError("No usable rows in archive.")

    full = pd.concat(frames, ignore_index=True)
    full = full.sort_values(["symbol", "datetime"]).reset_index(drop=True)
    print(f"  [OK] {len(full):,} rows | {full['symbol'].nunique()} symbols | "
          f"{full['datetime'].min()} -> {full['datetime'].max()} "
          f"({time.time()-t0:.0f}s)", flush=True)
    return full


@dataclass
class SymIntraday:
    """Per-symbol 15-min arrays + day slice index, used by exit scanners."""
    dt:    np.ndarray          # datetime64[ns]
    o:     np.ndarray
    h:     np.ndarray
    l:     np.ndarray
    c:     np.ndarray
    bdate: np.ndarray          # normalised datetime64 per bar
    day_slices: Dict[pd.Timestamp, Tuple[int, int]]   # date -> (lo, hi)
    day_order:  List[pd.Timestamp]                     # sorted dates


class DataContext:
    """
    Holds everything strategies share:
      intraday[sym] : SymIntraday (15-min arrays + day slices)
      daily[sym]    : daily OHLCV DataFrame (DatetimeIndex, lowercase cols)
      nifty         : daily Nifty close Series (index = normalised date)
      all_dates     : DatetimeIndex of daily bars in [START,END]
      date2i        : {date -> position in all_dates}
      close_grid[sym]: np.ndarray aligned to all_dates (ffilled close) for MTM
    """
    def __init__(self, intraday_long: pd.DataFrame, nifty: Optional[pd.Series],
                 start: str, end: str):
        t0 = time.time()
        self.sd = pd.Timestamp(start)
        self.ed = pd.Timestamp(end)

        # ---- daily resample per symbol --------------------------------------
        print("  Resampling 15-min -> daily ...", flush=True)
        il = intraday_long.copy()
        il["date"] = il["datetime"].dt.normalize()
        agg = (il.groupby(["symbol", "date"], sort=False)
                 .agg(open=("open", "first"), high=("high", "max"),
                      low=("low", "min"), close=("close", "last"),
                      volume=("volume", "sum")).reset_index())

        self.daily: Dict[str, pd.DataFrame] = {}
        for sym, g in agg.groupby("symbol", sort=False):
            d = g.set_index("date")[["open", "high", "low", "close", "volume"]]
            d = d.sort_index()
            d = d[(d.index >= self.sd) & (d.index <= self.ed)]
            if len(d) >= 2:
                self.daily[sym] = d

        # ---- master daily date axis ----------------------------------------
        all_d = sorted(set().union(*[set(d.index) for d in self.daily.values()]))
        self.all_dates = pd.DatetimeIndex(all_d)
        self.date2i = {d: i for i, d in enumerate(self.all_dates)}
        print(f"  Daily window: {self.all_dates[0].date()} -> "
              f"{self.all_dates[-1].date()} ({len(self.all_dates)} days, "
              f"{len(self.daily)} symbols)", flush=True)

        # ---- per-symbol 15-min arrays + day slices --------------------------
        print("  Indexing 15-min bars per symbol ...", flush=True)
        self.intraday: Dict[str, SymIntraday] = {}
        for sym, g in intraday_long.groupby("symbol", sort=False):
            g = g.sort_values("datetime")
            dt = g["datetime"].values
            bdate = g["datetime"].dt.normalize().values
            o = g["open"].values.astype(float)
            h = g["high"].values.astype(float)
            l = g["low"].values.astype(float)
            c = g["close"].values.astype(float)
            # build day slices
            slices, order = {}, []
            if len(bdate):
                start_idx = 0
                cur = bdate[0]
                for j in range(1, len(bdate)):
                    if bdate[j] != cur:
                        ts = pd.Timestamp(cur)
                        slices[ts] = (start_idx, j); order.append(ts)
                        start_idx = j; cur = bdate[j]
                ts = pd.Timestamp(cur)
                slices[ts] = (start_idx, len(bdate)); order.append(ts)
            self.intraday[sym] = SymIntraday(dt, o, h, l, c, bdate, slices, order)

        # ---- nifty ----------------------------------------------------------
        self.nifty = self._prep_nifty(nifty)

        # ---- MTM close grid -------------------------------------------------
        print("  Building MTM close grid ...", flush=True)
        n = len(self.all_dates)
        self.close_grid: Dict[str, np.ndarray] = {}
        for sym, d in self.daily.items():
            s = d["close"].reindex(self.all_dates).ffill()
            self.close_grid[sym] = s.values.astype(float)

        print(f"  DataContext ready ({time.time()-t0:.0f}s)\n", flush=True)

    def _prep_nifty(self, nifty: Optional[pd.Series]) -> pd.Series:
        if nifty is not None and len(nifty) > 0:
            s = nifty.copy()
            s.index = pd.to_datetime(s.index).normalize()
            s = s[~s.index.duplicated(keep="last")].sort_index()
            return s.astype(float)
        # synthetic equal-weight index from the universe
        print("  [INFO] Building synthetic equal-weight Nifty proxy", flush=True)
        frames = [d["close"] / d["close"].iloc[0] * 100
                  for d in self.daily.values() if len(d) > 0]
        s = pd.concat(frames, axis=1).mean(axis=1).sort_index()
        return s.reindex(self.all_dates, method="ffill").dropna().astype(float)

    # day slice helpers ------------------------------------------------------
    def days_after(self, sym: str, after_date: pd.Timestamp,
                   inclusive: bool = False) -> List[pd.Timestamp]:
        si = self.intraday.get(sym)
        if si is None:
            return []
        out = []
        for d in si.day_order:
            if d in self.date2i and (d >= after_date if inclusive else d > after_date):
                out.append(d)
        return out


def load_nifty_csv(path: str) -> Optional[pd.Series]:
    if not path or not os.path.exists(str(path)):
        print(f"  [WARN] Nifty CSV '{path}' not found -> synthetic proxy", flush=True)
        return None
    try:
        df = pd.read_csv(path)
        df.columns = [c.strip().title() for c in df.columns]
        dc = next(c for c in df.columns if "date" in c.lower())
        cc = next(c for c in df.columns if "close" in c.lower())
        # robust date parsing: handles ISO8601 (incl. tz like +05:30),
        # plain dates, and day-first fallbacks
        s_raw = df[dc].astype(str)
        dt = pd.to_datetime(s_raw, format="ISO8601", errors="coerce")
        if dt.isna().mean() > 0.5:
            dt = pd.to_datetime(s_raw, errors="coerce")             # infer per element
        if dt.isna().mean() > 0.5:
            dt = pd.to_datetime(s_raw, dayfirst=True, errors="coerce")
        # strip timezone but KEEP local wall-clock date (do not shift to UTC)
        if getattr(dt.dt, "tz", None) is not None:
            dt = dt.dt.tz_localize(None)
        df[dc] = dt
        df = df.dropna(subset=[dc])
        s = df.set_index(dc)[cc].astype(float).sort_index()
        print(f"  [OK] Nifty loaded: {s.index[0].date()} -> {s.index[-1].date()}",
              flush=True)
        return s
    except Exception as e:
        print(f"  [WARN] Nifty load failed: {e} -> synthetic proxy", flush=True)
        return None


# =============================================================================
# SHARED INDICATOR PRIMITIVES
# =============================================================================
def ewm_rsi(close: pd.Series, period: int) -> pd.Series:
    """EWM-based RSI (used by MB & BO originals)."""
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(span=period, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(span=period, adjust=False).mean()
    return 100 - (100 / (1 + gain / loss.replace(0, np.nan)))


def wilder_adx(high, low, close, period):
    """Wilder ADX on numpy arrays (Squeeze & RD originals, identical algo)."""
    high = np.asarray(high, float); low = np.asarray(low, float)
    close = np.asarray(close, float); n = len(high)
    tr = np.zeros(n); pdm = np.zeros(n); mdm = np.zeros(n)
    for i in range(1, n):
        tr[i] = max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1]))
        up = high[i]-high[i-1]; dn = low[i-1]-low[i]
        pdm[i] = up if (up > dn and up > 0) else 0.0
        mdm[i] = dn if (dn > up and dn > 0) else 0.0
    a = 1.0/period
    atr = np.zeros(n); ps = np.zeros(n); ms = np.zeros(n)
    if period < n:
        atr[period] = np.mean(tr[1:period+1])
        ps[period]  = np.mean(pdm[1:period+1])
        ms[period]  = np.mean(mdm[1:period+1])
    for i in range(period+1, n):
        atr[i] = atr[i-1]*(1-a)+tr[i]*a
        ps[i]  = ps[i-1]*(1-a)+pdm[i]*a
        ms[i]  = ms[i-1]*(1-a)+mdm[i]*a
    pdi = np.where(atr > 0, 100*ps/atr, 0.0)
    mdi = np.where(atr > 0, 100*ms/atr, 0.0)
    dx  = np.where((pdi+mdi) > 0, 100*np.abs(pdi-mdi)/(pdi+mdi), 0.0)
    adx = np.zeros(n); start = 2*period
    if start < n:
        adx[start] = np.mean(dx[period+1:start+1])
        for i in range(start+1, n):
            adx[i] = adx[i-1]*(1-a)+dx[i]*a
    return adx


def wilder_atr(high, low, close, period):
    high = np.asarray(high, float); low = np.asarray(low, float)
    close = np.asarray(close, float); n = len(high)
    tr = np.zeros(n)
    for i in range(1, n):
        tr[i] = max(high[i]-low[i], abs(high[i]-close[i-1]), abs(low[i]-close[i-1]))
    a = 1.0/period; atr = np.zeros(n)
    if period < n:
        atr[period] = np.mean(tr[1:period+1])
        for i in range(period+1, n):
            atr[i] = atr[i-1]*(1-a)+tr[i]*a
    return atr


# =============================================================================
# STRATEGY PLUGIN FRAMEWORK
# =============================================================================
@dataclass
class Candidate:
    symbol: str
    entry_dt: pd.Timestamp            # 15-min anchor (first bar of entry day) or daily
    entry_price: float                # strategy-specific fill price
    entry_date: pd.Timestamp          # normalised daily date of entry
    stop_price: float                 # recorded initial stop (for the log)
    rank_key: float                   # higher = better (strategy's own ranking)
    meta: dict = field(default_factory=dict)


@dataclass
class ExitPlan:
    exit_dt: Optional[pd.Timestamp]   # None => never exits within data (force-close)
    exit_px: float
    reason: str
    partial_dt: Optional[pd.Timestamp] = None
    partial_px: float = 0.0
    partial_frac: float = 0.0         # fraction of CURRENT shares sold at partial


def first_bar_dt(si: SymIntraday, date: pd.Timestamp) -> pd.Timestamp:
    sl = si.day_slices.get(pd.Timestamp(date))
    if sl is None:
        return pd.Timestamp(date)
    return pd.Timestamp(si.dt[sl[0]])


def walk_levels(si: SymIntraday, start_idx: int, date2i: dict, get_levels):
    """
    Generic downside 15-min walker.
      get_levels(date) -> list of (level, reason, min_bars)
      A level triggers when bars_walked > min_bars AND close <= level.
      If several trigger on one bar, the LARGEST level wins (closest to close).
    Returns (exit_dt, fill, reason, bars_walked) or (None,None,None,bars).
    """
    n = len(si.c)
    bars = 0
    cur_date = None
    cur = None
    for j in range(start_idx, n):
        bars += 1
        bd = pd.Timestamp(si.bdate[j])
        if bd not in date2i:
            continue
        if bd != cur_date:
            cur_date = bd
            cur = get_levels(bd)
        if not cur:
            continue
        close = si.c[j]
        hits = []
        for level, reason, mb in cur:
            if level is None or np.isnan(level) or level <= 0:
                continue
            if bars > mb and close <= level:
                hits.append((level, reason))
        if hits:
            hits.sort(key=lambda x: x[0], reverse=True)
            lv, rs = hits[0]
            return pd.Timestamp(si.dt[j]), float(lv), rs, bars
    return None, None, None, bars


def first_breach_above(si: SymIntraday, start_dt: pd.Timestamp, target: float):
    """First 15-min bar whose HIGH >= target (>= start_dt). Fill = target."""
    if target is None or np.isnan(target) or target <= 0:
        return None, None
    idx = int(np.searchsorted(si.dt, np.datetime64(pd.Timestamp(start_dt)), side="left"))
    for j in range(idx, len(si.h)):
        if si.h[j] >= target:
            return pd.Timestamp(si.dt[j]), float(target)
    return None, None


class StrategyBase:
    code = "??"
    name = "??"

    def __init__(self, ctx: DataContext):
        self.ctx = ctx
        self._exit_cache: Dict[Tuple[str, pd.Timestamp], ExitPlan] = {}

    # lifecycle -------------------------------------------------------------
    def prepare(self):
        raise NotImplementedError

    def reset_run_state(self):
        """Reset any per-RUN mutable state (e.g. NM blacklist). Default: none."""
        pass

    # portfolio hooks -------------------------------------------------------
    def candidates_for_date(self, date: pd.Timestamp,
                            held: Set[str]) -> List[Candidate]:
        return []

    def slot_cap_for_date(self, date: pd.Timestamp) -> Optional[int]:
        return None

    def on_close(self, symbol: str, reason: str):
        pass

    # exits (cached, deterministic) ----------------------------------------
    def _compute_exit(self, cand: Candidate) -> ExitPlan:
        raise NotImplementedError

    def get_exit(self, cand: Candidate) -> ExitPlan:
        key = (cand.symbol, cand.entry_date)
        ep = self._exit_cache.get(key)
        if ep is None:
            ep = self._compute_exit(cand)
            self._exit_cache[key] = ep
        return ep


# =============================================================================
# PLUGIN 1 :: MULTIBAGGER v5  (MB)
# =============================================================================
class MBConfig:
    multibagger_return_pct = 200.0; rolling_window_days = 252
    min_price = 10.0; min_avg_volume = 50_000
    sma_short = 50; sma_mid = 100; sma_long = 200; ema_short = 21
    rsi_period = 14; adx_period = 14; atr_period = 14; consol_days = 20
    rs_days = 63; volume_ma_days = 50; breakout_window_days = 260
    trail_phase1_sma = 50; trail_phase2_sma = 100; trail_switch_profit_pct = 50.0
    index_regime_sma = 200
    stop_loss_pct = 12.0; min_hold_days = 10
    min_entry_score = 72.0; min_entry_price = 15.0


class Multibagger(StrategyBase):
    code = "MB"
    name = "Multibagger v5"

    def __init__(self, ctx):
        super().__init__(ctx)
        self.cfg = MBConfig()
        self.ind: Dict[str, pd.DataFrame] = {}
        self.sma50: Dict[str, dict] = {}
        self.sma100: Dict[str, dict] = {}
        self.dclose: Dict[str, dict] = {}
        self.daily_scores: Dict[pd.Timestamp, Dict[str, float]] = {}

    # ---- indicators (verbatim from MB compute_all_indicators) -------------
    def _indicators(self, df, index_close):
        cfg = self.cfg
        df = df.copy()
        c = df["close"].astype(float)
        df["SMA_20"]  = c.rolling(20).mean()
        df["SMA_30"]  = c.rolling(30).mean()
        df["SMA_50"]  = c.rolling(cfg.sma_short).mean()
        df["SMA_100"] = c.rolling(cfg.sma_mid).mean()
        df["SMA_150"] = c.rolling(150).mean()
        df["SMA_200"] = c.rolling(cfg.sma_long).mean()
        df["EMA_21"]  = c.ewm(span=cfg.ema_short, adjust=False).mean()
        df["RSI"]             = ewm_rsi(c, cfg.rsi_period)
        df["Ret_1M"]          = c.pct_change(21) * 100
        df["Ret_3M"]          = c.pct_change(63) * 100
        df["Ret_6M"]          = c.pct_change(126) * 100
        df["Ret_12M_rolling"] = c.pct_change(cfg.rolling_window_days) * 100
        h = df["high"].astype(float); l = df["low"].astype(float)
        df["ADX"]     = wilder_adx(h.values, l.values, c.values, cfg.adx_period)
        df["ATR"]     = pd.concat([h-l, (h-c.shift()).abs(),
                                   (l-c.shift()).abs()], axis=1).max(axis=1)\
                          .rolling(cfg.atr_period).mean()
        df["ATR_Pct"] = df["ATR"] / c * 100
        df["Vol_20d"] = c.pct_change().rolling(20).std() * np.sqrt(252) * 100
        df["VolMA_50"]    = df["volume"].rolling(cfg.volume_ma_days).mean()
        df["Vol_Ratio"]   = df["volume"] / df["VolMA_50"]
        vol20             = df["volume"].rolling(20).mean()
        df["VolMA_Trend"] = vol20.pct_change(20) * 100
        rm = df["high"].shift(1).rolling(cfg.consol_days).max()
        rn = df["low"].shift(1).rolling(cfg.consol_days).min()
        df["Consol_Pct"] = (rm - rn) / rn * 100
        h52 = df["high"].rolling(cfg.breakout_window_days).max()
        l52 = df["low"].rolling(cfg.breakout_window_days).min()
        df["Pct_52W"]         = (c - l52) / (h52 - l52).replace(0, np.nan) * 100
        df["Near_52W_High"]   = (c >= h52.shift(1) * 0.95).astype(int)
        df["Is_52W_Breakout"] = (c >= h52.shift(1) * 1.005).astype(int)
        df["Above_SMA200"]        = (c > df["SMA_200"]).astype(int)
        df["Above_SMA50"]         = (c > df["SMA_50"]).astype(int)
        df["Price_vs_SMA200_Pct"] = (c / df["SMA_200"] - 1) * 100
        df["Price_vs_SMA50_Pct"]  = (c / df["SMA_50"]  - 1) * 100
        idx = index_close.reindex(df.index, method="ffill")
        df["RS_3M"] = (c.pct_change(63).values  - idx.pct_change(63).values)  * 100
        df["RS_6M"] = (c.pct_change(126).values - idx.pct_change(126).values) * 100
        df["SMA50_Slope"]  = df["SMA_50"].pct_change(10) * 100
        df["SMA200_Slope"] = df["SMA_200"].pct_change(20) * 100
        turnover = c * df["volume"]
        df["Turnover_MA20"]      = turnover.rolling(20).mean()
        df["Turnover_Growth_3M"] = (df["Turnover_MA20"] /
                                    df["Turnover_MA20"].shift(63) - 1) * 100
        return df

    def _score_row(self, row):
        WEIGHTS = {"momentum": 0.40, "trend": 0.30, "volume": 0.20,
                   "breakout": 0.05, "quality": 0.05}
        def safe(col, default=np.nan):
            v = row.get(col, default)
            return float(v) if pd.notna(v) else default
        mom = []
        for col, lo, span in (("RS_3M", -10, 40), ("RS_6M", -20, 60),
                              ("Turnover_Growth_3M", -20, 100), ("Ret_3M", -20, 60)):
            v = safe(col)
            if not np.isnan(v):
                mom.append(min(10, max(0, (v - lo) / span * 10)))
        mom_s = np.nanmean(mom) if mom else np.nan
        tr = []
        s50 = safe("SMA50_Slope")
        if not np.isnan(s50):
            tr.append(min(10, max(0, (s50 + 3) / 9 * 10)))
        s200 = safe("SMA200_Slope")
        if not np.isnan(s200):
            if s200 < -3.0: tr.append(0)
            elif s200 < -0.5: tr.append(min(7, max(0, (s200 + 3) / 2.5 * 7)))
            elif s200 <= 1.0: tr.append(10)
            elif s200 <= 3.0: tr.append(max(5, 10 - (s200 - 1.0) * 2.5))
            else: tr.append(3)
        adx = safe("ADX")
        if not np.isnan(adx): tr.append(min(10, adx / 50 * 10))
        tr_s = np.nanmean(tr) if tr else np.nan
        volw = []
        vt = safe("VolMA_Trend")
        if not np.isnan(vt): volw.append((min(10, max(0, (vt + 10) / 40 * 10)), 0.7))
        vr = safe("Vol_Ratio")
        if not np.isnan(vr): volw.append((min(10, vr / 3 * 10), 0.3))
        vol_s = (sum(s*w for s, w in volw) / sum(w for _, w in volw)) if volw else np.nan
        brk = []
        p52 = safe("Pct_52W")
        if not np.isnan(p52): brk.append(max(0, 10 - abs(p52 - 50) / 5))
        b52 = safe("Is_52W_Breakout")
        if not np.isnan(b52): brk.append(b52 * 8)
        brk_s = np.nanmean(brk) if brk else np.nan
        q = []
        consol = safe("Consol_Pct")
        if not np.isnan(consol):
            if consol < 5: q.append(2)
            elif consol <= 15: q.append((consol - 5) / 10 * 6 + 2)
            elif consol <= 30: q.append(10)
            elif consol <= 45: q.append(max(3, 10 - (consol - 30) / 15 * 7))
            else: q.append(1)
        rsi = safe("RSI")
        if not np.isnan(rsi):
            if rsi > 80: q.append(max(0, 10 - (rsi - 80) * 2))
            elif 45 <= rsi <= 70: q.append(10)
            elif rsi >= 30: q.append((rsi - 30) / 15 * 10)
            else: q.append(0)
        q_s = np.nanmean(q) if q else np.nan
        parts = [(WEIGHTS["momentum"], mom_s), (WEIGHTS["trend"], tr_s),
                 (WEIGHTS["volume"], vol_s), (WEIGHTS["breakout"], brk_s),
                 (WEIGHTS["quality"], q_s)]
        valid = [(w, s) for w, s in parts if not np.isnan(s)]
        if not valid:
            return np.nan
        return sum(w*s for w, s in valid) / sum(w for w, _ in valid) * 10

    def prepare(self):
        cfg = self.cfg
        idx_close = self.ctx.nifty
        kept = 0
        for sym, d in self.ctx.daily.items():
            if len(d) < cfg.sma_long + 20:
                continue
            avg_vol = d["volume"].iloc[-100:].mean()
            avg_prc = d["close"].iloc[-100:].mean()
            if avg_vol < cfg.min_avg_volume or avg_prc < cfg.min_price:
                continue
            try:
                ind = self._indicators(d, idx_close)
            except Exception:
                continue
            self.ind[sym] = ind
            self.sma50[sym]  = dict(zip(ind.index, ind["SMA_50"].values))
            self.sma100[sym] = dict(zip(ind.index, ind["SMA_100"].values))
            self.dclose[sym] = dict(zip(ind.index, ind["close"].values))
            kept += 1
        # daily scores
        for date in self.ctx.all_dates:
            scores = {}
            for sym, ind in self.ind.items():
                if date not in ind.index:
                    continue
                sc = self._score_row(ind.loc[date])
                if not np.isnan(sc):
                    scores[sym] = round(sc, 2)
            if scores:
                self.daily_scores[date] = scores
        print(f"    [MB] {kept} symbols scored", flush=True)

    def candidates_for_date(self, date, held):
        cfg = self.cfg
        scores = self.daily_scores.get(date)
        if not scores:
            return []
        ranked = sorted([(s, v) for s, v in scores.items()
                         if s not in held and v >= cfg.min_entry_score],
                        key=lambda x: -x[1])
        out = []
        for sym, sc in ranked:
            ep = self.dclose.get(sym, {}).get(date, np.nan)
            if pd.isna(ep) or ep <= 0 or ep < cfg.min_entry_price:
                continue
            si = self.ctx.intraday.get(sym)
            edt = first_bar_dt(si, date) if si else pd.Timestamp(date)
            out.append(Candidate(sym, edt, float(ep), pd.Timestamp(date),
                                  round(ep*(1-cfg.stop_loss_pct/100), 2), float(sc),
                                  {"score": sc}))
        return out

    def _compute_exit(self, cand):
        cfg = self.cfg
        sym = cand.symbol; ep = cand.entry_price; stop = cand.stop_price
        si = self.ctx.intraday.get(sym)
        if si is None:
            return ExitPlan(None, 0.0, "NO_DATA")
        d2i = self.ctx.date2i
        eidx = d2i.get(cand.entry_date)
        s50 = self.sma50.get(sym, {}); s100 = self.sma100.get(sym, {})
        for d in self.ctx.days_after(sym, cand.entry_date):
            di = d2i.get(d)
            days_held = (di - eidx) if (di is not None and eidx is not None) else 0
            sl = si.day_slices.get(d)
            if sl is None:
                continue
            sma50_t = s50.get(d, np.nan); sma100_t = s100.get(d, np.nan)
            for j in range(sl[0], sl[1]):
                bo = si.o[j]; bl = si.l[j]; bc = si.c[j]
                if bo < stop:
                    return ExitPlan(pd.Timestamp(si.dt[j]), bo, "STOP")
                if bl <= stop:
                    return ExitPlan(pd.Timestamp(si.dt[j]), stop, "STOP")
                if days_held >= cfg.min_hold_days:
                    profit = (bc / ep - 1) * 100
                    if profit >= cfg.trail_switch_profit_pct:
                        sv, tag = sma100_t, "TRAIL_SMA100"
                    else:
                        sv, tag = sma50_t, "TRAIL_SMA50"
                    if pd.notna(sv) and bc < float(sv):
                        return ExitPlan(pd.Timestamp(si.dt[j]), bc, tag)
        return ExitPlan(None, 0.0, "END_OF_PERIOD")


# =============================================================================
# PLUGIN 2 :: BREAKOUT v8  (BO)
# =============================================================================
class BOParams:
    CONSOL_DAYS = 5; CONSOL_PCT = 8.0; RSI_OVERBOUGHT = 75.0
    GAP_UP_MAX_PCT = 3.0; VOLUME_MULT = 2.0; VOLUME_AVG_DAYS = 20
    BREAKOUT_WEEKS = [52, 26, 13]; BREAKOUT_BUFFER_PCT = 0.5
    INDEX_SMA_FILTER = 44; GRAY_ROC_DAYS = 20; GRAY_ROC_THRESH = -5.0
    GRAY_MAX_POS = 5
    SL_FIXED = 9.0
    STOCK_SMA_FILTER = 198; EXIT_PERIOD = 198; N_CANDLES = 2
    ZOMBIE_DAYS = 20; ZOMBIE_MIN_PCT = 5.0
    RSI_PERIOD = 14; RS_LOOKBACK_DAYS = 63


class Breakout(StrategyBase):
    code = "BO"
    name = "Breakout v8"

    def __init__(self, ctx):
        super().__init__(ctx)
        self.p = BOParams()
        self.daily_cands: Dict[pd.Timestamp, pd.DataFrame] = {}
        self.sma_exit: Dict[str, dict] = {}
        self.entry_ok: Dict[pd.Timestamp, bool] = {}
        self.is_gray: Dict[pd.Timestamp, bool] = {}

    def _indicators(self, df, idx_close):
        p = self.p
        df = df.copy()
        df["SMA_exit"] = df["close"].rolling(p.EXIT_PERIOD).mean()
        df["SMA_198"]  = df["close"].rolling(198).mean()
        df["RSI"]   = ewm_rsi(df["close"], p.RSI_PERIOD)
        df["VolMA"] = df["volume"].rolling(p.VOLUME_AVG_DAYS).mean()
        rm = df["high"].shift(1).rolling(p.CONSOL_DAYS).max()
        rn = df["low"].shift(1).rolling(p.CONSOL_DAYS).min()
        df["Consol"] = (rm - rn) / rn * 100
        for w in p.BREAKOUT_WEEKS:
            df[f"High_{w}W"] = df["high"].shift(1).rolling(w*5).max()
        if idx_close is not None and len(idx_close) > 0:
            ix = idx_close.reindex(df.index, method="ffill")
            df["RS_Long"]  = (df["close"].pct_change(p.RS_LOOKBACK_DAYS).values -
                              ix.pct_change(p.RS_LOOKBACK_DAYS).values)
            df["RS_Short"] = (df["close"].pct_change(20).values -
                              ix.pct_change(20).values)
        else:
            df["RS_Long"] = df["RS_Short"] = np.nan
        df["TrendDist"]  = (df["close"] - df["SMA_198"]) / df["SMA_198"] * 100
        df["Gap_Up_Pct"] = ((df["open"] - df["close"].shift(1)) /
                            df["close"].shift(1) * 100).clip(lower=0)
        return df

    def prepare(self):
        p = self.p
        idx_close = self.ctx.nifty
        buf = 1 + p.BREAKOUT_BUFFER_PCT / 100
        per_date = {}
        kept = 0
        for sym, d in self.ctx.daily.items():
            if len(d) < max(p.STOCK_SMA_FILTER, p.EXIT_PERIOD) + 10:
                continue
            try:
                df = self._indicators(d, idx_close)
            except Exception:
                continue
            self.sma_exit[sym] = dict(zip(df.index, df["SMA_exit"].values))
            kept += 1
            dates = df.index
            close = df["close"].values.astype(float)
            vol = df["volume"].values.astype(float)
            vol_ma = df["VolMA"].values.astype(float)
            vr = np.where(vol_ma > 0, vol / vol_ma, np.nan)
            rsi = df["RSI"].values.astype(float)
            gap = df["Gap_Up_Pct"].values.astype(float)
            consol = df["Consol"].values.astype(float)
            sma198 = df["SMA_198"].values.astype(float)
            rs_long = df["RS_Long"].values.astype(float)
            rs_short = df["RS_Short"].values.astype(float)
            trd = df["TrendDist"].values.astype(float)
            above = close > sma198
            rsi_safe = rsi <= p.RSI_OVERBOUGHT
            gap_safe = gap <= p.GAP_UP_MAX_PCT
            rs_ok = np.where(np.isnan(rs_long), True, rs_long > 0)
            vol_ok = vr >= p.VOLUME_MULT
            consol_ok = consol <= p.CONSOL_PCT
            n = len(dates)
            bhit = np.zeros(n, dtype=bool); bw = np.full(n, np.nan)
            for w in p.BREAKOUT_WEEKS:
                col = f"High_{w}W"
                hw = df[col].values.astype(float)
                hit = (close >= hw * buf) & ~np.isnan(hw)
                new = hit & ~bhit
                bhit[new] = True; bw[new] = w
            fixed_ok = (bhit & above & rsi_safe & gap_safe & rs_ok & vol_ok
                        & consol_ok & ~np.isnan(consol))
            for i in np.where(fixed_ok)[0]:
                per_date.setdefault(dates[i], []).append(
                    {"sym": sym, "close": close[i], "vol_ratio": vr[i],
                     "rs_long": rs_long[i], "rs_short": rs_short[i],
                     "trd": trd[i], "bw": bw[i]})
        self.daily_cands = {d: pd.DataFrame(r) for d, r in per_date.items() if r}
        # regime
        sma = idx_close.rolling(p.INDEX_SMA_FILTER).mean()
        roc = idx_close.pct_change(p.GRAY_ROC_DAYS) * 100
        for d in self.ctx.all_dates:
            ic = idx_close.get(d, np.nan); sm = sma.get(d, np.nan); rc = roc.get(d, np.nan)
            bull = bool(not pd.isna(ic) and not pd.isna(sm) and ic > sm)
            self.entry_ok[d] = bull
            self.is_gray[d] = (bull and not pd.isna(rc) and rc < p.GRAY_ROC_THRESH)
        print(f"    [BO] {kept} symbols, {len(self.daily_cands)} signal days", flush=True)

    def candidates_for_date(self, date, held):
        p = self.p
        if not self.entry_ok.get(date, True):
            return []
        day_df = self.daily_cands.get(date)
        if day_df is None or day_df.empty:
            return []
        valid = day_df[~day_df["sym"].isin(held) & ~day_df["vol_ratio"].isna()].copy()
        if valid.empty:
            return []
        valid["score"] = (0.40*valid["rs_long"].rank(pct=True).fillna(0.5) +
                          0.30*valid["rs_short"].rank(pct=True).fillna(0.5) +
                          0.20*valid["trd"].rank(pct=True).fillna(0.5) +
                          0.10*valid["vol_ratio"].rank(pct=True).fillna(0.5))
        out = []
        for _, row in valid.sort_values("score", ascending=False).iterrows():
            ep = float(row["close"])
            if ep <= 0:
                continue
            sym = row["sym"]
            si = self.ctx.intraday.get(sym)
            edt = first_bar_dt(si, date) if si else pd.Timestamp(date)
            out.append(Candidate(sym, edt, ep, pd.Timestamp(date),
                                  round(ep*(1-p.SL_FIXED/100), 2),
                                  float(row["score"]), {"bw": row.get("bw")}))
        return out

    def slot_cap_for_date(self, date):
        return self.p.GRAY_MAX_POS if self.is_gray.get(date, False) else None

    def _compute_exit(self, cand):
        p = self.p
        sym = cand.symbol; ep = cand.entry_price; stop = cand.stop_price
        si = self.ctx.intraday.get(sym)
        if si is None:
            return ExitPlan(None, 0.0, "NO_DATA")
        d2i = self.ctx.date2i; eidx = d2i.get(cand.entry_date)
        se = self.sma_exit.get(sym, {})
        days_below = 0
        for d in self.ctx.days_after(sym, cand.entry_date):
            di = d2i.get(d)
            days_held = (di - eidx) if (di is not None and eidx is not None) else 0
            sl = si.day_slices.get(d)
            if sl is None:
                continue
            sma198 = se.get(d, np.nan)
            for j in range(sl[0], sl[1]):
                bo = si.o[j]; bl = si.l[j]; bc = si.c[j]
                if bo < stop:
                    return ExitPlan(pd.Timestamp(si.dt[j]), bo, "STOP_LOSS_GAP")
                if bl <= stop:
                    return ExitPlan(pd.Timestamp(si.dt[j]), stop, "STOP_LOSS")
                if not np.isnan(sma198):
                    days_below = days_below + 1 if bc < sma198 else 0
                if days_below >= p.N_CANDLES:
                    return ExitPlan(pd.Timestamp(si.dt[j]), bc,
                                    f"SMA{p.EXIT_PERIOD}_N{p.N_CANDLES}_EXIT")
                pnl_now = (bc - ep) / ep * 100
                if days_held >= p.ZOMBIE_DAYS and pnl_now < p.ZOMBIE_MIN_PCT:
                    return ExitPlan(pd.Timestamp(si.dt[j]), bc, "ZOMBIE_STOP")
        return ExitPlan(None, 0.0, "END_OF_PERIOD")

# =============================================================================
# PLUGIN 3 :: INSTITUTIONAL SQUEEZE 4.3  (SQ)
# =============================================================================
class SQParams:
    adx_len = 22; adx_thresh = 22; adx_rise_lvl = 18; max_adx_limit = 32
    ema_len = 40; atr_len = 14
    coil_bars = 15; coil_count = 5
    break_bars = 5; vol_mult = 2.25; rs_len = 30
    time_bars = 5; min_move_pct = 3.0
    atr_sl_mult = 4.0; atr_be_mult = 2.0; ema_trail_pct = 2.0
    profit_pct = 45.0; scale_out_pct = 40.0
    nifty_ema_len = 50


class Squeeze(StrategyBase):
    code = "SQ"
    name = "Institutional Squeeze 4.3"

    def __init__(self, ctx):
        super().__init__(ctx)
        self.p = SQParams()
        self.cand_by_date: Dict[pd.Timestamp, list] = {}
        self.atr_d:  Dict[str, dict] = {}
        self.ema_d:  Dict[str, dict] = {}
        self.high_d: Dict[str, dict] = {}
        self.close_d: Dict[str, dict] = {}

    def _signals(self, df, nh, ncl, ncl_lag):
        """Verbatim port of generate_signals (daily). Returns df with Buy/BuyPrice/etc."""
        p = self.p
        df = df.copy().reset_index()           # 'date' becomes a column
        df = df.rename(columns={"index": "date"})
        n = len(df)
        if n < max(p.adx_len*3, p.ema_len, p.atr_len, p.rs_len) + 10:
            df["Buy"] = False
            return df
        df["ADX"] = wilder_adx(df["high"].values, df["low"].values,
                               df["close"].values, p.adx_len)
        df["EMA"] = df["close"].ewm(span=p.ema_len, adjust=False).mean()
        df["ATR"] = wilder_atr(df["high"].values, df["low"].values,
                               df["close"].values, p.atr_len)
        df["PDL"] = df["low"].shift(1)
        df["MA20_vol"] = df["volume"].rolling(20).mean()
        # nifty merge by date
        dts = df["date"].values
        df["nifty_healthy"]   = [bool(nh.get(pd.Timestamp(d), False)) for d in dts]
        df["nifty_close"]     = [ncl.get(pd.Timestamp(d), np.nan) for d in dts]
        df["nifty_close_lag"] = [ncl_lag.get(pd.Timestamp(d), np.nan) for d in dts]
        df["adx_below"] = (df["ADX"] < p.adx_thresh).astype(int)
        df["coil_sum"]  = df["adx_below"].rolling(p.coil_bars, min_periods=1).sum()
        df["Coiling"]   = df["coil_sum"] >= p.coil_count
        df["Sweep"]     = (df["low"] < df["PDL"]) & (df["close"] > df["PDL"])
        df["ADX_prev"]  = df["ADX"].shift(1)
        df["AdxRising"] = (df["ADX"] > df["ADX_prev"]) & (df["ADX"] > p.adx_rise_lvl)
        df["SetupCandle"] = df["Coiling"] & df["Sweep"] & df["AdxRising"]
        df["SetupHigh"] = np.nan
        df.loc[df["SetupCandle"], "SetupHigh"] = df.loc[df["SetupCandle"], "high"]
        df["SetupHigh"] = df["SetupHigh"].ffill()
        df["setup_idx"] = np.nan
        df.loc[df["SetupCandle"], "setup_idx"] = df.index[df["SetupCandle"]]
        df["setup_idx"] = df["setup_idx"].ffill()
        df["BarsSinceSetup"] = df.index - df["setup_idx"]
        df["VolConfirm"] = df["volume"] > (df["MA20_vol"] * p.vol_mult)
        df["close_lag"] = df["close"].shift(p.rs_len)
        df["RS"] = np.where(
            (df["nifty_close_lag"] > 0) & (df["close_lag"] > 0),
            (df["close"]/df["close_lag"]) / (df["nifty_close"]/df["nifty_close_lag"]),
            0)
        df["close_prev"] = df["close"].shift(1)
        df["SetupHigh_prev"] = df["SetupHigh"].shift(1)
        df["CrossAbove"] = ((df["close"] > df["SetupHigh"]) &
                            (df["close_prev"] <= df["SetupHigh_prev"]))
        df["CrossAbove"] |= ((df["close"] > df["SetupHigh"]) &
                             df["SetupHigh_prev"].isna() & df["SetupHigh"].notna())
        df["Buy_Raw"] = (df["CrossAbove"] & (df["BarsSinceSetup"] < p.break_bars) &
                         df["VolConfirm"] & (df["RS"] > 1) &
                         (df["ADX"] < p.max_adx_limit) & df["nifty_healthy"])
        df["BuyPrice"] = np.where(df["Buy_Raw"],
                                  np.maximum(df["open"], df["SetupHigh"]), np.nan)
        df["EmaTrail"] = df["EMA"] * (1 - p.ema_trail_pct/100)
        df["Buy"] = df["Buy_Raw"]
        return df

    def prepare(self):
        p = self.p
        nifty = self.ctx.nifty
        ema50 = nifty.ewm(span=p.nifty_ema_len, adjust=False).mean()
        healthy = (nifty > ema50)
        ncl     = dict(zip(nifty.index, nifty.values))
        ncl_lag = dict(zip(nifty.index, nifty.shift(p.rs_len).values))
        nh      = dict(zip(healthy.index, healthy.values))
        kept = 0
        for sym, d in self.ctx.daily.items():
            try:
                sdf = self._signals(d, nh, ncl, ncl_lag)
            except Exception:
                continue
            if "Buy" not in sdf.columns:
                continue
            dd = sdf.set_index("date")
            self.atr_d[sym]   = dict(zip(dd.index, dd["ATR"].values))
            self.ema_d[sym]   = dict(zip(dd.index, dd["EmaTrail"].values))
            self.high_d[sym]  = dict(zip(dd.index, dd["high"].values))
            self.close_d[sym] = dict(zip(dd.index, dd["close"].values))
            buys = sdf[sdf["Buy"] == True]
            for _, r in buys.iterrows():
                bp = r["BuyPrice"]
                if pd.isna(bp) or bp <= 0:
                    continue
                self.cand_by_date.setdefault(pd.Timestamp(r["date"]), []).append({
                    "sym": sym, "bp": float(bp),
                    "rs": float(r["RS"]) if pd.notna(r["RS"]) else 0.0,
                    "atr": float(r["ATR"]) if pd.notna(r["ATR"]) else 0.0,
                })
            kept += 1
        print(f"    [SQ] {kept} symbols, {len(self.cand_by_date)} signal days", flush=True)

    def candidates_for_date(self, date, held):
        rows = self.cand_by_date.get(date)
        if not rows:
            return []
        rows = sorted([r for r in rows if r["sym"] not in held],
                      key=lambda x: -x["rs"])
        out = []
        for r in rows:
            sym = r["sym"]; bp = r["bp"]; atr = r["atr"]
            si = self.ctx.intraday.get(sym)
            edt = first_bar_dt(si, date) if si else pd.Timestamp(date)
            init_sl = bp - self.p.atr_sl_mult * atr if atr > 0 else bp
            out.append(Candidate(sym, edt, bp, pd.Timestamp(date),
                                  round(init_sl, 2), r["rs"],
                                  {"atr_at_entry": atr}))
        return out

    def _compute_exit(self, cand):
        p = self.p
        sym = cand.symbol; ent = cand.entry_price
        atr_e = cand.meta.get("atr_at_entry", 0.0)
        si = self.ctx.intraday.get(sym)
        if si is None:
            return ExitPlan(None, 0.0, "NO_DATA")
        atr_d = self.atr_d.get(sym, {}); ema_d = self.ema_d.get(sym, {})
        high_d = self.high_d.get(sym, {}); close_d = self.close_d.get(sym, {})

        def get_levels(bd):
            c_atr = atr_d.get(bd, np.nan)
            if np.isnan(c_atr) or c_atr <= 0:
                c_atr = atr_e
            c_high = high_d.get(bd, np.nan)
            if pd.notna(c_high) and c_high > (ent + p.atr_be_mult * c_atr):
                sl_level, sl_reason = ent, "BE_Stop"
            else:
                sl_level, sl_reason = ent - p.atr_sl_mult * c_atr, "SL_Hit"
            ema_t = ema_d.get(bd, np.nan)
            return [(sl_level, sl_reason, 0), (ema_t, "EMA_Trail", 0)]

        # stop/trail scan starts at the bar AFTER the entry bar
        start_idx = int(np.searchsorted(
            si.dt, np.datetime64(pd.Timestamp(cand.entry_dt) + pd.Timedelta(minutes=1)),
            side="left"))
        sx_dt, sx_px, sx_reason, _ = walk_levels(si, start_idx, self.ctx.date2i, get_levels)

        # time-stop: 5+ daily bars held & daily close < ent*(1+min_move)
        days = self.ctx.days_after(sym, cand.entry_date)   # ordered trading days after entry
        tx_dt = None; tx_px = None
        thresh = ent * (1 + p.min_move_pct/100)
        for k in range(p.time_bars - 1, len(days)):
            dk = days[k]
            ck = close_d.get(dk, np.nan)
            if pd.notna(ck) and ck < thresh:
                # fill at first 15-min bar of NEXT trading day (else last bar of dk)
                if k + 1 < len(days):
                    nd = days[k+1]; sl = si.day_slices.get(nd)
                    if sl is not None:
                        tx_dt = pd.Timestamp(si.dt[sl[0]]); tx_px = float(si.c[sl[0]])
                if tx_dt is None:
                    sl = si.day_slices.get(dk)
                    if sl is not None:
                        tx_dt = pd.Timestamp(si.dt[sl[1]-1]); tx_px = float(si.c[sl[1]-1])
                break

        # choose earliest of stop-scan and time-stop
        if sx_dt is not None and tx_dt is not None:
            if sx_dt <= tx_dt:
                fin_dt, fin_px, fin_rsn = sx_dt, sx_px, sx_reason
            else:
                fin_dt, fin_px, fin_rsn = tx_dt, tx_px, "Time_Stop"
        elif sx_dt is not None:
            fin_dt, fin_px, fin_rsn = sx_dt, sx_px, sx_reason
        elif tx_dt is not None:
            fin_dt, fin_px, fin_rsn = tx_dt, tx_px, "Time_Stop"
        else:
            fin_dt, fin_px, fin_rsn = None, 0.0, "END_OF_PERIOD"

        plan = ExitPlan(fin_dt, fin_px if fin_px else 0.0, fin_rsn)

        # scale-out partial (+45%): first 15-min HIGH >= ent*1.45, from entry bar
        so_dt, so_px = first_breach_above(si, cand.entry_dt, ent * (1 + p.profit_pct/100))
        if so_dt is not None and (fin_dt is None or so_dt <= fin_dt):
            plan.partial_dt = so_dt
            plan.partial_px = so_px
            plan.partial_frac = p.scale_out_pct / 100.0
        return plan


# =============================================================================
# PLUGIN 4 :: WYCKOFF FLAG  (WF)
# =============================================================================
class WFParams:
    MA_TYPE = "EMA"; EMA_FAST = 10; EMA_MID = 40; EMA_SLOW = 60
    POLE_BARS = 30; FLAG_BARS = 15; MIN_POLE_PCT = 5.0; MAX_FLAG_WIDTH = 10.0
    TREND_PERSIST_PCT = 0.70; SCORE_THRESHOLD = 3; MAX_SCORE = 5
    RS_LOOKBACK = 50
    SL_BUFFER_PCT = 2.0; EXIT_MA_PERIOD = 22; EXIT_BUFFER_PCT = 3.0
    REGIME_MA_PERIOD = 88
    MIN_TRAIL_BARS = 20            # 15-MIN bars
    RS_MIN_FILTER = True
    MAX_LOSS_PCT = 5.0
    GAP_THRESHOLD_PCT = 2.5


class WyckoffFlag(StrategyBase):
    code = "WF"
    name = "Wyckoff Flag"

    def __init__(self, ctx):
        super().__init__(ctx)
        self.p = WFParams()
        self.entries_by_date: Dict[pd.Timestamp, list] = {}
        self.trail_d: Dict[str, dict] = {}
        self.regime: Dict[pd.Timestamp, str] = {}
        self.blackout: Set[pd.Timestamp] = set()

    def _indicators(self, df, idx_close):
        p = self.p
        d = df.copy().reset_index().rename(columns={"index": "date"})
        C, H, L, V = d["close"], d["high"], d["low"], d["volume"]
        if p.MA_TYPE.upper() == "SMA":
            ema_f = C.rolling(p.EMA_FAST, min_periods=1).mean()
            ema_m = C.rolling(p.EMA_MID,  min_periods=1).mean()
            ema_s = C.rolling(p.EMA_SLOW, min_periods=1).mean()
        else:
            ema_f = C.ewm(span=p.EMA_FAST, adjust=False).mean()
            ema_m = C.ewm(span=p.EMA_MID,  adjust=False).mean()
            ema_s = C.ewm(span=p.EMA_SLOW, adjust=False).mean()
        uptrend = ((ema_f > ema_m) & (ema_m > ema_s)).astype(int)
        above_m = (C > ema_m).astype(int)
        trend_persist = (above_m.rolling(p.POLE_BARS).sum()
                         >= p.POLE_BARS * p.TREND_PERSIST_PCT).astype(int)
        pole_start = C.shift(p.FLAG_BARS + p.POLE_BARS)
        pole_end   = C.shift(p.FLAG_BARS)
        pole_gain  = (pole_end - pole_start) / pole_start.clip(lower=1e-9) * 100
        flag_high  = H.rolling(p.FLAG_BARS).max()
        flag_low   = L.rolling(p.FLAG_BARS).min()
        flag_width = (flag_high - flag_low) / flag_high.clip(lower=1e-9) * 100
        pole_vol = V.rolling(p.POLE_BARS).mean().shift(p.FLAG_BARS)
        flag_vol = V.rolling(p.FLAG_BARS).mean()
        vol_dry  = (flag_vol < pole_vol).astype(int)
        prior_range = (H.rolling(p.FLAG_BARS).max()
                       - L.rolling(p.FLAG_BARS).min()).shift(p.FLAG_BARS)
        range_tight = ((flag_high - flag_low) < prior_range).astype(int)
        score = (uptrend + (pole_gain >= p.MIN_POLE_PCT).astype(int)
                 + (flag_width < p.MAX_FLAG_WIDTH).astype(int)
                 + range_tight + vol_dry + trend_persist)
        pivot = flag_high.shift(1)
        cross_up = (C > pivot * 1.01) & (C.shift(1) <= pivot.shift(1) * 1.01)
        is_flag = ((score >= p.SCORE_THRESHOLD) & (score <= p.MAX_SCORE)
                   & cross_up & (pivot > 0))
        flag_stop = flag_low * (1 - p.SL_BUFFER_PCT/100)
        ema_stop  = ema_m    * (1 - p.SL_BUFFER_PCT/100)
        stop_loss = pd.concat([flag_stop, ema_stop], axis=1).max(axis=1)
        exit_ma    = C.rolling(p.EXIT_MA_PERIOD).mean()
        trail_exit = exit_ma * (1 - p.EXIT_BUFFER_PCT/100)
        if idx_close is not None and len(idx_close) > 0:
            idx_al = d["date"].map(lambda dt: idx_close.get(pd.Timestamp(dt), np.nan))
            idx_al = idx_al.ffill()
            ratio = C / idx_al.clip(lower=1e-9)
            rs_score = ratio.pct_change(p.RS_LOOKBACK) * 100
        else:
            rs_score = pd.Series(np.zeros(len(d)), index=d.index)
        d["Score"] = score; d["Signal"] = is_flag.astype(int)
        d["StopLoss"] = stop_loss; d["TrailExit"] = trail_exit
        d["RS_Score"] = rs_score
        return d

    def prepare(self):
        p = self.p
        idx_close = self.ctx.nifty
        # regime SMA88 + blackout (>2.5% nifty move)
        ma = idx_close.rolling(p.REGIME_MA_PERIOD).mean()
        for dt in self.ctx.all_dates:
            px = idx_close.get(dt, np.nan); mv = ma.get(dt, np.nan)
            self.regime[dt] = ("BULL" if (pd.isna(px) or pd.isna(mv))
                               else ("BULL" if px >= mv else "BEAR"))
        move = idx_close.pct_change().abs() * 100
        self.blackout = set(pd.Timestamp(d) for d in
                            move[move > p.GAP_THRESHOLD_PCT].index)
        warmup = p.POLE_BARS + p.FLAG_BARS + p.EXIT_MA_PERIOD + 10
        kept = 0
        for sym, d in self.ctx.daily.items():
            if len(d) < warmup:
                continue
            try:
                sdf = self._indicators(d, idx_close)
            except Exception:
                continue
            ts = sdf.set_index("date")["TrailExit"]
            self.trail_d[sym] = dict(zip(ts.index, ts.values))
            sig = sdf[sdf["Signal"] == 1]
            dates_arr = sdf["date"].values
            for _, row in sig.iterrows():
                sdate = pd.Timestamp(row["date"])
                # entry on NEXT daily bar's open
                future = dates_arr[dates_arr > np.datetime64(sdate)]
                if len(future) == 0:
                    continue
                ndate = pd.Timestamp(future[0])
                nrow = sdf[sdf["date"] == future[0]].iloc[0]
                ep = nrow["open"] if nrow["open"] > 0 else nrow["close"]
                self.entries_by_date.setdefault(ndate, []).append((
                    float(row["RS_Score"]) if pd.notna(row["RS_Score"]) else 0.0,
                    sym, float(ep), float(row["StopLoss"]),
                    float(row["TrailExit"]), int(row["Score"])))
            kept += 1
        print(f"    [WF] {kept} symbols, {len(self.entries_by_date)} entry days", flush=True)

    def candidates_for_date(self, date, held):
        p = self.p
        if self.regime.get(date, "BULL") == "BEAR":
            return []
        if date in self.blackout:
            return []
        lst = self.entries_by_date.get(date)
        if not lst:
            return []
        if p.RS_MIN_FILTER and len(lst) > 1:
            rs_median = float(np.median([s[0] for s in lst]))
        else:
            rs_median = -np.inf
        out = []
        for rs, sym, ep, tech_stop, trail_init, sc in sorted(lst, key=lambda x: -x[0]):
            if sym in held or ep <= 0:
                continue
            if p.RS_MIN_FILTER and rs < rs_median:
                continue
            eff_stop = max(tech_stop, ep * (1 - p.MAX_LOSS_PCT/100))
            si = self.ctx.intraday.get(sym)
            edt = first_bar_dt(si, date) if si else pd.Timestamp(date)
            out.append(Candidate(sym, edt, ep, pd.Timestamp(date),
                                  round(eff_stop, 2), rs,
                                  {"score": sc, "eff_stop": eff_stop}))
        return out

    def _compute_exit(self, cand):
        p = self.p
        sym = cand.symbol
        stop = cand.meta.get("eff_stop", cand.stop_price)
        si = self.ctx.intraday.get(sym)
        if si is None:
            return ExitPlan(None, 0.0, "NO_DATA")
        trail_d = self.trail_d.get(sym, {})

        def get_levels(bd):
            tr = trail_d.get(bd, np.nan)
            return [(stop, "Stop Loss", 0), (tr, "Trailing Exit", p.MIN_TRAIL_BARS)]

        sl = si.day_slices.get(pd.Timestamp(cand.entry_date))
        start_idx = sl[0] if sl is not None else int(np.searchsorted(
            si.dt, np.datetime64(pd.Timestamp(cand.entry_dt)), side="left"))
        ex_dt, ex_px, reason, _ = walk_levels(si, start_idx, self.ctx.date2i, get_levels)
        if ex_dt is None:
            return ExitPlan(None, 0.0, "End of Data")
        return ExitPlan(ex_dt, ex_px, reason)

# =============================================================================
# PLUGIN 5 :: RSI DUAL-STAGE RECOVERY v6.11  (RD)
#   NOTE: scale-ins OMITTED (sizing feature, deviation D). Entry & regime-aware
#   ATR-trail exit preserved verbatim.
# =============================================================================
class RDParams:
    score_thresh = 60; recovery_lvl = 55
    adx_len = 22; adx_rising_bars = 5; vol_mult = 1.3
    atr_mult_bull = 4.0; atr_mult_bear = 1.0; atr_len = 14
    regime_sma_period = 44


class RsiDual(StrategyBase):
    code = "RD"
    name = "RSI Dual-Stage v6.11"

    def __init__(self, ctx):
        super().__init__(ctx)
        self.p = RDParams()
        self.cand_by_date: Dict[pd.Timestamp, list] = {}
        self.high_d: Dict[str, dict] = {}
        self.atr_d:  Dict[str, dict] = {}
        self.regime: Dict[pd.Timestamp, str] = {}
        self.ready_date: Optional[pd.Timestamp] = None

    @staticmethod
    def _rsi_states(closes_arr, period):
        n = len(closes_arr)
        rsi = np.zeros(n); ag = np.zeros(n); al = np.zeros(n)
        if n < period + 1:
            return rsi, ag, al
        deltas = np.diff(closes_arr)
        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        avg_g = np.mean(gains[:period]); avg_l = np.mean(losses[:period])
        ag[period] = avg_g; al[period] = avg_l
        rsi[period] = 100.0 if avg_l == 0 else 100.0 - 100.0/(1.0 + avg_g/avg_l)
        for i in range(period, len(deltas)):
            avg_g = (avg_g*(period-1) + gains[i]) / period
            avg_l = (avg_l*(period-1) + losses[i]) / period
            ag[i+1] = avg_g; al[i+1] = avg_l
            rsi[i+1] = 100.0 if avg_l == 0 else 100.0 - 100.0/(1.0 + avg_g/avg_l)
        return rsi, ag, al

    def _signals(self, df):
        """Verbatim port of RD generate_signals (weekly-RSI reconstruction)."""
        p = self.p
        df = df.copy().reset_index().rename(columns={"index": "date"})
        n = len(df)
        min_bars = max(p.adx_len*3, 60, p.atr_len + 1)
        if n < min_bars:
            return None
        closes = df["close"].values
        wk = pd.Series(df["date"].values).dt.isocalendar()
        df["_yw"] = wk["year"].values * 100 + wk["week"].values
        week_groups = df.groupby("_yw")["close"].last()
        uniq = sorted(df["_yw"].unique())
        weekly_closes = np.array([week_groups[y] for y in uniq])
        w2i = {y: i for i, y in enumerate(uniq)}
        rsi14, ag14, al14 = self._rsi_states(weekly_closes, 14)
        rsi3,  ag3,  al3  = self._rsi_states(weekly_closes, 3)
        d_wrsi14 = np.zeros(n); d_wscore = np.zeros(n)
        for i in range(n):
            yw = df["_yw"].iloc[i]; widx = w2i[yw]
            if widx == 0:
                d_wrsi14[i] = rsi14[widx] if widx < len(rsi14) else 0
                d_wscore[i] = (rsi14[widx] + rsi3[widx]) if widx < len(rsi3) else 0
                continue
            today_close = closes[i]; prev_wc = weekly_closes[widx-1]
            delta = today_close - prev_wc
            gain = max(delta, 0); loss = max(-delta, 0)
            if widx-1 >= 14 and widx < len(ag14):
                new_ag = (ag14[widx-1]*13 + gain) / 14
                new_al = (al14[widx-1]*13 + loss) / 14
                d_wrsi14[i] = 100.0 if new_al == 0 else 100.0 - 100.0/(1.0 + new_ag/new_al)
            else:
                d_wrsi14[i] = rsi14[widx] if widx < len(rsi14) else 0
            if widx-1 >= 3 and widx < len(ag3):
                new_ag3 = (ag3[widx-1]*2 + gain) / 3
                new_al3 = (al3[widx-1]*2 + loss) / 3
                rsi3_val = 100.0 if new_al3 == 0 else 100.0 - 100.0/(1.0 + new_ag3/new_al3)
            else:
                rsi3_val = rsi3[widx] if widx < len(rsi3) else 0
            d_wscore[i] = d_wrsi14[i] + rsi3_val
        df["WeeklyRSI14"] = d_wrsi14
        df["CompositeScore"] = d_wscore
        df.drop(columns=["_yw"], inplace=True)
        df["ADX"] = wilder_adx(df["high"].values, df["low"].values,
                               df["close"].values, p.adx_len)
        adx_prev = np.roll(df["ADX"].values, 1); adx_prev[0] = 0
        adx_rising = (df["ADX"].values > adx_prev).astype(int)
        adx_sum = pd.Series(adx_rising).rolling(p.adx_rising_bars,
                  min_periods=p.adx_rising_bars).sum().fillna(0).values
        df["AdxStrength"] = adx_sum == p.adx_rising_bars
        df["MA20_vol"] = df["volume"].rolling(20).mean()
        df["VolConfirm"] = df["volume"] > (df["MA20_vol"] * p.vol_mult)
        df["ATR"] = wilder_atr(df["high"].values, df["low"].values,
                               df["close"].values, p.atr_len)
        df["prev_high"] = df["high"].shift(1)
        df["Stage1"] = ((df["CompositeScore"] > p.score_thresh) &
                        (df["close"] > df["prev_high"]) & df["VolConfirm"])
        df["Stage2"] = ((df["WeeklyRSI14"] >= p.recovery_lvl) &
                        df["AdxStrength"] & df["VolConfirm"])
        df["Buy"] = df["Stage1"] | df["Stage2"]
        df["BuyBasis"] = np.where(df["Stage1"], 1, np.where(df["Stage2"], 2, 0))
        return df

    def prepare(self):
        p = self.p
        nifty = self.ctx.nifty
        sma = nifty.rolling(p.regime_sma_period).mean()
        for dt in nifty.index:
            mv = sma.get(dt, np.nan)
            if pd.isna(mv):
                continue
            if self.ready_date is None:
                self.ready_date = dt
            self.regime[dt] = "BULL" if nifty.get(dt) >= mv else "BEAR"
        if self.ready_date is None:
            self.ready_date = self.ctx.all_dates[0]
        kept = 0
        for sym, d in self.ctx.daily.items():
            try:
                sdf = self._signals(d)
            except Exception:
                sdf = None
            if sdf is None:
                continue
            dd = sdf.set_index("date")
            self.high_d[sym] = dict(zip(dd.index, dd["high"].values))
            self.atr_d[sym]  = dict(zip(dd.index, dd["ATR"].values))
            buys = sdf[sdf["Buy"] == True]
            for _, r in buys.iterrows():
                self.cand_by_date.setdefault(pd.Timestamp(r["date"]), []).append({
                    "sym": sym,
                    "close": float(r["close"]),
                    "score": float(r["CompositeScore"]),
                    "basis": int(r["BuyBasis"]),
                })
            kept += 1
        print(f"    [RD] {kept} symbols, {len(self.cand_by_date)} signal days "
              f"(ready {self.ready_date.date() if self.ready_date is not None else 'NA'})",
              flush=True)

    def _reg(self, date):
        return self.regime.get(pd.Timestamp(date), "BULL")

    def candidates_for_date(self, date, held):
        if self.ready_date is not None and date < self.ready_date:
            return []
        if self._reg(date) == "BEAR":      # new entries BULL-only
            return []
        rows = self.cand_by_date.get(date)
        if not rows:
            return []
        rows = sorted([r for r in rows if r["sym"] not in held],
                      key=lambda x: -x["score"])
        out = []
        for r in rows:
            sym = r["sym"]; ep = r["close"]
            if ep <= 0:
                continue
            si = self.ctx.intraday.get(sym)
            edt = first_bar_dt(si, date) if si else pd.Timestamp(date)
            out.append(Candidate(sym, edt, ep, pd.Timestamp(date),
                                  0.0, r["score"],
                                  {"stage": r["basis"] if r["basis"] in (1, 2) else 1}))
        return out

    def _compute_exit(self, cand):
        p = self.p
        sym = cand.symbol
        si = self.ctx.intraday.get(sym)
        if si is None:
            return ExitPlan(None, 0.0, "NO_DATA")
        high_d = self.high_d.get(sym, {}); atr_d = self.atr_d.get(sym, {})
        # build path-dependent trail level per holding day (incl. entry day)
        trail_by_date = {}
        running_high = high_d.get(cand.entry_date, np.nan)
        for d in self.ctx.days_after(sym, cand.entry_date, inclusive=True):
            hv = high_d.get(d, np.nan)
            if pd.notna(hv):
                running_high = hv if (np.isnan(running_high)) else max(running_high, hv)
            reg = self._reg(d)
            mult = p.atr_mult_bear if reg == "BEAR" else p.atr_mult_bull
            c_atr = atr_d.get(d, np.nan)
            if np.isnan(c_atr) or c_atr <= 0 or np.isnan(running_high):
                trail_by_date[d] = (0.0, "ATR_Trail_Bull")
            else:
                trail_by_date[d] = (running_high - c_atr * mult,
                                    "ATR_Trail_Bear" if reg == "BEAR" else "ATR_Trail_Bull")

        def get_levels(bd):
            t = trail_by_date.get(bd)
            if t is None or t[0] <= 0:
                return []
            return [(t[0], t[1], 0)]

        start_idx = int(np.searchsorted(
            si.dt, np.datetime64(pd.Timestamp(cand.entry_dt) + pd.Timedelta(minutes=1)),
            side="left"))
        ex_dt, ex_px, reason, _ = walk_levels(si, start_idx, self.ctx.date2i, get_levels)
        if ex_dt is None:
            return ExitPlan(None, 0.0, "End_Of_Data")
        return ExitPlan(ex_dt, ex_px, reason)


# =============================================================================
# PLUGIN 6 :: NSE MOMENTUM-TREND v5  (NM)  --  WEEKLY rebalance, daily-only
# =============================================================================
class NMParams:
    STOP_LOSS_PCT = 0.10; TRAILING_STOP_PCT = 0.10; TRAILING_TRIGGER = 0.15
    MIN_VOLUME = 200_000; MIN_HISTORY_DAYS = 400; BACKTEST_START_YEAR = 2016
    NIFTY_REGIME_SMA = 150
    NIFTY_WEAK_MOM_THRESHOLD = -0.05; NIFTY_WEAK_MOM_MAX_SLOTS = 5
    BLOCKED_ENTRY_MONTHS = [1, 2, 7]
    BLACKLIST_AFTER_N_STOPS = 2; MIN_HOLDING_DAYS = 10
    MIN_SCORE_PERCENTILE = 0.50
    SMA_PERIOD = 66
    trend_margin = 0.02; rsi_min = 40; rsi_max = 70
    momentum_60_min = 0.05; momentum_20_min = 0.0; vol_ratio_min = 1.0
    trend_score_min = 0.02
    trend_break_pct = 0.04; rsi_overbought = 82
    bear_exit_on_neg_trend = True
    WEAK = dict(rsi_min=50, rsi_max=65, momentum_60_min=0.10, trend_score_min=0.04)
    OCT  = dict(rsi_min=50, rsi_max=65, momentum_60_min=0.10)
    SCORE_WEIGHTS = dict(momentum_60=0.40, momentum_20=0.30,
                         trend_score=0.20, vol_ratio=0.10)


def _rsi_simple(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _atr_simple(df, period=14):
    pc = df["close"].shift(1)
    tr = pd.concat([df["high"]-df["low"], (df["high"]-pc).abs(),
                    (df["low"]-pc).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()


class NseMomentum(StrategyBase):
    code = "NM"
    name = "NSE Momentum-Trend v5"

    def __init__(self, ctx):
        super().__init__(ctx)
        self.p = NMParams()
        self.feat: Dict[str, pd.DataFrame] = {}   # per-symbol daily features (date-indexed)
        self.rebal_dates: List[pd.Timestamp] = []
        self.bull: Dict[pd.Timestamp, int] = {}
        self.ret4w: Dict[pd.Timestamp, float] = {}
        self.universe: Set[str] = set()
        # per-RUN mutable state
        self.blacklisted: Set[str] = set()
        self.consec_stops: Dict[str, int] = {}

    def reset_run_state(self):
        self.blacklisted = set()
        self.consec_stops = {}

    def prepare(self):
        p = self.p
        nifty = self.ctx.nifty
        sma = nifty.rolling(p.NIFTY_REGIME_SMA).mean()
        bull = (nifty > sma).astype(int)
        r4w = nifty.pct_change(20)
        self.bull  = {pd.Timestamp(d): int(v) for d, v in bull.items()}
        self.ret4w = {pd.Timestamp(d): (float(v) if pd.notna(v) else 0.0)
                      for d, v in r4w.items()}
        # weekly rebalance dates = first trading day of each ISO week, year>=START
        seen = set()
        for d in self.ctx.all_dates:
            if d.year < p.BACKTEST_START_YEAR:
                continue
            key = (d.isocalendar()[0], d.isocalendar()[1])
            if key not in seen:
                seen.add(key); self.rebal_dates.append(d)
        # per-symbol features + universe filter
        kept = 0
        for sym, d in self.ctx.daily.items():
            if len(d) < 80:
                continue
            g = d.copy()
            g["rsi14"] = _rsi_simple(g["close"], 14)
            g["atr14"] = _atr_simple(g, 14)
            g["momentum_20"] = g["close"] / g["close"].shift(20) - 1
            g["momentum_60"] = g["close"] / g["close"].shift(60) - 1
            g["SMA_66"] = g["close"].rolling(p.SMA_PERIOD).mean()
            g["trend_score"] = (g["close"] - g["SMA_66"]) / g["SMA_66"].replace(0, np.nan)
            g["vol_ma20"] = g["volume"].rolling(20).mean()
            g["vol_ratio"] = g["volume"] / g["vol_ma20"]
            self.feat[sym] = g
            active = g[g.index.year >= p.BACKTEST_START_YEAR]
            if len(active) >= p.MIN_HISTORY_DAYS and active["volume"].mean() >= p.MIN_VOLUME:
                self.universe.add(sym)
            kept += 1
        self._rebal_set = set(self.rebal_dates)
        print(f"    [NM] {kept} symbols, universe {len(self.universe)}, "
              f"{len(self.rebal_dates)} weekly rebalances", flush=True)

    # ---- entries (only on rebalance dates) --------------------------------
    def slot_cap_for_date(self, date):
        if date not in self._rebal_set:
            return 0
        weak = self.ret4w.get(date, 0.0) < self.p.NIFTY_WEAK_MOM_THRESHOLD
        return self.p.NIFTY_WEAK_MOM_MAX_SLOTS if weak else None

    def candidates_for_date(self, date, held):
        p = self.p
        if date not in self._rebal_set:
            return []
        if self.bull.get(date, 1) == 0:                 # bear market: block
            return []
        if date.month in p.BLOCKED_ENTRY_MONTHS:
            return []
        weak = self.ret4w.get(date, 0.0) < p.NIFTY_WEAK_MOM_THRESHOLD
        is_oct = (date.month == 10)
        if weak:
            rsi_min, rsi_max = p.WEAK["rsi_min"], p.WEAK["rsi_max"]
            mom_min, ts_min  = p.WEAK["momentum_60_min"], p.WEAK["trend_score_min"]
        elif is_oct:
            rsi_min, rsi_max = p.OCT["rsi_min"], p.OCT["rsi_max"]
            mom_min, ts_min  = p.OCT["momentum_60_min"], p.trend_score_min
        else:
            rsi_min, rsi_max = p.rsi_min, p.rsi_max
            mom_min, ts_min  = p.momentum_60_min, p.trend_score_min
        # gather candidate rows for this date
        recs = []
        for sym in self.universe:
            if sym in held or sym in self.blacklisted:
                continue
            g = self.feat.get(sym)
            if g is None or date not in g.index:
                continue
            r = g.loc[date]
            if r[["rsi14", "momentum_60", "trend_score", "SMA_66",
                  "close", "vol_ratio", "atr14"]].isna().any():
                continue
            if not (r["close"] > r["SMA_66"] * (1 + p.trend_margin)
                    and rsi_min <= r["rsi14"] <= rsi_max
                    and r["momentum_60"] >= mom_min
                    and r["momentum_20"] >= p.momentum_20_min
                    and r["vol_ratio"] >= p.vol_ratio_min
                    and r["trend_score"] >= ts_min):
                continue
            recs.append({"sym": sym, "close": float(r["close"]),
                         "momentum_60": float(r["momentum_60"]),
                         "momentum_20": float(r["momentum_20"]),
                         "trend_score": float(r["trend_score"]),
                         "vol_ratio": float(r["vol_ratio"])})
        if not recs:
            return []
        cdf = pd.DataFrame(recs)
        for col, wt in p.SCORE_WEIGHTS.items():
            cdf[f"_r_{col}"] = cdf[col].rank(pct=True) * wt
        cdf["_score"] = cdf[[f"_r_{c}" for c in p.SCORE_WEIGHTS]].sum(axis=1)
        cutoff = cdf["_score"].quantile(p.MIN_SCORE_PERCENTILE)
        cdf = cdf[cdf["_score"] >= cutoff]
        if cdf.empty:
            return []
        out = []
        for _, r in cdf.sort_values("_score", ascending=False).iterrows():
            sym = r["sym"]; ep = r["close"]
            out.append(Candidate(sym, pd.Timestamp(date), ep, pd.Timestamp(date),
                                  round(ep * (1 - p.STOP_LOSS_PCT), 2),
                                  float(r["_score"]), {"nm": True}))
        return out

    def on_close(self, symbol, reason):
        if self.p.BLACKLIST_AFTER_N_STOPS <= 0:
            return
        if reason in ("stop_loss", "trailing_stop"):
            self.consec_stops[symbol] = self.consec_stops.get(symbol, 0) + 1
            if self.consec_stops[symbol] >= self.p.BLACKLIST_AFTER_N_STOPS:
                self.blacklisted.add(symbol)
        else:
            self.consec_stops[symbol] = 0

    def _compute_exit(self, cand):
        """
        Walk forward over rebalance dates AFTER entry. Track peak across
        rebalance closes. Priority each rebalance:
          stop_loss > trailing_stop > overbought_half(partial 50%) >
          trend_break > bear_market. overbought_half fires once then holds.
        Fill = that rebalance day's close.
        """
        p = self.p
        sym = cand.symbol; ent = cand.entry_price
        g = self.feat.get(sym)
        if g is None:
            return ExitPlan(None, 0.0, "no_data")
        future = [d for d in self.rebal_dates if d > cand.entry_date]
        peak = ent
        half_done = False
        partial_dt = None; partial_px = 0.0
        last_close = ent
        for d in future:
            holding_days = (d - cand.entry_date).days
            if d not in g.index:
                if holding_days >= p.MIN_HOLDING_DAYS:
                    plan = ExitPlan(d, float(last_close), "no_data")
                    if partial_dt is not None:
                        plan.partial_dt = partial_dt; plan.partial_px = partial_px
                        plan.partial_frac = 0.5
                    return plan
                continue
            r = g.loc[d]
            price = float(r["close"]); last_close = price
            if price > peak:
                peak = price
            trail_active = (peak / ent - 1) >= p.TRAILING_TRIGGER
            trailing_hit = trail_active and price < peak * (1 - p.TRAILING_STOP_PCT)
            stop_hit = price < ent * (1 - p.STOP_LOSS_PCT)
            sma66 = r.get("SMA_66", np.nan); rsi = r.get("rsi14", np.nan)
            tscore = r.get("trend_score", np.nan)
            if holding_days >= p.MIN_HOLDING_DAYS:
                trend_break = pd.notna(sma66) and price < sma66 * (1 - p.trend_break_pct)
                overbought  = pd.notna(rsi) and rsi > p.rsi_overbought
                bear_exit   = (self.bull.get(d, 1) == 0 and p.bear_exit_on_neg_trend
                               and pd.notna(tscore) and tscore < 0)
            else:
                trend_break = overbought = bear_exit = False

            if stop_hit:
                plan = ExitPlan(d, price, "stop_loss")
            elif trailing_hit:
                plan = ExitPlan(d, price, "trailing_stop")
            elif overbought and not half_done:
                # partial 50%, then continue holding the rest
                partial_dt = d; partial_px = price; half_done = True
                peak = price        # reset peak after half exit (original behaviour)
                continue
            elif trend_break:
                plan = ExitPlan(d, price, "trend_break")
            elif bear_exit:
                plan = ExitPlan(d, price, "bear_market")
            else:
                continue
            if partial_dt is not None:
                plan.partial_dt = partial_dt; plan.partial_px = partial_px
                plan.partial_frac = 0.5
            return plan
        # never exited within data
        plan = ExitPlan(None, 0.0, "END_OF_PERIOD")
        if partial_dt is not None:
            plan.partial_dt = partial_dt; plan.partial_px = partial_px
            plan.partial_frac = 0.5
        return plan

# =============================================================================
# PART 4 :: COMBINED PORTFOLIO ENGINE
# =============================================================================
# Position record (plain dict). One symbol is held by at most one strategy.
#   sym, code, entry_dt, entry_date, entry_price, shares, init_shares,
#   stop_price, plan(ExitPlan), partial_done(bool), rank_key, meta

def _split_budget(total_slots: int, n: int) -> List[int]:
    base = total_slots // n
    rem  = total_slots - base * n
    return [base + (1 if i < rem else 0) for i in range(n)]


class ComboPortfolio:
    """
    Runs ONE 3-strategy combination as a single shared TOTAL_SLOTS-stock book.

    Deviations from the standalone strategies (portfolio layer only):
      * equal-weight sizing  : shares = int( (equity / TOTAL_SLOTS) / entry_px )
      * per-strategy budget   : the 20 slots split across the 3 strategies
      * global symbol dedup    : a symbol is held by at most one strategy
      * common gross accounting: no per-trade commission (the majority of the
        source strategies book gross PnL; using one rule keeps combos
        comparable). Each strategy's own entry/exit PRICES are untouched.
    """
    def __init__(self, strategies: List[StrategyBase], budgets: List[int],
                 ctx: DataContext, total_capital: float, total_slots: int):
        self.strats = strategies
        self.codes  = [s.code for s in strategies]
        self.budget = {s.code: b for s, b in zip(strategies, budgets)}
        self.by_code = {s.code: s for s in strategies}
        self.ctx = ctx
        self.cap0 = total_capital
        self.total_slots = total_slots

    def run(self):
        ctx = self.ctx
        for s in self.strats:
            s.reset_run_state()

        cash = float(self.cap0)
        positions: Dict[str, dict] = {}          # sym -> pos
        open_count = {c: 0 for c in self.codes}
        trades = []
        equity_curve = {}
        all_dates = ctx.all_dates
        n = len(all_dates)
        cg = ctx.close_grid

        def cur_price(sym, di):
            arr = cg.get(sym)
            if arr is None:
                return None
            v = arr[di]
            return None if np.isnan(v) else float(v)

        def equity_at(di):
            eq = cash
            for sym, pos in positions.items():
                px = cur_price(sym, di)
                eq += pos["shares"] * (px if px is not None else pos["entry_price"])
            return eq

        def process_exits(date, di):
            nonlocal cash
            done = []
            for sym, pos in list(positions.items()):
                plan: ExitPlan = pos["plan"]
                # ---- partial first ----
                if (not pos["partial_done"] and plan.partial_dt is not None
                        and pd.Timestamp(plan.partial_dt).normalize() <= date
                        and plan.partial_frac > 0):
                    psh = int(pos["shares"] * plan.partial_frac)
                    if psh > 0:
                        cash += psh * plan.partial_px
                        pnl = (plan.partial_px - pos["entry_price"]) * psh
                        trades.append(self._trade_row(pos, plan.partial_dt,
                                      plan.partial_px, psh, pnl, "PARTIAL",
                                      pos["meta_reason_partial"]))
                        pos["shares"] -= psh
                    pos["partial_done"] = True
                # ---- final exit ----
                if (plan.exit_dt is not None
                        and pd.Timestamp(plan.exit_dt).normalize() <= date):
                    sh = pos["shares"]
                    px = plan.exit_px
                    pnl = (px - pos["entry_price"]) * sh
                    cash += sh * px
                    trades.append(self._trade_row(pos, plan.exit_dt, px, sh,
                                  pnl, "EXIT", plan.reason))
                    self.by_code[pos["code"]].on_close(sym, plan.reason)
                    open_count[pos["code"]] -= 1
                    done.append(sym)
            for sym in done:
                positions.pop(sym, None)

        for di, date in enumerate(all_dates):
            # 1) exits / partials due on or before today
            process_exits(date, di)

            # 2) entries (strategy order = combo order); equal-weight sizing
            held = set(positions.keys())
            total_open = len(positions)
            equity_now = equity_at(di)
            alloc = equity_now / self.total_slots
            for code in self.codes:
                if total_open >= self.total_slots:
                    break
                strat = self.by_code[code]
                cap = strat.slot_cap_for_date(date)
                budget = self.budget[code]
                allowed_total = budget if cap is None else min(budget, cap)
                allowed_new = allowed_total - open_count[code]
                if allowed_new <= 0:
                    continue
                allowed_new = min(allowed_new, self.total_slots - total_open)
                if allowed_new <= 0:
                    continue
                cands = strat.candidates_for_date(date, held)
                taken = 0
                for cand in cands:
                    if taken >= allowed_new:
                        break
                    sym = cand.symbol
                    if sym in held:
                        continue
                    ep = cand.entry_price
                    if ep is None or ep <= 0:
                        continue
                    shares = int(alloc / ep)
                    if shares <= 0:
                        continue
                    cost = shares * ep
                    if cost > cash:
                        shares = int(cash / ep)
                        cost = shares * ep
                    if shares <= 0:
                        continue
                    plan = strat.get_exit(cand)
                    cash -= cost
                    positions[sym] = {
                        "sym": sym, "code": code,
                        "entry_dt": cand.entry_dt, "entry_date": cand.entry_date,
                        "entry_price": ep, "shares": shares, "init_shares": shares,
                        "stop_price": cand.stop_price, "plan": plan,
                        "partial_done": False, "rank_key": cand.rank_key,
                        "meta_reason_partial": self._partial_reason(code),
                    }
                    held.add(sym); taken += 1
                    total_open += 1; open_count[code] += 1

            # 3) same-day exit sweep (handles intraday exits on the entry day)
            process_exits(date, di)

            # 4) MTM
            equity_curve[date] = round(equity_at(di), 2)

        # ---- force-close survivors at last available close ----
        last_di = n - 1
        last_date = all_dates[last_di]
        for sym, pos in list(positions.items()):
            px = cur_price(sym, last_di)
            if px is None:
                px = pos["entry_price"]
            sh = pos["shares"]
            pnl = (px - pos["entry_price"]) * sh
            cash += sh * px
            trades.append(self._trade_row(pos, last_date, px, sh, pnl,
                          "EXIT", "END_OF_PERIOD"))
            open_count[pos["code"]] -= 1
        positions.clear()

        trades_df = pd.DataFrame(trades)
        eq = pd.Series(equity_curve).sort_index()
        return trades_df, eq

    @staticmethod
    def _partial_reason(code):
        return {"SQ": "Scale_Out_45", "NM": "Overbought_Half"}.get(code, "PARTIAL")

    @staticmethod
    def _trade_row(pos, exit_dt, exit_px, shares, pnl, kind, reason):
        ep = pos["entry_price"]
        return {
            "Strategy":      pos["code"],
            "Symbol":        pos["sym"],
            "Entry_Date":    str(pd.Timestamp(pos["entry_date"]).date()),
            "Entry_Datetime":str(pd.Timestamp(pos["entry_dt"])),
            "Entry_Price":   round(ep, 4),
            "Exit_Datetime": str(pd.Timestamp(exit_dt)),
            "Exit_Date":     str(pd.Timestamp(exit_dt).date()),
            "Exit_Price":    round(float(exit_px), 4),
            "Shares":        int(shares),
            "Leg":           kind,           # PARTIAL or EXIT
            "PnL":           round(float(pnl), 2),
            "Return_Pct":    round((exit_px / ep - 1) * 100, 3) if ep else 0.0,
            "Exit_Reason":   reason,
            "Rank_Key":      round(float(pos["rank_key"]), 4),
        }


# =============================================================================
# METRICS + REPORTING
# =============================================================================
def compute_metrics(trades_df: pd.DataFrame, eq: pd.Series,
                    cap0: float) -> dict:
    if eq is None or eq.empty:
        return {"trades": 0, "cagr": 0.0, "sharpe": 0.0, "sortino": 0.0,
                "max_dd": 0.0, "mar": 0.0, "win_rate": 0.0, "pf": 0.0,
                "final_eq": cap0, "total_return": 0.0, "avg_hold": 0.0}
    final_eq = float(eq.iloc[-1])
    ny = max((eq.index[-1] - eq.index[0]).days / 365.25, 1e-9)
    cagr = ((final_eq / cap0) ** (1/ny) - 1) * 100 if final_eq > 0 else -100.0
    ret = eq.pct_change().dropna()
    std = ret.std()
    sharpe = (ret.mean() / std * np.sqrt(252)) if std > 0 else 0.0
    dstd = ret[ret < 0].std()
    sortino = (ret.mean() / dstd * np.sqrt(252)) if dstd and dstd > 0 else 0.0
    peak = eq.cummax()
    dd = ((eq - peak) / peak * 100).min()
    mar = (cagr / abs(dd)) if dd < 0 else float("inf")

    exits = trades_df[trades_df["Leg"] == "EXIT"] if not trades_df.empty else pd.DataFrame()
    n = len(exits)
    if n > 0:
        wins = exits[exits["PnL"] > 0]; loss = exits[exits["PnL"] <= 0]
        win_rate = len(wins) / n * 100
        gp = wins["PnL"].sum(); gl = abs(loss["PnL"].sum())
        pf = (gp / gl) if gl > 0 else float("inf")
        # hold days from entry to exit
        hd = (pd.to_datetime(exits["Exit_Date"]) -
              pd.to_datetime(exits["Entry_Date"])).dt.days
        avg_hold = float(hd.mean())
    else:
        win_rate = pf = avg_hold = 0.0
    return {"trades": n, "cagr": cagr, "sharpe": sharpe, "sortino": sortino,
            "max_dd": float(dd), "mar": mar, "win_rate": win_rate, "pf": pf,
            "final_eq": final_eq, "total_return": (final_eq/cap0 - 1)*100,
            "avg_hold": avg_hold}


def save_combo(out_dir, idx, codes, trades_df, eq):
    tag = f"{idx:02d}_{'_'.join(codes)}"
    if trades_df is not None and not trades_df.empty:
        trades_df.to_csv(os.path.join(out_dir, f"combo_{tag}_trades.csv"), index=False)
    if eq is not None and not eq.empty:
        eq.to_frame("Equity").to_csv(os.path.join(out_dir, f"combo_{tag}_equity.csv"))


# =============================================================================
# DRIVER  --  run every C(6,3)=20 combination
# =============================================================================
def build_strategies(ctx: DataContext) -> Dict[str, StrategyBase]:
    classes = {"MB": Multibagger, "BO": Breakout, "SQ": Squeeze,
               "WF": WyckoffFlag, "RD": RsiDual, "NM": NseMomentum}
    out = {}
    for code in STRATEGY_CODES:
        cls = classes[code]
        print(f"  Preparing {code} ({cls.name}) ...", flush=True)
        inst = cls(ctx)
        t0 = time.time()
        inst.prepare()
        print(f"    {code} prepared ({time.time()-t0:.0f}s)", flush=True)
        out[code] = inst
    return out


def run_all_combos(ctx: DataContext):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"\n{SEP}\n  PREPARING 6 STRATEGIES (once, reused across combos)\n{SEP}")
    strat_map = build_strategies(ctx)

    combos = list(itertools.combinations(STRATEGY_CODES, 3))
    print(f"\n{SEP}\n  RUNNING {len(combos)} THREE-STRATEGY COMBINATIONS\n{SEP}")

    summary_rows = []
    for idx, codes in enumerate(combos, 1):
        budgets = _split_budget(TOTAL_SLOTS, len(codes))
        strategies = [strat_map[c] for c in codes]
        t0 = time.time()
        engine = ComboPortfolio(strategies, budgets, ctx,
                                TOTAL_CAPITAL, TOTAL_SLOTS)
        trades_df, eq = engine.run()
        m = compute_metrics(trades_df, eq, TOTAL_CAPITAL)
        save_combo(OUTPUT_DIR, idx, list(codes), trades_df, eq)
        budget_str = "/".join(str(b) for b in budgets)
        summary_rows.append({
            "Combo": "+".join(codes),
            "Budgets": budget_str,
            "Trades": m["trades"],
            "CAGR_%": round(m["cagr"], 2),
            "Sharpe": round(m["sharpe"], 2),
            "Sortino": round(m["sortino"], 2),
            "MaxDD_%": round(m["max_dd"], 2),
            "MAR": round(m["mar"], 2) if np.isfinite(m["mar"]) else np.inf,
            "WinRate_%": round(m["win_rate"], 1),
            "PF": round(m["pf"], 2) if np.isfinite(m["pf"]) else np.inf,
            "FinalEquity": round(m["final_eq"], 0),
            "TotalRet_%": round(m["total_return"], 1),
            "AvgHold_d": round(m["avg_hold"], 1),
        })
        print(f"  [{idx:02d}/{len(combos)}] {'+'.join(codes):12s} "
              f"budget {budget_str:6s} | trades={m['trades']:4d} "
              f"CAGR={m['cagr']:7.2f}% Sharpe={m['sharpe']:5.2f} "
              f"MaxDD={m['max_dd']:7.2f}% ({time.time()-t0:.0f}s)", flush=True)

    summary = pd.DataFrame(summary_rows).sort_values(
        "Sharpe", ascending=False).reset_index(drop=True)
    summary.to_csv(os.path.join(OUTPUT_DIR, "ALL_COMBINATIONS_SUMMARY.csv"),
                   index=False)

    print(f"\n{SEP}\n  ALL COMBINATIONS  --  RANKED BY SHARPE\n{SEP}")
    print(tabulate(summary, headers="keys", tablefmt="simple",
                   showindex=False, floatfmt=".2f"))
    print(f"\n  [SAVED] -> {OUTPUT_DIR}/  "
          f"(per-combo trades+equity, ALL_COMBINATIONS_SUMMARY.csv)")
    return summary


def main():
    print(f"\n{SEP}\n  MULTI-STRATEGY COMBINATION BACKTEST  (every C(6,3)=20 triples)\n{SEP}")
    t_total = time.time()
    intraday_long = load_intraday_zip(INTRADAY_ZIP, BLACKLIST)
    nifty = load_nifty_csv(NIFTY_CSV)
    ctx = DataContext(intraday_long, nifty, START_DATE, END_DATE)
    del intraday_long
    run_all_combos(ctx)
    print(f"\n  TOTAL RUNTIME: {time.time()-t_total:.0f}s\n{SEP}\n")


if __name__ == "__main__":
    main()
