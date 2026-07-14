import os
import time
import json
import zipfile
from datetime import datetime
import numpy as np
import pandas as pd
from tabulate import tabulate

from sqlmodel import Session
from .base_strategy import BaseStrategy
from ..dao.deployed_strategy_dao import get_deployed_strategy_by_id
from ..dao.db import engine
from ..models.db_models import TradeNotification

from kiteconnect import exceptions
from zerodha import Zerodha
from ..common.logging_config import create_logger
from . import multi_strategy_engine as bt

# Initialize logger
logger = create_logger(__name__, 'p2_cash_strategy_live.log')

# Setup trail parameters for strategy engine
bt.WFParams.MIN_TRAIL_BARS = int(os.environ.get("WF_MIN_TRAIL_DAYS", 1))

# --- Pure Helper Functions ---
def inr(x):
    try:    return f"Rs {x:,.0f}"
    except Exception: return str(x)

def fdate(ts): return pd.Timestamp(ts).strftime("%Y-%m-%d")

def make_candidate(pos):
    return bt.Candidate(symbol=pos["symbol"], entry_dt=pd.Timestamp(pos["entry_dt"]),
        entry_price=float(pos["entry_price"]), entry_date=pd.Timestamp(pos["entry_date"]),
        stop_price=float(pos["stop_price"]), rank_key=float(pos["rank_key"]),
        meta=pos.get("meta", {}) or {})

def _partial_reason(code):
    return {"SQ": "Scale_Out_45", "NM": "Overbought_Half"}.get(code, "PARTIAL")

def _json_meta(meta):
    out = {}
    for k, v in (meta or {}).items():
        if isinstance(v, np.floating): out[k] = float(v)
        elif isinstance(v, np.integer): out[k] = int(v)
        else: out[k] = v
    return out

def _parse_dates(s):
    s = s.astype(str)
    dt = pd.to_datetime(s, format="ISO8601", errors="coerce")
    if dt.isna().mean() > 0.5: dt = pd.to_datetime(s, errors="coerce")
    if getattr(dt.dt, "tz", None) is not None: dt = dt.dt.tz_localize(None)
    return dt

def load_daily(path):
    if not os.path.exists(path): raise FileNotFoundError(f"Daily data not found: {path}")
    logger.info(f"  Reading DAILY archive: {path}")
    if path.lower().endswith(".zip"):
        z = zipfile.ZipFile(path)
        members = [n for n in z.namelist() if n.lower().endswith(".csv") and "__MACOSX" not in n]
        if not members: raise ValueError("No CSV inside the zip.")
        sources = [(m, lambda m=m: pd.read_csv(z.open(m), low_memory=False)) for m in members]
    else:
        z = None; sources = [(path, lambda: pd.read_csv(path, low_memory=False))]
    frames = []
    for i, (name, reader) in enumerate(sources, 1):
        df = bt._normalize_columns(reader())
        if i == 1: logger.info(f"  Canonical columns: {list(df.columns)}")
        if "symbol" not in df.columns:
            df["symbol"] = os.path.splitext(os.path.basename(name))[0].upper()
        if "datetime" in df.columns: df["datetime"] = _parse_dates(df["datetime"])
        elif "date" in df.columns:   df["datetime"] = _parse_dates(df["date"])
        else: raise ValueError(f"No date column in {name}: {list(df.columns)}")
        if "volume" not in df.columns: df["volume"] = 0.0
        miss = [c for c in ("symbol","open","high","low","close") if c not in df.columns]
        if miss: raise ValueError(f"Missing {miss} in {name}")
        df = df.dropna(subset=["datetime","open","high","low","close"])
        df = df[["symbol","datetime","open","high","low","close","volume"]]
        if bt.BLACKLIST: df = df[~df["symbol"].isin(bt.BLACKLIST)]
        if not df.empty: frames.append(df)
    if z is not None: z.close()
    if not frames: raise ValueError("No usable rows in daily archive.")
    full = pd.concat(frames, ignore_index=True).sort_values(["symbol","datetime"]).reset_index(drop=True)
    logger.info(f"  [OK] {len(full):,} daily rows | {full['symbol'].nunique()} symbols | "
          f"{full['datetime'].min().date()} -> {full['datetime'].max().date()}")
    return full


class DailyDataContext(bt.DataContext):
    def __init__(self, daily_long, nifty, start, end):
        t0 = time.time(); self.sd = pd.Timestamp(start); self.ed = pd.Timestamp(end)
        df = daily_long.copy(); df["date"] = pd.to_datetime(df["datetime"]).dt.normalize()
        self.daily = {}
        for sym, g in df.groupby("symbol", sort=False):
            d = g.sort_values("date").set_index("date")[["open","high","low","close","volume"]]
            d = d[~d.index.duplicated(keep="last")]
            d = d[(d.index >= self.sd) & (d.index <= self.ed)]
            if len(d) >= 2: self.daily[sym] = d
        all_d = sorted(set().union(*[set(d.index) for d in self.daily.values()]))
        self.all_dates = pd.DatetimeIndex(all_d); self.date2i = {d: i for i, d in enumerate(self.all_dates)}
        logger.info(f"  Daily window: {self.all_dates[0].date()} -> {self.all_dates[-1].date()} "
              f"({len(self.all_dates)} days, {len(self.daily)} symbols)")
        self.intraday = {}
        for sym, d in self.daily.items():
            dates = d.index; dt = dates.values.astype("datetime64[ns]")
            o = d["open"].values.astype(float); h = d["high"].values.astype(float)
            l = d["low"].values.astype(float); c = d["close"].values.astype(float)
            slices = {pd.Timestamp(dates[k]): (k, k+1) for k in range(len(dates))}
            order = [pd.Timestamp(x) for x in dates]
            self.intraday[sym] = bt.SymIntraday(dt, o, h, l, c, dt.copy(), slices, order)
        self.nifty = self._prep_nifty(nifty)
        self.close_grid = {}
        for sym, d in self.daily.items():
            self.close_grid[sym] = d["close"].reindex(self.all_dates).ffill().values.astype(float)
        logger.info(f"  DailyDataContext ready ({time.time()-t0:.0f}s)\n")


# --- Main Strategy Class ---
class StrategyP2Cash(BaseStrategy):
    def __init__(self, params):
        super().__init__()
        
        # Auth Config
        self.user_id = params["api_key"]
        self.password = params["api_secret"]
        self.twofa = params["session_token"]
        
        try:
            self.kite = Zerodha(user_id=self.user_id, password=self.password, twofa=self.twofa)
            self.kite.login()
            logger.info("Zerodha initialized successfully for P2 Cash Strategy")
        except Exception as e:
            logger.error(f"Failed to initialize Zerodha: ", exc_info=e)
            raise e

        # P2 Constants
        self.PORTFOLIO_NAME = params["csv_file"]
        
        # --- PATH EXTRACTION ---
        self.BASE_DIR = os.path.dirname(self.PORTFOLIO_NAME)
        self.CLEAN_NAME = os.path.basename(self.PORTFOLIO_NAME).replace(".csv", "")
        # -----------------------

        self.CODES   = ['BO', 'WF', 'RD']
        self.BUDGETS = [7, 7, 6]
        self.CAPITAL = bt.TOTAL_CAPITAL
        self.SLOTS   = bt.TOTAL_SLOTS
        self.SEP = "=" * 84

        self.FULL = {"MB": "Multibagger v5", "BO": "Breakout v8", "SQ": "Institutional Squeeze 4.3",
                     "WF": "Wyckoff Flag", "RD": "RSI Dual-Stage v6.11", "NM": "NSE Momentum-Trend v5"}
        self.CLASSES = {"MB": bt.Multibagger, "BO": bt.Breakout, "SQ": bt.Squeeze,
                        "WF": bt.WyckoffFlag, "RD": bt.RsiDual, "NM": bt.NseMomentum}

        # Environments and Files
        self.DAILY_DATA = os.environ.get("DAILY_DATA", os.environ.get("INTRADAY_ZIP", "stock_daily.csv"))
        self.NIFTY_CSV  = os.environ.get("NIFTY_CSV", bt.NIFTY_CSV)
        self.GO_LIVE_DATE = os.environ.get("GO_LIVE_DATE")   
        
        # --- SPLIT ROUTING ---
        self.STATE_DIR  = os.environ.get("LIVE_STATE_DIR", f"state_{self.CLEAN_NAME}")
        self.REPORT_DIR = os.environ.get("LIVE_REPORT_DIR", f"reports_{self.CLEAN_NAME}")
        self.TRACK_CSV  = os.environ.get("LIVE_TRACK_CSV", os.path.join(self.BASE_DIR, f"{self.CLEAN_NAME}_report.csv"))
        # ---------------------
        
        self.run_count = 0

    def relogin(self):
        try:
            self.kite = Zerodha(self.user_id, self.password, self.twofa)
            self.kite.login()
            logger.info("Zerodha initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Zerodha: ", exc_info=e)
            raise e

    def load_state(self):
        p = os.path.join(self.STATE_DIR, f"{self.CLEAN_NAME}.json")
        if not os.path.exists(p): return None
        with open(p) as f: return json.load(f)

    def save_state(self, state):
        os.makedirs(self.STATE_DIR, exist_ok=True)
        with open(os.path.join(self.STATE_DIR, f"{self.CLEAN_NAME}.json"), "w") as f:
            json.dump(state, f, indent=2, default=str)

    def init_state(self):
        return {"name": self.CLEAN_NAME, "codes": self.CODES, "budgets": self.BUDGETS,
                "capital0": float(self.CAPITAL), "cash": float(self.CAPITAL),
                "last_processed_date": None, "positions": {}, "closed_trades": [],
                "equity_history": [], "nm_blacklist": [], "nm_consec": {}}

    def advance_portfolio(self, state, strat_map, ctx):
        codes = state["codes"]; budget = {c: b for c, b in zip(codes, state["budgets"])}
        by_code = {c: strat_map[c] for c in codes}
        if "NM" in by_code:
            nm = by_code["NM"]; nm.blacklisted = set(state.get("nm_blacklist", []))
            nm.consec_stops = dict(state.get("nm_consec", {}))
        cash = float(state["cash"])
        positions = {s: dict(p) for s, p in state["positions"].items()}
        for p in positions.values(): p["partial_done"] = bool(p.get("partial_done", False))
        open_count = {c: 0 for c in codes}
        for p in positions.values():
            if p["code"] in open_count: open_count[p["code"]] += 1
        all_dates = ctx.all_dates; date2i = ctx.date2i; cg = ctx.close_grid; latest = all_dates[-1]
        lpd = state["last_processed_date"]
        
        if lpd is None:
            if self.GO_LIVE_DATE:
                gd = pd.Timestamp(self.GO_LIVE_DATE)
                new_dates = [d for d in all_dates if d > gd]
            else:
                new_dates = [latest]
        else:
            new_dates = [d for d in all_dates if d > pd.Timestamp(lpd)]
            
        actions = {"entries": [], "exits": [], "partials": [], "dates": new_dates}
        closed = state["closed_trades"]

        def cur_price(sym, di):
            arr = cg.get(sym)
            if arr is None: return None
            v = arr[di]
            return None if (v is None or (isinstance(v, float) and np.isnan(v))) else float(v)
            
        def equity_at(di):
            eq = cash
            for sym, pos in positions.items():
                px = cur_price(sym, di); eq += pos["shares"] * (px if px is not None else pos["entry_price"])
            return eq
            
        def process_exits(date, di):
            nonlocal cash; done = []
            for sym, pos in list(positions.items()):
                strat = by_code[pos["code"]]; plan = strat.get_exit(make_candidate(pos))
                if (not pos["partial_done"] and plan.partial_dt is not None
                        and pd.Timestamp(plan.partial_dt).normalize() <= date and plan.partial_frac > 0):
                    psh = int(pos["shares"] * plan.partial_frac)
                    if psh > 0:
                        cash += psh * plan.partial_px; pnl = (plan.partial_px - pos["entry_price"]) * psh
                        rec = {"Portfolio": self.CLEAN_NAME, "Strategy": pos["code"], "Symbol": sym, "Leg": "PARTIAL",
                               "Entry_Date": fdate(pos["entry_date"]), "Entry_Price": round(pos["entry_price"], 2),
                               "Exit_Date": fdate(plan.partial_dt), "Exit_Datetime": str(pd.Timestamp(plan.partial_dt)),
                               "Exit_Price": round(float(plan.partial_px), 2), "Shares": psh, "PnL": round(float(pnl), 2),
                               "Return_%": round((plan.partial_px/pos["entry_price"]-1)*100, 2), "Reason": _partial_reason(pos["code"])}
                        closed.append(rec); actions["partials"].append(rec); pos["shares"] -= psh
                    pos["partial_done"] = True
                    
                if plan.exit_dt is not None and pd.Timestamp(plan.exit_dt).normalize() <= date:
                    sh = pos["shares"]; px = float(plan.exit_px); pnl = (px - pos["entry_price"]) * sh; cash += sh * px
                    rec = {"Portfolio": self.CLEAN_NAME, "Strategy": pos["code"], "Symbol": sym, "Leg": "EXIT",
                           "Entry_Date": fdate(pos["entry_date"]), "Entry_Price": round(pos["entry_price"], 2),
                           "Exit_Date": fdate(plan.exit_dt), "Exit_Datetime": str(pd.Timestamp(plan.exit_dt)),
                           "Exit_Price": round(px, 2), "Shares": sh, "PnL": round(float(pnl), 2),
                           "Return_%": round((px/pos["entry_price"]-1)*100, 2), "Reason": plan.reason,
                           "Hold_Days": (pd.Timestamp(plan.exit_dt).normalize()-pd.Timestamp(pos["entry_date"]).normalize()).days}
                    closed.append(rec); actions["exits"].append(rec)
                    by_code[pos["code"]].on_close(sym, plan.reason); open_count[pos["code"]] -= 1; done.append(sym)
            for sym in done: positions.pop(sym, None)
            
        def do_entries(date, di):
            nonlocal cash; held = set(positions.keys()); total_open = len(positions)
            alloc = equity_at(di) / self.SLOTS
            for code in codes:
                if total_open >= self.SLOTS: break
                strat = by_code[code]; cap = strat.slot_cap_for_date(date)
                allowed_total = budget[code] if cap is None else min(budget[code], cap)
                allowed_new = allowed_total - open_count[code]
                if allowed_new <= 0: continue
                allowed_new = min(allowed_new, self.SLOTS - total_open)
                if allowed_new <= 0: continue
                taken = 0
                for cand in strat.candidates_for_date(date, held):
                    if taken >= allowed_new: break
                    sym = cand.symbol
                    if sym in held: continue
                    ep = cand.entry_price
                    if ep is None or ep <= 0: continue
                    shares = int(alloc / ep)
                    if shares <= 0: continue
                    cost = shares * ep
                    if cost > cash: shares = int(cash / ep); cost = shares * ep
                    if shares <= 0: continue
                    cash -= cost
                    positions[sym] = {"symbol": sym, "code": code, "entry_dt": str(pd.Timestamp(cand.entry_dt)),
                        "entry_date": fdate(cand.entry_date), "entry_price": float(ep), "shares": shares,
                        "init_shares": shares, "stop_price": float(cand.stop_price), "partial_done": False,
                        "rank_key": float(cand.rank_key), "meta": _json_meta(cand.meta)}
                    actions["entries"].append({"Portfolio": self.CLEAN_NAME, "Strategy": code, "Symbol": sym,
                        "Entry_Date": fdate(date), "Entry_Datetime": str(pd.Timestamp(cand.entry_dt)),
                        "Entry_Price": round(float(ep), 2), "Shares": shares, "Allocation": round(cost, 0),
                        "Stop": round(float(cand.stop_price), 2), "Rank": round(float(cand.rank_key), 3)})
                    held.add(sym); taken += 1; total_open += 1; open_count[code] += 1
                    
        for d in new_dates:
            di = date2i[d]; process_exits(d, di); do_entries(d, di); process_exits(d, di)
            state["equity_history"].append([fdate(d), round(equity_at(di), 2)])
            
        state["cash"] = float(cash); state["positions"] = positions
        state["last_processed_date"] = fdate(latest)
        if "NM" in by_code:
            state["nm_blacklist"] = sorted(by_code["NM"].blacklisted); state["nm_consec"] = by_code["NM"].consec_stops
        return actions, latest

    def portfolio_stats(self, state, ctx):
        di = ctx.date2i[ctx.all_dates[-1]]; cg = ctx.close_grid
        cash = state["cash"]; cap0 = state["capital0"]; invested = 0.0; open_pnl = 0.0; rows = []
        for sym, p in state["positions"].items():
            arr = cg.get(sym); px = None
            if arr is not None and not (isinstance(arr[di], float) and np.isnan(arr[di])): px = float(arr[di])
            mv = p["shares"] * (px if px is not None else p["entry_price"]); invested += mv
            opl = (px - p["entry_price"]) * p["shares"] if px is not None else 0.0; open_pnl += opl
            rows.append({"Strategy": p["code"], "Symbol": sym, "Entry_Date": p["entry_date"],
                "Entry": round(p["entry_price"], 2), "Now": round(px, 2) if px is not None else "n/a",
                "Shares": p["shares"], "Mkt_Value": round(mv, 0), "Open_PnL": round(opl, 0),
                "Open_%": round((px/p["entry_price"]-1)*100, 2) if px else 0.0, "Init_Stop": round(p["stop_price"], 2)})
        equity = cash + invested
        realized = sum(t["PnL"] for t in state["closed_trades"])
        exits = [t for t in state["closed_trades"] if t["Leg"] == "EXIT"]; n = len(exits)
        wins = [t for t in exits if t["PnL"] > 0]; gp = sum(t["PnL"] for t in wins)
        gl = abs(sum(t["PnL"] for t in exits if t["PnL"] <= 0))
        win_rate = (len(wins)/n*100) if n else 0.0
        pf = (gp/gl) if gl > 0 else (float("inf") if gp > 0 else 0.0)
        eh = state["equity_history"]; max_dd = 0.0; cagr = None; sharpe = 0.0
        if len(eh) >= 2:
            s = pd.Series([e[1] for e in eh], index=pd.to_datetime([e[0] for e in eh]))
            peak = s.cummax(); max_dd = float(((s-peak)/peak*100).min())
            days = max((s.index[-1]-s.index[0]).days, 1)
            if days >= 25 and s.iloc[-1] > 0: cagr = ((s.iloc[-1]/cap0)**(365.25/days)-1)*100
            r = s.pct_change().dropna()
            if r.std() > 0: sharpe = float(r.mean()/r.std()*np.sqrt(252))
        return {"cash": cash, "invested": invested, "equity": equity,
            "deployed_pct": (invested/equity*100) if equity else 0.0, "open_positions": len(state["positions"]),
            "realized_pnl": realized, "open_pnl": open_pnl, "total_return_pct": (equity/cap0-1)*100,
            "closed_trades": n, "win_rate": win_rate, "pf": pf, "max_dd": max_dd, "cagr": cagr,
            "sharpe": sharpe, "holdings_rows": rows}

    def build_report(self, state, actions, stats, latest):
        title = "  +  ".join(f"{self.FULL[c]} ({b})" for c, b in zip(state["codes"], state["budgets"]))
        L = [self.SEP, f"  ACCOUNT PORTFOLIO {self.CLEAN_NAME}", f"  {title}",
             f"  Processed through: {fdate(latest)}   (new days this run: {len(actions['dates'])})", self.SEP]
        def tbl(rows, cols):
            if not rows: return "    (none)"
            return tabulate(pd.DataFrame(rows)[cols], headers="keys", tablefmt="simple", showindex=False)
        L += ["\n  >> EXITS today / since last run:",
              tbl(actions["exits"], ["Strategy","Symbol","Entry_Date","Entry_Price","Exit_Date",
                  "Exit_Datetime","Exit_Price","Shares","PnL","Return_%","Reason","Hold_Days"])]
        L += ["\n  >> PARTIAL scale-outs:",
              tbl(actions["partials"], ["Strategy","Symbol","Exit_Date","Exit_Price","Shares","PnL","Return_%","Reason"])]
        L += ["\n  >> NEW ENTRIES (place per each strategy's fill convention - see header):",
              tbl(actions["entries"], ["Strategy","Symbol","Entry_Date","Entry_Datetime","Entry_Price","Shares","Allocation","Stop","Rank"])]
        L += [f"\n  >> CURRENT HOLDINGS ({stats['open_positions']}/{self.SLOTS} slots):",
              tbl(stats["holdings_rows"], ["Strategy","Symbol","Entry_Date","Entry","Now","Shares","Mkt_Value","Open_PnL","Open_%","Init_Stop"])]
        cagr = f"{stats['cagr']:.2f}%" if stats["cagr"] is not None else "n/a (short history)"
        pf = "inf" if stats["pf"] == float("inf") else f"{stats['pf']:.2f}"
        L += ["\n  >> PORTFOLIO STATS",
              f"     Cash ........... {inr(stats['cash'])}",
              f"     Invested (MTM).. {inr(stats['invested'])}",
              f"     Total Equity ... {inr(stats['equity'])}",
              f"     Deployed ....... {stats['deployed_pct']:.1f}%   ({stats['open_positions']}/{self.SLOTS} slots)",
              f"     Open P&L ....... {inr(stats['open_pnl'])}",
              f"     Realized P&L ... {inr(stats['realized_pnl'])}  (since go-live)",
              f"     Total Return ... {stats['total_return_pct']:.2f}%   on {inr(state['capital0'])} base",
              f"     Closed trades .. {stats['closed_trades']}   Win rate {stats['win_rate']:.1f}%   Profit factor {pf}",
              f"     Max Drawdown ... {stats['max_dd']:.2f}%   Sharpe {stats['sharpe']:.2f}   CAGR(go-live) {cagr}", ""]
        return "\n".join(L)

    def append_tracking(self, state, stats, latest):
        row = {"Run_Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Processed_Through": fdate(latest), "Portfolio": self.CLEAN_NAME,
            "Open_Positions": stats["open_positions"], "Cash": round(stats["cash"], 0),
            "Invested": round(stats["invested"], 0), "Equity": round(stats["equity"], 0),
            "Deployed_%": round(stats["deployed_pct"], 1), "Open_PnL": round(stats["open_pnl"], 0),
            "Realized_PnL": round(stats["realized_pnl"], 0), "Total_Return_%": round(stats["total_return_pct"], 2),
            "Closed_Trades": stats["closed_trades"], "Win_Rate_%": round(stats["win_rate"], 1),
            "Profit_Factor": (round(stats["pf"], 2) if stats["pf"] != float("inf") else "inf"),
            "Max_DD_%": round(stats["max_dd"], 2)}
        df = pd.DataFrame([row])
        df.to_csv(self.TRACK_CSV, index=False)

    def diagnose(self, ctx, strat_map, codes, date):
        nf = ctx.nifty
        def ma(w): return float(nf.rolling(w).mean().iloc[-1]) if len(nf) >= w else float("nan")
        c = float(nf.iloc[-1]); s44 = ma(44); s88 = ma(88); s150 = ma(150)
        e50 = float(nf.ewm(span=50, adjust=False).mean().iloc[-1])
        def st(cond): return "OK" if cond else "BLOCK"
        msg = f"\n  >> DIAGNOSTICS for {fdate(date)}\n"
        msg += f"     Nifty close {c:.0f}\n"
        msg += f"     SMA44 {s44:.0f} [{st(c>=s44)}] (RD/BO)   SMA88 {s88:.0f} [{st(c>=s88)}] (WF)\n"
        msg += f"   SMA150 {s150:.0f} [{st(c>s150)}] (NM)   EMA50 {e50:.0f} [{st(c>e50)}] (SQ)\n"
        nm = strat_map.get("NM")
        if nm is not None:
            msg += f"     NM weekly-rebalance day? {'YES' if date in getattr(nm,'_rebal_set',set()) else 'NO'}   month-blocked? {'YES' if date.month in nm.p.BLOCKED_ENTRY_MONTHS else 'NO'}   universe={len(getattr(nm,'universe',[]))}\n"
        for code in codes:
            s = strat_map[code]; post = len(s.candidates_for_date(date, set()))
            raw = len(getattr(s, "cand_by_date", {}).get(date, [])) if hasattr(s, "cand_by_date") else None
            extra = f"  ready={fdate(s.ready_date)}" if code == "RD" else ""
            rawtxt = f"raw signals={raw}, " if raw is not None else ""
            msg += f"     {code}: {rawtxt}eligible after all gates={post}{extra}\n"
        msg += "     (eligible>0 but 0 entries => slot/budget/dedup full; eligible=0 => no signal that day OR a gate above blocked it)\n"
        logger.info(msg)


    def _log_trades_to_db(self, actions, db_session, deployed_strategy):
        try:
            for entry in actions.get("entries", []):
                new_trade = TradeNotification(
                    user_id=deployed_strategy.user_id,
                    strategy_id=deployed_strategy.strategy_id,
                    account_id=deployed_strategy.session_token.account_id,
                    action="ENTRY",
                    symbol=entry["Symbol"],
                    instrument_detail="Equity",
                    price=entry["Entry_Price"],
                    qty=entry["Shares"]
                )
                db_session.add(new_trade)

            for exit_trade in actions.get("exits", []):
                new_trade = TradeNotification(
                    user_id=deployed_strategy.user_id,
                    strategy_id=deployed_strategy.strategy_id,
                    account_id=deployed_strategy.session_token.account_id,
                    action="EXIT",
                    symbol=exit_trade["Symbol"],
                    instrument_detail="Equity",
                    price=exit_trade["Exit_Price"],
                    qty=exit_trade["Shares"]
                )
                db_session.add(new_trade)

            for partial in actions.get("partials", []):
                new_trade = TradeNotification(
                    user_id=deployed_strategy.user_id,
                    strategy_id=deployed_strategy.strategy_id,
                    account_id=deployed_strategy.session_token.account_id,
                    action="EXIT",
                    symbol=partial["Symbol"],
                    instrument_detail="Equity",
                    price=partial["Exit_Price"],
                    qty=partial["Shares"]
                )
                db_session.add(new_trade)

            db_session.commit()
            logger.info(f"Database Notifications Synced: {len(actions.get('entries', []))} Entries, {len(actions.get('exits', []))} Exits, {len(actions.get('partials', []))} Partials.")
        except Exception as e:
            logger.error(f"Failed to log trades to DB for Strategy {deployed_strategy.strategy_id}: {e}")
            db_session.rollback()


    # <------------Main Scheduler Hook------>
    def run(self, deployed_strategy_id: int) -> None:
        self.relogin()
        self.run_count += 1
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        log_msg = f"========== STARTING P2 EXECUTION #{self.run_count} AT {current_time} =========="
        logger.info(log_msg)

        with Session(engine) as db_session:
            deployed_strategy = get_deployed_strategy_by_id(db_session, deployed_strategy_id)
            
            if not deployed_strategy or deployed_strategy.status != "RUNNING":
                logger.info(f"Strategy {deployed_strategy_id} is stopped. Aborting scheduled run.")
                return

            try:
                db_session.refresh(deployed_strategy)
                
                # P2 relies on stock_daily.csv and nifty_data.csv produced by P1.
                daily_long = load_daily(self.DAILY_DATA)
                nf = bt.load_nifty_csv(self.NIFTY_CSV)
                ctx = DailyDataContext(daily_long, nf, bt.START_DATE, bt.END_DATE)
                del daily_long 
                
                latest = ctx.all_dates[-1]
                logger.info(f"  Latest data date: {fdate(latest)}")
                
                logger.info(f"  Preparing strategies: {', '.join(self.CODES)}")
                strat_map = {}
                for c in self.CODES:
                    inst = self.CLASSES[c](ctx)
                    inst.prepare()
                    strat_map[c] = inst
                    
                state = self.load_state() or self.init_state()
                state["budgets"] = self.BUDGETS
                lpd = state["last_processed_date"]
                os.makedirs(self.REPORT_DIR, exist_ok=True)
                
                if lpd is not None and pd.Timestamp(lpd) >= latest:
                    stats = self.portfolio_stats(state, ctx)
                    logger.info(f"No new data since {lpd} (latest in file = {fdate(latest)}). Exiting gracefully.")
                    rep = self.build_report(state, {"entries": [], "exits": [], "partials": [], "dates": []}, stats, latest)
                else:
                    actions, latest = self.advance_portfolio(state, strat_map, ctx)
                    stats = self.portfolio_stats(state, ctx)
                    self.save_state(state)
                    
                    rep = self.build_report(state, actions, stats, latest)
                    self._log_trades_to_db(actions, db_session, deployed_strategy)

                self.append_tracking(state, stats, latest)
                
                if stats["holdings_rows"]:
                    pd.DataFrame(stats["holdings_rows"]).to_csv(self.PORTFOLIO_NAME, index=False)
                    logger.info(f"  [SAVED] Holdings CSV -> {self.PORTFOLIO_NAME}")
                
                logger.info(rep)
                self.diagnose(ctx, strat_map, self.CODES, latest)
                
                rp = os.path.join(self.REPORT_DIR, f"daily_report_{fdate(latest)}.txt")
                with open(rp, "w") as f: 
                    f.write(rep)
                logger.info(f"  [SAVED] report -> {rp}")
                logger.info(f"  [SAVED] state  -> {self.STATE_DIR}/{self.CLEAN_NAME}.json   tracking -> {self.TRACK_CSV}")
                logger.info(f"[INFO]: YAY! We have successfully reached the end of this interval!") 
                
            except Exception as e:
                logger.error(f"Error during P2 scheduled execution:", exc_info=e)