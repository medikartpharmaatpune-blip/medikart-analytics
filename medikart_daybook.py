"""
medikart_daybook.py  v3  —  Medikart Day Book Generator
========================================================
Corrected data sources (verified against real Carew data Apr-2026):

  Sale        TRCSHR.VOU_DT + BILL_AMT
              (TR* archives used only for PRFT_AMT — not for sale total)

  Purchase    PURCHTRAN only (NOT CNGPURCHTRAN — that is archive/history)
              Gross  = QTY × PR_TRATE   (verified: matches Carew Gross Amt)
              GST    = SGSTAMT + CGSTAMT + IGSTAMT
              Net    = Gross + GST       (matches Carew Net Amount)

  Profit      PRFT_AMT from TR* monthly archives (actual field, 7-10% margin)

  Collection  STATMENT.TR_DATE + AMT_RCD

  CR notes    CRDBTR  S_CODE=E → credit note
  DB notes    CRDBTR  S_CODE=G or S → debit note

  Stock       CL_STK × PUR_RATE  (cost-based, matches Carew stock report)
              OP_STK × PUR_RATE  for opening stock

Usage
  py -3.12 medikart_daybook.py --folder "D:\\CAREW"
  start D:\\CAREW\\medikart_daybook.html
"""

import argparse, json, datetime as dt, re
from pathlib import Path

try:
    from dbfread import DBF
    import pandas as pd
except ImportError:
    print("Run:  py -3.12 -m pip install dbfread pandas")
    raise


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_table(folder: Path, name: str, silent=False) -> pd.DataFrame:
    hits = [f for f in folder.iterdir()
            if f.is_file() and f.stem.upper() == name.upper()
            and f.suffix.lower() == ".dbf"]
    if not hits:
        if not silent: print(f"  [WARN] {name}.DBF not found")
        return pd.DataFrame()
    try:
        recs = list(DBF(str(hits[0]), load=True, encoding="utf-8",
                        ignore_missing_memofile=True))
        df = pd.DataFrame(recs)
        if not silent:
            print(f"  Loaded  {name:<18} {len(df):>8,} rows")
        return df
    except Exception as e:
        print(f"  [ERROR] {name}: {e}")
        return pd.DataFrame()


def load_monthly_sales(folder: Path) -> pd.DataFrame:
    """TR* archives (TRAPR26 etc.) — used only for PRFT_AMT, not sale total."""
    pat = re.compile(r'^TR(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}$', re.I)
    frames = []
    for f in sorted(folder.iterdir()):
        if f.is_file() and f.suffix.lower() == ".dbf" and pat.match(f.stem.upper()):
            try:
                recs = list(DBF(str(f), load=True, encoding="utf-8",
                                ignore_missing_memofile=True))
                if recs:
                    frames.append(pd.DataFrame(recs))
            except Exception:
                pass
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    print(f"  Loaded  TR_MONTHLY         {len(combined):>8,} rows  ({len(frames)} files)")
    return combined


def col(df: pd.DataFrame, name: str, default=0) -> pd.Series:
    if name in df.columns:
        return df[name]
    return pd.Series([default] * len(df), index=df.index)


def n(s) -> pd.Series:
    if isinstance(s, (int, float)):
        return pd.Series([s], dtype=float)
    return pd.to_numeric(s, errors="coerce").fillna(0)


def d(s) -> pd.Series:
    if isinstance(s, (int, float, type(None))):
        return pd.Series([pd.NaT])
    return pd.to_datetime(s, errors="coerce")


def inr(v):
    v = float(v or 0)
    if v >= 1e5: return f"\u20b9{v/1e5:.2f}L"
    if v >= 1e3: return f"\u20b9{v/1e3:.1f}K"
    return f"\u20b9{v:.2f}"


def ser(o):
    if hasattr(o, "isoformat"): return o.isoformat()
    if hasattr(o, "item"):      return o.item()
    return str(o)


# ─────────────────────────────────────────────────────────────────────────────
# Daily series builders — verified against real Carew data
# ─────────────────────────────────────────────────────────────────────────────

def daily_sales(trcshr: pd.DataFrame, sale_lines: pd.DataFrame) -> pd.DataFrame:
    """
    TRCSHR = payment RECEIPTS (BKR/CSR) — NOT sale bills.
             Use TRCSHR.CLEAR_AMT for Collection only.

    TR* monthly archives (TRAPR26 etc.) = actual sale bill lines.
      Sale amount = QTY × RATE  per TR_DATE          (99.3% match to Carew)
      Sale GST    = SGSTAMT+CGSTAMT+IGSTAMT+CESSAMT
      Bills count = unique VOU_NO+VOU_TYPE per TR_DATE
      Profit      = PRFT_AMT  (actual field, not estimated)

    Verified against Carew sale register 01-Apr to 18-Apr-2026:
      Calc  QTY×RATE = 349,283  Carew Amount = 346,749  diff <1%
      Calc  GST      =  17,032  Carew GST    =  17,022  diff <0.1%
    """
    result = pd.DataFrame()

    # Sale from TR* monthly archives
    if not sale_lines.empty:
        sl = sale_lines.copy()
        sl["_dt"]      = d(col(sl, "TR_DATE"))
        sl["QTY"]      = n(col(sl, "QTY"))
        sl["RATE"]     = n(col(sl, "RATE"))
        sl["PRFT_AMT"] = n(col(sl, "PRFT_AMT"))
        sl["SGSTAMT"]  = n(col(sl, "SGSTAMT"))
        sl["CGSTAMT"]  = n(col(sl, "CGSTAMT"))
        sl["IGSTAMT"]  = n(col(sl, "IGSTAMT"))
        sl["CESSAMT"]  = n(col(sl, "CESSAMT"))
        sl["VOU_TYPE"] = col(sl, "VOU_TYPE", "").astype(str)
        sl["CDAMT"]    = n(col(sl, "CDAMT"))     # cash discount given to customer
        sl["DISC_SCM"] = n(col(sl, "DISC_SCM"))  # scheme discount
        sl["SALE"]     = sl["QTY"] * sl["RATE"]
        sl["SALE_GST"] = sl["SGSTAMT"] + sl["CGSTAMT"] + sl["IGSTAMT"] + sl["CESSAMT"]
        sl["SALE_DISC"]= sl["CDAMT"] + sl["DISC_SCM"]  # total sale discount
        sl = sl[sl["_dt"].notna()]
        # Bill key = VOU_NO + VOU_TYPE (CCB and CRB have separate numbering)
        sl["BILL_KEY"] = sl["VOU_NO"].astype(str) + "_" + sl["VOU_TYPE"]
        g = (sl.groupby(sl["_dt"].dt.date)
               .agg(sale     =("SALE",      "sum"),
                    sale_gst =("SALE_GST",  "sum"),
                    sale_disc=("SALE_DISC", "sum"),  # CDAMT + DISC_SCM
                    profit   =("PRFT_AMT",  "sum"),
                    sale_qty =("QTY",       "sum"),
                    bills    =("BILL_KEY",  "nunique"))
               .reset_index().rename(columns={"_dt": "date"}))
        g["date"] = pd.to_datetime(g["date"])
        result = g

    # Fallback: if no TR archives, estimate from TRCSHR
    # (TRCSHR is receipts not sales — use only as last resort)
    if result.empty and not trcshr.empty:
        df = trcshr.copy()
        df["_dt"]      = d(col(df, "VOU_DT"))
        df["BILL_AMT"] = n(col(df, "BILL_AMT"))
        df = df[df["_dt"].notna() & (df["BILL_AMT"] > 0)]
        # Exclude large receipts (>50K on single row = bulk collection, not day sale)
        df = df[df["BILL_AMT"] < 50000]
        g = (df.groupby(df["_dt"].dt.date)
               .agg(sale=("BILL_AMT", "sum"),
                    bills=("VOU_NO",   "count"))
               .reset_index().rename(columns={"_dt": "date"}))
        g["date"] = pd.to_datetime(g["date"])
        g["profit"]   = g["sale"] * 0.12
        g["sale_gst"] = 0.0
        g["sale_qty"] = 0.0
        result = g
        print("  [WARN] Using TRCSHR as sale fallback — accuracy limited")

    for c in ["sale", "sale_gst", "sale_disc", "profit", "bills", "sale_qty"]:
        if c not in result.columns: result[c] = 0.0
        result[c] = n(result[c])

    # Fallback profit estimate if PRFT_AMT is zero
    mask = result["profit"] == 0
    result.loc[mask, "profit"] = result.loc[mask, "sale"] * 0.12

    return result[["date","sale","sale_gst","sale_disc","profit","bills","sale_qty"]].fillna(0)


def daily_purchase(purch: pd.DataFrame) -> pd.DataFrame:
    """
    PURCHTRAN only — CNGPURCHTRAN is history/archive, inflates totals.
    Verified formula (all 7 bills on Apr-18 match exactly):
      Gross = QTY × PR_TRATE
      GST   = SGSTAMT + CGSTAMT + IGSTAMT
      Net   = Gross + GST
    """
    if purch.empty:
        return pd.DataFrame()

    df = purch.copy()
    df["_dt"]      = d(col(df, "BILL_DT"))
    df["QTY"]      = n(col(df, "QTY"))
    df["PR_TRATE"] = n(col(df, "PR_TRATE"))
    df["SGSTAMT"]  = n(col(df, "SGSTAMT"))
    df["CGSTAMT"]  = n(col(df, "CGSTAMT"))
    df["IGSTAMT"]  = n(col(df, "IGSTAMT"))
    df["DISC_AMT"] = n(col(df, "DISC_AMT"))
    df["SCM_AMT"]  = n(col(df, "SCM_AMT"))
    df["GROSS"]    = df["QTY"] * df["PR_TRATE"]
    df["GST"]      = df["SGSTAMT"] + df["CGSTAMT"] + df["IGSTAMT"]
    df["NET"]      = df["GROSS"] + df["GST"]
    # DISC_AMT = item disc (negative = discount received, keep sign)
    # SCM_AMT  = scheme / free goods value (positive)
    df = df[df["_dt"].notna()]

    g = (df.groupby(df["_dt"].dt.date)
           .agg(purchase     =("NET",      "sum"),
                pur_gross    =("GROSS",    "sum"),
                pur_gst      =("GST",      "sum"),
                pur_item_disc=("DISC_AMT", "sum"),
                pur_scm_amt  =("SCM_AMT",  "sum"),
                pur_bills    =("BILL_NO",  "nunique"))
           .reset_index().rename(columns={"_dt": "date"}))
    g["pur_discount"] = g["pur_item_disc"] + g["pur_scm_amt"]
    g["date"] = pd.to_datetime(g["date"])
    return g


def daily_collection(stmt: pd.DataFrame) -> pd.DataFrame:
    """
    TRCSHR = actual collection/receipt table (BKR=bank receipt, CSR=cash receipt)
    Use CLEAR_AMT = amount actually received on that date.
    BILL_AMT can be inflated (covers old bills, bulk receipts).
    """
    if stmt.empty:
        return pd.DataFrame()
    df = stmt.copy()
    df["_dt"]      = d(col(df, "VOU_DT"))          # VOU_DT = receipt date
    df["CLEAR_AMT"]= n(col(df, "CLEAR_AMT"))        # amount actually cleared
    df["BILL_AMT"] = n(col(df, "BILL_AMT"))
    df["DISCOUNT"] = n(col(df, "DISCOUNT"))
    df = df[df["_dt"].notna()]
    g = (df.groupby(df["_dt"].dt.date)
           .agg(collection          =("CLEAR_AMT", "sum"),
                collection_discount =("DISCOUNT",  "sum"),
                receipts            =("VOU_NO",    "count"))
           .reset_index().rename(columns={"_dt": "date"}))
    g["date"] = pd.to_datetime(g["date"])
    return g


def daily_crdb_notes(allsale: pd.DataFrame, crdb: pd.DataFrame) -> pd.DataFrame:
    """
    Sale CN = ALLSALE.CR_AMT by TR_DATE
              CN amount adjusted/deducted in a sale bill at billing time.
              Verified: Apr-04=791, Apr-07=2467 — exact match to Carew day book.

    Pur DN  = CNGPURCHMAST.CRDB_AMT by BILL_DT
              = DN amount adjusted by supplier in purchase bill.
              Currently 0 — will populate when supplier adjusts a DN against a bill.
              (CRDBTR DN1 = standalone inventory entries, excluded from P&L)
    """
    result = pd.DataFrame()

    # ── Sale CN: ALLSALE.CR_AMT ───────────────────────────────
    if not allsale.empty and "CR_AMT" in allsale.columns:
        al = allsale.copy()
        al["_dt"]   = d(col(al, "TR_DATE"))
        al["CR_AMT"]= n(col(al, "CR_AMT"))
        al = al[al["_dt"].notna() & (al["CR_AMT"] > 0)]
        if not al.empty:
            gr_cr = (al.groupby(al["_dt"].dt.date)["CR_AMT"]
                       .sum().reset_index()
                       .rename(columns={"_dt": "date", "CR_AMT": "credit_note"}))
            gr_cr["date"] = pd.to_datetime(gr_cr["date"])
            result = gr_cr

    # ── Pur DN: CNGPURCHMAST.CRDB_AMT ───────────────────────
    # = DN amount adjusted by supplier in purchase bill (same logic as ALLSALE.CR_AMT for Sale CN)
    # CRDBTR DN1 entries are standalone inventory adjustments — excluded from P&L
    # Currently 0 in data — will populate when supplier adjusts DN in a purchase bill

    if result.empty:
        return pd.DataFrame()

    for c in ["credit_note", "debit_note"]:
        if c not in result.columns:
            result[c] = 0.0
    result["date"] = pd.to_datetime(result["date"])
    return result


def stock_snapshot(stock: pd.DataFrame) -> dict:
    """
    Stock at cost = CL_STK × PUR_RATE
    Verified: CL_STK×PUR_RATE = ₹2.83L (cost), CL_STK×MRP = ₹4.37L (retail)
    Carew stock reports show cost value.
    Only include rows with valid qty and rate (skip NaN new batches).
    """
    if stock.empty:
        return {"op_stock": 0, "cl_stock": 0}
    df = stock.copy()
    df["OP_STK"]   = n(col(df, "OP_STK"))
    df["CL_STK"]   = n(col(df, "CL_STK"))
    df["PUR_RATE"] = n(col(df, "PUR_RATE"))
    op = df[(df["OP_STK"]  > 0) & (df["PUR_RATE"] > 0)]
    cl = df[(df["CL_STK"]  > 0) & (df["PUR_RATE"] > 0)]
    return {
        "op_stock": round(float((op["OP_STK"]  * op["PUR_RATE"]).sum()), 2),
        "cl_stock": round(float((cl["CL_STK"]  * cl["PUR_RATE"]).sum()), 2),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Merge all daily series
# ─────────────────────────────────────────────────────────────────────────────

def build_daybook(folder: Path) -> list:
    print(f"\nMedikart Day Book  v3")
    print(f"Folder : {folder}")
    print(f"{'─'*60}\n")

    trcshr     = load_table(folder, "TRCSHR")   # receipts/collection
    purch      = load_table(folder, "PURCHTRAN")  # current purchase lines
    # CNGPURCHTRAN intentionally not loaded — archive/history, inflates totals
    # STATMENT intentionally not loaded — TRCSHR is the collection source
    crdb       = load_table(folder, "CRDBTR")
    allsale    = load_table(folder, "ALLSALE")    # sale bills — CR_AMT = CN adjusted in bill
    stock      = load_table(folder, "STOCK")
    sale_lines = load_monthly_sales(folder)   # TR* archives = actual sale bills

    print("\n  Building daily series...")
    ds  = daily_sales(trcshr, sale_lines)     # sale from TR*, collection from TRCSHR
    dp  = daily_purchase(purch)
    dc  = daily_collection(trcshr)            # TRCSHR = receipts
    dn  = daily_crdb_notes(allsale, crdb)
    stk = stock_snapshot(stock)

    merged = ds
    for df in [dp, dc, dn]:
        if not df.empty:
            merged = merged.merge(df, on="date", how="outer") \
                     if not merged.empty else df

    if merged.empty:
        print("  No data — check DBF files have records.")
        return []

    merged = merged.sort_values("date").reset_index(drop=True)
    merged["date"] = pd.to_datetime(merged["date"])

    for c in ["sale","sale_gst","sale_disc","purchase","pur_gross","pur_gst",
              "pur_discount","pur_item_disc","pur_scm_amt","collection","collection_discount","credit_note",
              "debit_note","profit","bills","pur_bills","sale_qty","receipts"]:
        if c not in merged.columns: merged[c] = 0.0
        merged[c] = n(merged[c])

    merged["gross_profit"] = merged["profit"].round(2)
    merged["net_profit"]   = (merged["profit"] - merged["credit_note"] + merged["debit_note"]).round(2)

    # Rolling stock at cost (purchase gross = cost of goods in)
    running = stk["op_stock"]
    op_vals, cl_vals = [], []
    for _, row in merged.iterrows():
        op_vals.append(round(running, 2))
        cogs    = row["sale"] - row["profit"]   # cost of goods sold
        running = max(0, running + row["pur_gross"] - cogs)
        cl_vals.append(round(running, 2))

    merged["op_stock"] = op_vals
    merged["cl_stock"] = cl_vals

    # Calendar columns
    merged["dow"]     = merged["date"].dt.strftime("%a")
    merged["week"]    = "W" + merged["date"].dt.isocalendar().week \
                              .astype(str).str.zfill(2) \
                        + "-" + merged["date"].dt.year.astype(str)
    merged["month"]   = merged["date"].dt.strftime("%Y-%m")
    merged["quarter"] = "Q" + ((merged["date"].dt.month-1)//3+1).astype(str) \
                        + "-" + merged["date"].dt.year.astype(str)
    merged["year"]    = merged["date"].dt.year.astype(str)

    merged["margin_pct"]     = (merged["profit"] /
        merged["sale"].replace(0, float("nan")) * 100).fillna(0).round(2)
    merged["net_margin_pct"] = (merged["net_profit"] /
        merged["sale"].replace(0, float("nan")) * 100).fillna(0).round(2)
    merged["collection_pct"] = (merged["collection"] /
        merged["sale"].replace(0, float("nan")) * 100).fillna(0).round(2)

    for c in ["sale","purchase","pur_gross","pur_gst","collection",
              "credit_note","debit_note","profit","gross_profit","net_profit",
              "op_stock","cl_stock"]:
        if c in merged.columns: merged[c] = merged[c].round(2)

    merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")
    rows = merged.to_dict(orient="records")

    def tot(k): return sum(r.get(k, 0) for r in rows)
    print(f"\n  Days        : {len(rows):,}")
    if rows:
        print(f"  Range       : {rows[0]['date']} \u2192 {rows[-1]['date']}")
    print(f"  Sale        : {inr(tot('sale'))}")
    print(f"  Purchase    : {inr(tot('purchase'))}  "
          f"(gross {inr(tot('pur_gross'))} + GST {inr(tot('pur_gst'))})")
    print(f"  Profit      : {inr(tot('profit'))}")
    print(f"  Collection  : {inr(tot('collection'))}")
    print(f"  Credit notes: {inr(tot('credit_note'))}")
    print(f"  Debit notes : {inr(tot('debit_note'))}")
    print(f"  Op stock    : {inr(stk['op_stock'])}")
    print(f"  Cl stock    : {inr(stk['cl_stock'])}")
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# HTML
# ─────────────────────────────────────────────────────────────────────────────

def build_html(rows: list) -> str:
    return HTML_TEMPLATE.replace("__DATA__", json.dumps(rows, default=ser))


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Medikart — Day Book</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>
<style>
:root{
  --bg:#f7f7f5;--sf:#fff;--s2:#f1efe8;
  --bd:rgba(0,0,0,.1);--bd2:rgba(0,0,0,.2);
  --t1:#1a1a18;--t2:#5f5e5a;--t3:#888780;
  --blue:#185FA5;--blu-bg:#e6f1fb;--blu-t:#0c447c;
  --green:#1D9E75;--grn-bg:#e1f5ee;--grn-t:#085041;
  --amber:#BA7517;--amb-bg:#faeeda;--amb-t:#633806;
  --red:#A32D2D;--red-bg:#fcebeb;--red-t:#501313;
  --r:8px;--rl:12px;
}
@media(prefers-color-scheme:dark){:root{
  --bg:#1e1e1c;--sf:#2c2c2a;--s2:#252523;
  --bd:rgba(255,255,255,.1);--bd2:rgba(255,255,255,.2);
  --t1:#e8e6df;--t2:#b4b2a9;--t3:#73726c;
  --blu-bg:#0c2a4a;--blu-t:#85b7eb;
  --grn-bg:#04342c;--grn-t:#9fe1cb;
  --amb-bg:#412402;--amb-t:#fac775;
  --red-bg:#2d0f0f;--red-t:#f09595;
}}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;
  background:var(--bg);color:var(--t1);font-size:13px}
.topbar{background:var(--sf);border-bottom:.5px solid var(--bd);
  padding:10px 20px;display:flex;align-items:center;gap:14px;flex-wrap:wrap;
  position:sticky;top:0;z-index:100}
.logo{font-size:15px;font-weight:600;display:flex;align-items:center;gap:7px}
.pip{width:8px;height:8px;border-radius:50%;background:var(--blue)}
.spacer{flex:1}
/* controls */
.cbar{background:var(--sf);border-bottom:.5px solid var(--bd);
  padding:8px 20px;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.cl{font-size:11px;color:var(--t3);white-space:nowrap}
select,input[type=date]{padding:4px 8px;border:.5px solid var(--bd2);
  border-radius:var(--r);background:var(--s2);color:var(--t1);font-size:11px}
.bg{display:flex}
.bg .btn{border-radius:0;border-right:none}
.bg .btn:first-child{border-radius:var(--r) 0 0 var(--r)}
.bg .btn:last-child{border-radius:0 var(--r) var(--r) 0;border-right:.5px solid var(--bd2)}
.btn{padding:4px 12px;border:.5px solid var(--bd2);background:var(--s2);
  color:var(--t2);font-size:11px;cursor:pointer}
.btn:hover,.btn.active{background:var(--blu-bg);color:var(--blu-t)}
.btn-xl{padding:4px 14px;border:.5px solid var(--bd2);background:var(--s2);
  color:var(--t2);font-size:11px;cursor:pointer;border-radius:var(--r)}
.btn-xl:hover{background:var(--grn-bg);color:var(--grn-t)}
/* body */
.body{padding:14px 20px}
/* kpi strip */
.ks{display:grid;grid-template-columns:repeat(8,minmax(0,1fr));gap:7px;margin-bottom:12px}
.kc{background:var(--s2);border-radius:var(--r);padding:9px 11px}
.kl{font-size:9px;color:var(--t3);text-transform:uppercase;letter-spacing:.04em;margin-bottom:2px}
.kv{font-size:15px;font-weight:600;line-height:1.1}
.ks2{font-size:9px;margin-top:2px;color:var(--t3)}
.pos{color:var(--green)}.neg{color:var(--red)}.wn{color:var(--amber)}
/* chart */
.cc{background:var(--sf);border:.5px solid var(--bd);border-radius:var(--rl);
  padding:13px 15px;margin-bottom:12px}
.ch{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;margin-bottom:9px}
.ct{font-size:12px;font-weight:600}
.leg{display:flex;gap:10px;flex-wrap:wrap}
.li{display:flex;align-items:center;gap:4px;font-size:10px;color:var(--t3)}
.lsq{width:7px;height:7px;border-radius:2px}
.cw{position:relative;width:100%;height:230px}
/* table card */
.tc{background:var(--sf);border:.5px solid var(--bd);border-radius:var(--rl);padding:13px 15px}
.th{display:flex;align-items:center;gap:8px;margin-bottom:8px;flex-wrap:wrap}
.tt{font-size:12px;font-weight:600}
.ti{font-size:10px;color:var(--t3);margin-left:auto}
.srch{padding:4px 8px;border:.5px solid var(--bd2);border-radius:var(--r);
  background:var(--s2);color:var(--t1);font-size:11px;width:150px}
.tw{overflow-x:auto}
table{width:100%;border-collapse:collapse;min-width:900px}
thead th{padding:6px 8px;text-align:right;font-size:9px;font-weight:600;
  color:var(--t3);text-transform:uppercase;letter-spacing:.04em;
  border-bottom:.5px solid var(--bd2);background:var(--s2);
  cursor:pointer;white-space:nowrap;user-select:none;position:sticky;top:0}
thead th:first-child{text-align:left;min-width:90px}
thead th:hover{color:var(--t1)}
thead th.sa::after{content:" ▲";opacity:.6}
thead th.sd::after{content:" ▼";opacity:.6}
tbody tr{border-bottom:.5px solid var(--bd);cursor:pointer}
tbody tr:hover{background:var(--s2)}
tbody tr.xr{background:var(--blu-bg)!important}
tbody td{padding:5px 8px;color:var(--t1);text-align:right;font-size:11px;white-space:nowrap}
tbody td:first-child{text-align:left;font-weight:500}
.tot td{font-weight:600;border-top:.5px solid var(--bd2);background:var(--s2)!important;cursor:default}
.dr-tr{display:none}
.dr-tr.open{display:table-row}
.dr-cell{padding:0!important}
.dr-in{padding:7px 10px 7px 28px;background:var(--s2);border-bottom:.5px solid var(--bd)}
.dr-in table{min-width:0;font-size:10px}
.dr-in thead th{font-size:9px;background:transparent;position:static}
.pg{display:flex;align-items:center;gap:6px;margin-top:7px;font-size:10px;color:var(--t3)}
.pg button{padding:2px 7px;border:.5px solid var(--bd2);border-radius:var(--r);
  background:var(--s2);color:var(--t1);font-size:10px;cursor:pointer}
.pg button:hover{background:var(--blu-bg);color:var(--blu-t)}
.pg button:disabled{opacity:.3;cursor:default}
.badge{display:inline-block;padding:1px 5px;border-radius:8px;font-size:9px;font-weight:600}
.b-sun{background:var(--red-bg);color:var(--red-t)}
.b-sat{background:var(--amb-bg);color:var(--amb-t)}
@media(max-width:900px){.ks{grid-template-columns:repeat(4,1fr)}}
</style>
</head>
<body>

<div class="topbar">
  <div class="logo"><span class="pip"></span>Medikart</div>
  <span style="font-size:13px;color:var(--t2)">Day Book — Trading Summary</span>
  <div class="spacer"></div>
  <span style="font-size:10px;color:var(--t3)" id="dr"></span>
</div>

<div class="cbar">
  <!-- Granularity -->
  <span class="cl">View by</span>
  <div class="bg">
    <button class="btn active" onclick="setGrain('day',this)">Day</button>
    <button class="btn" onclick="setGrain('week',this)">Week</button>
    <button class="btn" onclick="setGrain('month',this)">Month</button>
    <button class="btn" onclick="setGrain('quarter',this)">Quarter</button>
    <button class="btn" onclick="setGrain('year',this)">Year</button>
  </div>
  <!-- Date range -->
  <span class="cl">From</span>
  <input type="date" id="df" onchange="run()">
  <span class="cl">To</span>
  <input type="date" id="dt" onchange="run()">
  <!-- Quick ranges -->
  <span class="cl">Quick</span>
  <select id="qr" onchange="qRange(this.value)">
    <option value="all" selected>All data</option>
    <option value="7">Last 7 days</option>
    <option value="14">Last 14 days</option>
    <option value="30">Last 30 days</option>
    <option value="90">Last 3 months</option>
    <option value="180">Last 6 months</option>
    <option value="365">Last year</option>
    <option value="fy">This financial year</option>
    <option value="lfy">Last financial year</option>
    <option value="cm">This month</option>
    <option value="lm">Last month</option>
  </select>
  <!-- Chart selector -->
  <span class="cl">Chart</span>
  <select id="cm" onchange="renderChart()">
    <option value="all">All metrics</option>
    <option value="sp">Sale + Purchase</option>
    <option value="profit">Profit</option>
    <option value="stock">Stock levels</option>
    <option value="collection">Collection</option>
    <option value="notes">CR / DB notes</option>
  </select>
  <div class="spacer"></div>
  <span style="font-size:10px;color:var(--t3)" id="rc"></span>
</div>

<div class="body">
  <div class="ks" id="kpis"></div>

  <div class="cc">
    <div class="ch">
      <div class="ct" id="cht"></div>
      <div class="leg" id="cleg"></div>
      <button class="btn-xl" onclick="exportChart()">Export chart data ↓</button>
    </div>
    <div class="cw"><canvas id="ch" role="img" aria-label="Day book chart">Day book</canvas></div>
  </div>

  <div class="tc">
    <div class="th">
      <div class="tt" id="tttl">Day-wise detail</div>
      <input class="srch" id="srch" placeholder="Search…" oninput="srchFn(this.value)">
      <div class="ti" id="tcnt"></div>
      <button class="btn-xl" onclick="exportTable()">Export to Excel ↓</button>
    </div>
    <div class="tw"><table>
      <thead id="thead"></thead>
      <tbody id="tbody"></tbody>
    </table></div>
    <div class="pg" id="pg"></div>
  </div>
</div>

<script>
const RAW=__DATA__;
let grain='day',filt=[],disp=[],sk='date',sd=-1,pg=0,mc=null;
const PS=50;
const GKEY={day:'date',week:'week',month:'month',quarter:'quarter',year:'year'};
const GLBL={day:'Date',week:'Week',month:'Month',quarter:'Quarter',year:'Year'};
const DRILL={year:'quarter',quarter:'month',month:'week',week:'day',day:null};

// ── utils ──────────────────────────────────────────────────────────────────
const inr=v=>{const a=Math.abs(parseFloat(v)||0),s=v<0?'-':'';
  if(a>=1e7)return s+'₹'+(a/1e5).toFixed(1)+'L';
  if(a>=1e5)return s+'₹'+(a/1e5).toFixed(2)+'L';
  if(a>=1e3)return s+'₹'+(a/1e3).toFixed(1)+'K';
  return s+'₹'+a.toFixed(0)};
const num=v=>(parseFloat(v)||0).toLocaleString('en-IN');
const pct=(a,b)=>b?(a/b*100).toFixed(1)+'%':'—';
const cc=v=>parseFloat(v)>=0?'pos':'neg';

// ── aggregation ────────────────────────────────────────────────────────────
function agg(rows,gk){
  const m={};
  rows.forEach(r=>{
    const k=r[gk];
    if(!m[k])m[k]={period:k,date:r.date,dow:r.dow||'',
      sale:0,sale_gst:0,sale_disc:0,
      purchase:0,pur_gross:0,pur_gst:0,pur_discount:0,pur_item_disc:0,pur_scm_amt:0,
      collection:0,credit_note:0,debit_note:0,
      profit:0,gross_profit:0,net_profit:0,
      bills:0,pur_bills:0,
      op_stock:r.op_stock,cl_stock:r.cl_stock,_days:0,_rows:[]};
    const x=m[k];
    ['sale','sale_gst','sale_disc',
     'purchase','pur_gross','pur_gst','pur_discount','pur_item_disc','pur_scm_amt',
     'collection','credit_note','debit_note',
     'profit','gross_profit','net_profit','bills','pur_bills']
      .forEach(c=>x[c]+=(parseFloat(r[c])||0));
    x.cl_stock=r.cl_stock; x._days++; x._rows.push(r);
  });
  return Object.values(m).map(x=>({...x,
    margin_pct:    x.sale?x.gross_profit/x.sale*100:0,
    net_margin_pct:x.sale?x.net_profit/x.sale*100:0,
    collection_pct:x.sale?x.collection/x.sale*100:0,
  }));
}

// ── quick range ────────────────────────────────────────────────────────────
function qRange(v){
  const today=new Date();
  let from='',to='';
  const fmt=d=>d.toISOString().substring(0,10);
  const fy_start=()=>{
    const m=today.getMonth();
    return new Date(m>=3?today.getFullYear():today.getFullYear()-1,3,1);
  };
  if(v==='all'){}
  else if(v==='cm'){from=fmt(new Date(today.getFullYear(),today.getMonth(),1));to=fmt(today);}
  else if(v==='lm'){
    const lm=new Date(today.getFullYear(),today.getMonth()-1,1);
    const le=new Date(today.getFullYear(),today.getMonth(),0);
    from=fmt(lm);to=fmt(le);}
  else if(v==='fy'){from=fmt(fy_start());to=fmt(today);}
  else if(v==='lfy'){
    const s=fy_start(); s.setFullYear(s.getFullYear()-1);
    const e=new Date(s.getFullYear()+1,2,31);
    from=fmt(s);to=fmt(e);}
  else{from=fmt(new Date(today-v*86400000));to=fmt(today);}
  document.getElementById('df').value=from;
  document.getElementById('dt').value=to;
  run();
}

// ── filter + run ───────────────────────────────────────────────────────────
function run(){
  const from=document.getElementById('df').value;
  const to=document.getElementById('dt').value;
  let rows=RAW;
  if(from) rows=rows.filter(r=>r.date>=from);
  if(to)   rows=rows.filter(r=>r.date<=to);
  filt=agg(rows,GKEY[grain]);
  filt.sort((a,b)=>a[sk]>b[sk]?sd:a[sk]<b[sk]?-sd:0);
  pg=0; disp=filt;
  document.getElementById('rc').textContent=
    filt.length.toLocaleString()+' '+(grain==='day'?'days':grain+'s');
  document.getElementById('tttl').textContent=GLBL[grain]+'-wise trading summary';
  renderKPIs(); renderChart(); renderHead(); renderTbl();
}

function setGrain(g,el){
  grain=g;
  document.querySelectorAll('.bg .btn').forEach(b=>b.classList.remove('active'));
  el.classList.add('active');
  run();
}

function srchFn(q){
  q=q.toLowerCase();
  disp=q?filt.filter(r=>JSON.stringify(r).toLowerCase().includes(q)):filt;
  pg=0; renderTbl();
}

// ── KPI strip ──────────────────────────────────────────────────────────────
function renderKPIs(){
  const t=filt.reduce((a,r)=>{
    ['sale','purchase','collection','credit_note','debit_note',
     'profit','gross_profit','net_profit','sale_disc','pur_discount','pur_gst','bills','pur_bills']
      .forEach(k=>a[k]=(a[k]||0)+(parseFloat(r[k])||0));
    return a;
  },{});
  const op=filt.length?filt[0].op_stock:0;
  const cl=filt.length?filt[filt.length-1].cl_stock:0;
  document.getElementById('kpis').innerHTML=[
    {l:'Sale',         v:inr(t.sale),        s:num(Math.round(t.bills))+' bills'},
    {l:'Purchase',     v:inr(t.purchase),     s:num(Math.round(t.pur_bills))+' bills · GST '+inr(t.pur_gst)},
    {l:'Gross profit', v:inr(t.profit),       s:pct(t.profit,t.sale)+' margin',c:'pos'},
    {l:'Collection',   v:inr(t.collection),   s:pct(t.collection,t.sale)+' of sales'},
    {l:'Sale discount', v:inr(t.sale_disc),    s:'CD + Scheme disc'},
    {l:'Pur discount',  v:inr(t.pur_discount), s:'discount received',c:'pos'},
    {l:'Gross profit', v:inr(t.gross_profit||0), s:'PRFT_AMT from TR lines',c:'pos'},
    {l:'Sale CN',      v:inr(t.credit_note),    s:'Customer returns',c:'neg'},
    {l:'Pur DN',       v:inr(t.debit_note),     s:'Supplier returns',c:'pos'},
    {l:'Net profit',   v:inr(t.net_profit||0),  s:'Gross - CN + DN',c:'pos'},
    {l:'Closing stock',v:inr(cl),             s:'Op: '+inr(op)},
  ].map(k=>`<div class="kc"><div class="kl">${k.l}</div>
    <div class="kv ${k.c||''}">${k.v}</div>
    <div class="ks2">${k.s}</div></div>`).join('');
}

// ── chart ──────────────────────────────────────────────────────────────────
const CC={sale:'#185FA5',purchase:'#E24B4A',profit:'#1D9E75',
  collection:'#534AB7',credit_note:'#BA7517',debit_note:'#888780',
  op_stock:'#85B7EB',cl_stock:'#5DCAA5'};
const CL={sale:'Sale',purchase:'Purchase',profit:'Profit',
  collection:'Collection',credit_note:'Credit note',debit_note:'Debit note',
  op_stock:'Opening stock',cl_stock:'Closing stock'};
const MSETS={
  all:['sale','purchase','profit','collection'],
  sp:['sale','purchase'],profit:['profit'],
  stock:['op_stock','cl_stock'],collection:['collection','sale'],
  notes:['credit_note','debit_note']};
const MTITLES={all:'Sale / Purchase / Profit / Collection',sp:'Sale vs Purchase',
  profit:'Profit trend',stock:'Opening vs Closing stock',
  collection:'Collection vs Sales',notes:'Credit & Debit notes'};

function renderChart(){
  const metric=document.getElementById('cm').value;
  const keys=MSETS[metric];
  const hasStock=keys.some(k=>k.includes('stock'));

  const datasets=keys.map(k=>{
    const isLine=['profit','collection'].includes(k)&&keys.length>1;
    return{label:CL[k],data:filt.map(r=>+((r[k]||0)/1000).toFixed(1)),
      backgroundColor:CC[k]+(isLine?'':'55'),borderColor:CC[k],
      borderWidth:isLine?2:1,type:isLine?'line':'bar',
      tension:.3,pointRadius:filt.length>60?0:2,
      borderDash:k==='purchase'?[4,2]:[],
      yAxisID:hasStock&&k.includes('stock')?'y2':'y',
      fill:false,order:isLine?1:2};
  });

  document.getElementById('cleg').innerHTML=keys.map(k=>
    `<span class="li"><span class="lsq" style="background:${CC[k]}"></span>${CL[k]}</span>`
  ).join('');
  document.getElementById('cht').textContent=MTITLES[metric];

  if(mc)mc.destroy();
  mc=new Chart(document.getElementById('ch'),{
    type:'bar',data:{labels:filt.map(r=>r.period),datasets},
    options:{responsive:true,maintainAspectRatio:false,
      interaction:{mode:'index',intersect:false},
      plugins:{legend:{display:false},
        tooltip:{callbacks:{label:c=>` ${c.dataset.label}: ₹${c.raw}K`}}},
      scales:{
        y:{grid:{color:'rgba(128,128,128,.1)'},ticks:{font:{size:9},callback:v=>'₹'+v+'K'}},
        y2:hasStock?{position:'right',grid:{drawOnChartArea:false},
          ticks:{font:{size:9},callback:v=>'₹'+v+'K'}}:{display:false},
        x:{ticks:{font:{size:9},autoSkip:true,maxTicksLimit:24,maxRotation:35}}
      }}
  });
}

// ── table ──────────────────────────────────────────────────────────────────
const COLS=[
  {k:'period',l:()=>GLBL[grain],r:false,
    f:(v,r)=>{const d=r.dow;
      const b=d==='Sun'?'<span class="badge b-sun">Sun</span>':
               d==='Sat'?'<span class="badge b-sat">Sat</span>':'';
      return`${v} ${b}`}},
  {k:'_days',  l:()=>grain==='day'?'':'Days',  r:true,  f:v=>grain==='day'?'':num(v)},
  {k:'sale',        l:()=>'Sale',         r:true, f:v=>inr(v)},
  {k:'sale_disc',   l:()=>'Sale disc',    r:true, f:v=>inr(v)},
  {k:'purchase',    l:()=>'Purchase',     r:true, f:v=>inr(v)},
  {k:'pur_gst',     l:()=>'GST (purch)',  r:true, f:v=>inr(v)},
  {k:'pur_discount',l:()=>'Pur disc',     r:true, f:v=>inr(v)},
  {k:'gross_profit', l:()=>'Gross profit', r:true,
    f:(v,r)=>`<span class="${cc(v)}">${inr(v)}</span> <span style="font-size:9px;color:var(--t3)">${(+r.margin_pct||0).toFixed(1)}%</span>`},
  {k:'credit_note',  l:()=>'Sale CN',      r:true, f:v=>`<span class="${parseFloat(v)>0?'neg':''}">${inr(v)}</span>`},
  {k:'debit_note',   l:()=>'Pur DN',       r:true, f:v=>`<span class="${parseFloat(v)>0?'pos':''}">${inr(v)}</span>`},
  {k:'net_profit',   l:()=>'Net profit',   r:true, f:v=>`<span class="${cc(v)}" style="font-weight:600">${inr(v)}</span>`},
  {k:'collection',  l:()=>'Collection',   r:true,
    f:(v,r)=>`${inr(v)} <span style="font-size:9px;color:var(--t3)">${(+r.collection_pct||0).toFixed(1)}%</span>`},

  {k:'op_stock',    l:()=>'Op stock',     r:true, f:v=>inr(v)},
  {k:'cl_stock',    l:()=>'Cl stock',     r:true, f:v=>inr(v)},
  {k:'bills',       l:()=>'Bills',        r:true, f:v=>num(Math.round(v))},
];

function renderHead(){
  const vis=COLS.filter(c=>c.l());
  document.getElementById('thead').innerHTML='<tr>'+
    vis.map(c=>{const lbl=c.l();if(!lbl)return'';
      const cls=sk===c.k?(sd===1?'sa':'sd'):'';
      return`<th class="${cls}" onclick="sortBy('${c.k}')">${lbl}</th>`;
    }).join('')+
    (DRILL[grain]?'<th style="width:18px"></th>':'')+
  '</tr>';
}

function renderTbl(){
  document.getElementById('tcnt').textContent=disp.length.toLocaleString()+' rows';
  const vis=COLS.filter(c=>c.l());
  const hasDrill=!!DRILL[grain];
  const rows=disp.slice(pg*PS,(pg+1)*PS);
  const tot=disp.reduce((a,r)=>{
    ['sale','sale_gst','sale_disc',
     'purchase','pur_gross','pur_gst','pur_discount','pur_item_disc','pur_scm_amt',
     'collection','credit_note','debit_note',
     'profit','gross_profit','net_profit','bills','pur_bills']
      .forEach(k=>a[k]=(a[k]||0)+(parseFloat(r[k])||0));
    a._days=(a._days||0)+(r._days||1); return a;
  },{});
  tot.period='TOTAL';tot.dow='';
  tot.margin_pct=    tot.sale?tot.gross_profit/tot.sale*100:0;
  tot.net_margin_pct=tot.sale?tot.net_profit/tot.sale*100:0;
  tot.collection_pct=tot.sale?tot.collection/tot.sale*100:0;
  tot.op_stock=disp.length?disp[0].op_stock:0;
  tot.cl_stock=disp.length?disp[disp.length-1].cl_stock:0;

  const mkRow=(r,isTot)=>{
    const cells=vis.map(c=>{const lbl=c.l();if(!lbl)return'';
      const fmt=c.f?c.f(r[c.k],r):(r[c.k]==null?'—':r[c.k]);
      return`<td style="text-align:${c.r?'right':'left'}">${fmt}</td>`;
    }).join('');
    const drill=hasDrill&&!isTot
      ?`<td style="text-align:center;color:var(--blue);font-weight:600;font-size:13px"
          onclick="event.stopPropagation();tgl('${(r.period||'').replace(/[^a-zA-Z0-9]/g,'-')}')">+</td>`
      :(hasDrill?'<td></td>':'');
    return`<tr class="${isTot?'tot':''}" ${!isTot?`onclick="tgl('${(r.period||'').replace(/[^a-zA-Z0-9]/g,'-')}')"`:''}>
      ${cells}${drill}</tr>`;
  };

  let html=rows.map(r=>{
    const id='dr-'+(r.period||'').replace(/[^a-zA-Z0-9]/g,'-');
    const drill=hasDrill?`<tr class="dr-tr" id="${id}">
      <td colspan="${vis.length+1}" class="dr-cell">
        <div class="dr-in">${drillHtml(r)}</div>
      </td></tr>`:'';
    return mkRow(r,false)+drill;
  }).join('');
  html+=mkRow(tot,true);
  document.getElementById('tbody').innerHTML=html;
  renderPg();
}

function drillHtml(row){
  const ng=DRILL[grain];
  if(!ng||!row._rows||!row._rows.length)return'<em style="color:var(--t3)">No detail</em>';
  const da=agg(row._rows,GKEY[ng]).sort((a,b)=>a.period>b.period?1:-1);
  const cols=['period','sale','sale_disc','purchase','pur_gst','pur_discount',
              'gross_profit','credit_note','debit_note','net_profit','collection','bills'];
  const head=cols.map(c=>`<th style="text-align:${c==='period'?'left':'right'}">${
    c==='period'?GLBL[ng]:c.replace(/_/g,' ')}</th>`).join('');
  const rows=da.map(r=>`<tr>
    <td style="text-align:left;font-weight:500">${r.period}</td>
    <td style="text-align:right">${inr(r.sale)}</td>
    <td style="text-align:right">${inr(r.sale_disc)}</td>
    <td style="text-align:right">${inr(r.purchase)}</td>
    <td style="text-align:right">${inr(r.pur_gst)}</td>
    <td style="text-align:right">${inr(r.pur_discount)}</td>
    <td style="text-align:right;color:${(r.gross_profit||0)>=0?'var(--green)':'var(--red)'}">${inr(r.gross_profit||0)}</td>
    <td style="text-align:right;color:var(--red)">${inr(r.credit_note)}</td>
    <td style="text-align:right;color:var(--green)">${inr(r.debit_note)}</td>
    <td style="text-align:right;font-weight:600;color:${(r.net_profit||0)>=0?'var(--green)':'var(--red)'}">${inr(r.net_profit||0)}</td>
    <td style="text-align:right">${inr(r.collection)}</td>
    <td style="text-align:right">${num(Math.round(r.bills))}</td>
  </tr>`).join('');
  return`<table><thead><tr>${head}</tr></thead><tbody>${rows}</tbody></table>`;
}

function tgl(id){
  const el=document.getElementById('dr-'+id);if(!el)return;
  const open=el.classList.contains('open');
  document.querySelectorAll('.dr-tr.open').forEach(e=>e.classList.remove('open'));
  document.querySelectorAll('tbody tr.xr').forEach(e=>e.classList.remove('xr'));
  if(!open){el.classList.add('open');el.previousElementSibling?.classList.add('xr');}
}

function sortBy(k){
  sd=sk===k?sd*-1:-1;sk=k;
  filt.sort((a,b)=>a[k]>b[k]?sd:a[k]<b[k]?-sd:0);
  disp=filt;pg=0;renderHead();renderTbl();
}

function renderPg(){
  const tot=Math.ceil(disp.length/PS)||1;
  const el=document.getElementById('pg');
  el.innerHTML=`<button id="pp" ${pg===0?'disabled':''}>← Prev</button>
    <span style="padding:0 5px">Page ${pg+1}/${tot}</span>
    <button id="pn" ${pg>=tot-1?'disabled':''}>Next →</button>
    <span> · ${disp.length.toLocaleString()} rows</span>`;
  document.getElementById('pp')?.addEventListener('click',()=>{pg--;renderTbl();});
  document.getElementById('pn')?.addEventListener('click',()=>{pg++;renderTbl();});
}

// ── Export to Excel (SheetJS) ──────────────────────────────────────────────
function exportTable(){
  const from=document.getElementById('df').value||'all';
  const to=document.getElementById('dt').value||'all';
  const fname=`Medikart_DayBook_${grain}_${from}_${to}.xlsx`;

  // Build clean flat data for export (no HTML)
  const exportData=disp.map(r=>({
    [GLBL[grain]]:  r.period,
    'Days':         r._days||1,
    'Sale (₹)':     parseFloat(r.sale)||0,
    'Sale disc (₹)': parseFloat(r.sale_disc)||0,
    'Pur disc (₹)':  parseFloat(r.pur_discount)||0,
    'Purchase (₹)': parseFloat(r.purchase)||0,
    'GST-Purch (₹)':parseFloat(r.pur_gst)||0,
    'Profit (₹)':   parseFloat(r.profit)||0,
    'Margin %':     parseFloat(r.margin_pct)||0,
    'Collection (₹)':parseFloat(r.collection)||0,
    'Coll % of Sale':parseFloat(r.collection_pct)||0,
    'Gross profit (₹)': parseFloat(r.gross_profit)||0,
    'Gross margin %':   parseFloat(r.margin_pct)||0,
    'Sale CN (₹)':      parseFloat(r.credit_note)||0,
    'Pur DN (₹)':       parseFloat(r.debit_note)||0,
    'Net profit (₹)':   parseFloat(r.net_profit)||0,
    'Net margin %':     parseFloat(r.net_margin_pct)||0,
    'Op Stock (₹)': parseFloat(r.op_stock)||0,
    'Cl Stock (₹)': parseFloat(r.cl_stock)||0,
    'Bills':        Math.round(parseFloat(r.bills)||0),
    'Pur Bills':    Math.round(parseFloat(r.pur_bills)||0),
  }));

  // Add totals row
  const tot={};
  exportData.forEach(r=>Object.keys(r).forEach(k=>{
    if(typeof r[k]==='number') tot[k]=(tot[k]||0)+r[k];
  }));
  tot[GLBL[grain]]='TOTAL';
  exportData.push(tot);

  const ws=XLSX.utils.json_to_sheet(exportData);

  // Column widths
  ws['!cols']=[{wch:14},{wch:6},{wch:14},{wch:12},{wch:14},{wch:14},
               {wch:12},{wch:9},{wch:14},{wch:12},{wch:12},{wch:12},
               {wch:14},{wch:14},{wch:7},{wch:8}];

  const wb=XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb,ws,'Day Book');
  XLSX.writeFile(wb,fname);
}

function exportChart(){
  const metric=document.getElementById('cm').value;
  const keys=MSETS[metric];
  const fname=`Medikart_Chart_${grain}_${metric}.xlsx`;
  const exportData=filt.map(r=>{
    const row={[GLBL[grain]]:r.period};
    keys.forEach(k=>row[CL[k]]=parseFloat(r[k])||0);
    return row;
  });
  const ws=XLSX.utils.json_to_sheet(exportData);
  const wb=XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb,ws,'Chart Data');
  XLSX.writeFile(wb,fname);
}

// ── init ───────────────────────────────────────────────────────────────────
function init(){
  const dates=RAW.map(r=>r.date).sort();
  if(dates.length)
    document.getElementById('dr').textContent=
      `Data: ${dates[0]}  to  ${dates[dates.length-1]}`;

  // Default: last 90 days
  const today=new Date();
  const from=new Date(today-90*86400000);
  document.getElementById('df').value=from.toISOString().substring(0,10);
  document.getElementById('dt').value=today.toISOString().substring(0,10);
  document.getElementById('qr').value='90';
  run();
}
document.addEventListener('DOMContentLoaded',init);
</script>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run(folder_arg: str):
    folder = Path(folder_arg)
    rows   = build_daybook(folder)

    if not rows:
        print("\nNo rows generated — check DBF files have records.\n")
        return

    json_path = folder / "medikart_daybook.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, default=ser)
    print(f"\n  JSON  : {json_path}  ({json_path.stat().st_size//1024} KB)")

    html      = build_html(rows)
    html_path = folder / "medikart_daybook.html"
    html_path.write_text(html, encoding="utf-8")
    print(f"  HTML  : {html_path}  ({html_path.stat().st_size//1024} KB)")
    print(f"\nOpen medikart_daybook.html in any browser — works fully offline.\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Medikart Day Book Generator v2")
    ap.add_argument("--folder", required=True,
                    help='DBF folder e.g. "D:\\CAREW"')
    args = ap.parse_args()
    run(args.folder)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run(folder_arg: str):
    folder = Path(folder_arg)
    rows   = build_daybook(folder)

    if not rows:
        print("\nNo rows generated — check DBF files have records.\n")
        return

    json_path = folder / "medikart_daybook.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, default=ser)
    print(f"\n  JSON  : {json_path}  ({json_path.stat().st_size//1024} KB)")

    html      = build_html(rows)
    html_path = folder / "medikart_daybook.html"
    html_path.write_text(html, encoding="utf-8")
    print(f"  HTML  : {html_path}  ({html_path.stat().st_size//1024} KB)")
    print(f"\nOpen medikart_daybook.html in any browser — works fully offline.\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Medikart Day Book Generator v3")
    ap.add_argument("--folder", required=True,
                    help='DBF folder e.g. "D:\\CAREW"')
    args = ap.parse_args()
    run(args.folder)
