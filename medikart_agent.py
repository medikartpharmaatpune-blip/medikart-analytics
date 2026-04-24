"""
medikart_agent.py  —  Medikart Data Agent (runs on Windows Server)
===================================================================
Reads all DBF files, builds JSON data files, uploads to GCP Cloud Storage.
Run as a Windows Scheduled Task every 15 minutes.

Setup
  py -3.12 -m pip install dbfread pandas google-cloud-storage
  py -3.12 medikart_agent.py --folder "D:\\CAREW" --once
  (then set up as a scheduled task for every 15 minutes)

Scheduled task command:
  py -3.12 C:\\Users\\Administrator\\MEDIKART\\medikart_agent.py --folder "D:\\CAREW"
"""

import argparse, json, re, time, datetime, logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("medikart_agent.log", encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)

try:
    from dbfread import DBF
    import pandas as pd
    from google.cloud import storage
except ImportError:
    print("Run:  py -3.12 -m pip install dbfread pandas google-cloud-storage")
    raise

PROJECT_ID  = "medikart-494016"
BUCKET_NAME = "medikart-494016-data"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers (same as daybook script)
# ─────────────────────────────────────────────────────────────────────────────

def load_table(folder: Path, name: str) -> pd.DataFrame:
    hits = [f for f in folder.iterdir()
            if f.is_file() and f.stem.upper() == name.upper()
            and f.suffix.lower() == ".dbf"]
    if not hits:
        log.warning(f"  {name}.DBF not found")
        return pd.DataFrame()
    try:
        recs = list(DBF(str(hits[0]), load=True, encoding="utf-8",
                        ignore_missing_memofile=True))
        df = pd.DataFrame(recs)
        log.info(f"  Loaded  {name:<18} {len(df):>8,} rows")
        return df
    except Exception as e:
        log.error(f"  {name}: {e}")
        return pd.DataFrame()


def load_monthly(folder: Path) -> pd.DataFrame:
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
    return pd.concat(frames, ignore_index=True)


def n(s) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0)


def d(s) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def ser(o):
    if hasattr(o, "isoformat"): return o.isoformat()
    if hasattr(o, "item"):      return o.item()
    return str(o)


def col(df, name, default=0):
    if name in df.columns: return df[name]
    return pd.Series([default]*len(df), index=df.index)


# ─────────────────────────────────────────────────────────────────────────────
# Data builders
# ─────────────────────────────────────────────────────────────────────────────

def build_daybook(folder: Path) -> list:
    """Reuse the same logic as medikart_daybook.py."""
    # Import from the existing daybook script
    import sys
    sys.path.insert(0, str(folder.parent / "MEDIKART"))
    try:
        import medikart_daybook as db
        rows = db.build_daybook(folder)
        return rows
    except Exception as e:
        log.warning(f"Could not import medikart_daybook: {e} — building inline")

    # Inline fallback (abbreviated)
    trcshr    = load_table(folder, "TRCSHR")
    purch     = load_table(folder, "PURCHTRAN")
    crdb      = load_table(folder, "CRDBTR")
    allsale   = load_table(folder, "ALLSALE")
    stock     = load_table(folder, "STOCK")
    sale_lines= load_monthly(folder)

    frames = []
    if not sale_lines.empty:
        sl = sale_lines.copy()
        for c in ["QTY","RATE","PRFT_AMT","SGSTAMT","CGSTAMT","IGSTAMT","CESSAMT","CDAMT","DISC_SCM"]:
            sl[c] = n(col(sl,c))
        sl["_dt"] = d(col(sl,"TR_DATE"))
        sl["SALE"] = sl["QTY"]*sl["RATE"]
        sl["SALE_DISC"] = sl["CDAMT"]+sl["DISC_SCM"]
        sl["BILL_KEY"] = sl["VOU_NO"].astype(str)+"_"+col(sl,"VOU_TYPE","").astype(str)
        sl = sl[sl["_dt"].notna()]
        g = sl.groupby(sl["_dt"].dt.date).agg(
            sale=("SALE","sum"), profit=("PRFT_AMT","sum"),
            sale_disc=("SALE_DISC","sum"), bills=("BILL_KEY","nunique")
        ).reset_index().rename(columns={"_dt":"date"})
        g["date"] = pd.to_datetime(g["date"])
        frames.append(g)

    if not frames:
        return []

    merged = frames[0]
    merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")
    return merged.to_dict(orient="records")


def build_products(folder: Path) -> dict:
    """Product-level data: sales, stock, margins, expiry."""
    sale_lines = load_monthly(folder)
    stock      = load_table(folder, "STOCK")
    purch      = load_table(folder, "PURCHTRAN")

    # Product name lookup
    product    = load_table(folder, "PRODUCT")
    lkp = {}
    if not product.empty:
        for _, r in product.iterrows():
            # Prefer PRODNM (longer, actual product name) over PROD_NAME
            name = str(r.get("PRODNM") or r.get("PROD_NAME") or "").strip()
            if not name: continue
            # Index by PROD_NO (primary key used in STOCK and TR_MONTHLY)
            pno = str(r.get("PROD_NO") or "").strip().upper()
            if pno: lkp[pno] = name
            # Also index by PRODID as fallback
            try:
                pid = int(r.get("PRODID") or 0)
                if pid: lkp[pid] = name
            except: pass

    # Company lookup
    company = load_table(folder, "COMPANY")
    comp_lkp = {}
    if not company.empty:
        for _, r in company.iterrows():
            cname = str(r.get("COMP_NAME") or "").strip()
            try:
                cid = int(r.get("COMPID") or r.get("COMP_NO") or 0)
                if cid: comp_lkp[cid] = cname
            except: pass

    result = {"generated": datetime.datetime.now().isoformat()}

    # ── Top selling products ─────────────────────────────
    if not sale_lines.empty:
        sl = sale_lines.copy()
        for c in ["QTY","RATE","PRFT_AMT","CDAMT"]:
            sl[c] = n(col(sl,c))
        sl["SALE_VAL"] = sl["QTY"]*sl["RATE"]
        sl["PRODID"]   = n(col(sl,"PRODID"))
        sl["PROD_NO"]  = col(sl,"PROD_NO","").astype(str).str.strip().str.upper()

        def get_name(r):
            # TR_MONTHLY links via PROD_NO (string key)
            pno = str(r.get("PROD_NO","")).strip().upper()
            pid = int(r.get("PRODID") or 0)
            return lkp.get(pno) or lkp.get(pid) or pno or "?"

        sl["PROD_NAME"] = sl.apply(get_name, axis=1)
        grp = sl.groupby("PROD_NAME").agg(
            qty=("QTY","sum"), sale_val=("SALE_VAL","sum"),
            profit=("PRFT_AMT","sum"), cd_amt=("CDAMT","sum")
        ).reset_index()
        grp["margin_pct"] = (grp["profit"]/grp["sale_val"].replace(0,float("nan"))*100).fillna(0).round(2)
        grp = grp.sort_values("sale_val", ascending=False)
        result["top_products"] = grp.head(100).round(2).to_dict(orient="records")
        result["total_products_sold"] = int((grp["qty"]>0).sum())

    # ── Stock status ─────────────────────────────────────
    if not stock.empty:
        st = stock.copy()
        for c in ["CL_STK","OP_STK","PUR_RATE","MRP","STK_IN","SL_STK"]:
            st[c] = n(col(st,c))
        st["PROD_NO"]  = col(st,"PROD_NO","").astype(str).str.strip().str.upper()
        st["PRODID"]   = n(col(st,"PRODID"))
        st["EXPIRY"]   = col(st,"EXPIRY","").astype(str).str.strip()
        st["D_EXPIRY"] = d(col(st,"D_EXPIRY"))
        st["STK_HOLD"] = col(st,"STK_HOLD",False)
        st["BATCH"]    = col(st,"PR_BATCHNO","").astype(str).str.strip()
        st["GODCODE"]  = col(st,"GODCODE","").astype(str).str.strip()
        st["LSTSALE_DT"]= d(col(st,"LSTSALE_DT"))

        def get_prod_name(r):
            # STOCK links via PROD_NO (PRODID is null in STOCK)
            pno = str(r.get("PROD_NO","")).strip().upper()
            pid = int(r.get("PRODID") or 0)
            return lkp.get(pno) or lkp.get(pid) or pno or "?"

        st["PROD_NAME"] = st.apply(get_prod_name, axis=1)
        st["cl_val"]    = (st["CL_STK"]*st["PUR_RATE"]).round(2)
        st["cl_val_mrp"]= (st["CL_STK"]*st["MRP"]).round(2)

        today = pd.Timestamp.now()

        # Near expiry (within 90 days) and expired
        near_expiry = st[st["D_EXPIRY"].notna() & (st["CL_STK"]>0)].copy()
        near_expiry["days_to_expiry"] = (near_expiry["D_EXPIRY"]-today).dt.days
        near_expiry = near_expiry[near_expiry["days_to_expiry"]<=90].copy()
        near_expiry = near_expiry.sort_values("days_to_expiry")
        result["near_expiry"] = near_expiry[[
            "PROD_NAME","BATCH","EXPIRY","days_to_expiry",
            "CL_STK","MRP","cl_val_mrp","GODCODE"
        ]].rename(columns={
            "PROD_NAME":"product","BATCH":"batch","EXPIRY":"expiry",
            "CL_STK":"qty","MRP":"mrp","cl_val_mrp":"value","GODCODE":"godown"
        }).head(200).to_dict(orient="records")

        # Slow/non-moving (no sale in last 60 days, has stock)
        cutoff_60 = today - pd.Timedelta(days=60)
        slow = st[(st["CL_STK"]>0) &
                  (st["LSTSALE_DT"].isna() | (st["LSTSALE_DT"]<cutoff_60))].copy()
        slow["days_since_sale"] = (today-slow["LSTSALE_DT"]).dt.days.fillna(9999).astype(int)
        slow = slow.sort_values("cl_val", ascending=False)
        result["slow_moving"] = slow[[
            "PROD_NAME","BATCH","CL_STK","PUR_RATE","cl_val","days_since_sale","LSTSALE_DT"
        ]].rename(columns={
            "PROD_NAME":"product","BATCH":"batch","CL_STK":"qty",
            "PUR_RATE":"pur_rate","cl_val":"value","LSTSALE_DT":"last_sale"
        }).head(200).to_dict(orient="records")

        # Stock summary
        result["stock_summary"] = {
            "total_batches":  int(len(st)),
            "total_cl_value": round(float(st["cl_val"].sum()), 2),
            "total_cl_mrp":   round(float(st["cl_val_mrp"].sum()), 2),
            "on_hold":        int((st["STK_HOLD"]==True).sum()),
            "zero_stock":     int((st["CL_STK"]<=0).sum()),
            "negative_stock": int((st["CL_STK"]<0).sum()),
            "near_expiry_count": len(result.get("near_expiry",[])),
            "slow_moving_count": len(result.get("slow_moving",[])),
        }

        # Full stock list for availability lookup
        avail = st[st["CL_STK"]>0][[
            "PROD_NAME","BATCH","EXPIRY","CL_STK","MRP","PUR_RATE","cl_val","GODCODE"
        ]].rename(columns={
            "PROD_NAME":"product","BATCH":"batch","EXPIRY":"expiry",
            "CL_STK":"qty","MRP":"mrp","PUR_RATE":"pur_rate",
            "cl_val":"value","GODCODE":"godown"
        }).sort_values("product")
        result["stock_availability"] = avail.to_dict(orient="records")

    return result


def build_gst(folder: Path) -> dict:
    """GST breakdowns from sale lines and purchases."""
    sale_lines = load_monthly(folder)
    purch      = load_table(folder, "PURCHTRAN")

    result = {"generated": datetime.datetime.now().isoformat()}

    if not sale_lines.empty:
        sl = sale_lines.copy()
        sl["_dt"] = d(col(sl,"TR_DATE"))
        for c in ["SGSTAMT","CGSTAMT","IGSTAMT","CESSAMT","QTY","RATE"]:
            sl[c] = n(col(sl,c))
        sl["SALE_VAL"] = sl["QTY"]*sl["RATE"]
        sl = sl[sl["_dt"].notna()]
        g = sl.groupby(sl["_dt"].dt.strftime("%Y-%m")).agg(
            sale=("SALE_VAL","sum"),
            sgst=("SGSTAMT","sum"), cgst=("CGSTAMT","sum"),
            igst=("IGSTAMT","sum"), cess=("CESSAMT","sum")
        ).reset_index().rename(columns={"_dt":"month"})
        g["total_gst"] = g["sgst"]+g["cgst"]+g["igst"]+g["cess"]
        result["sale_gst_monthly"] = g.round(2).to_dict(orient="records")

    if not purch.empty:
        pt = purch.copy()
        pt["_dt"] = d(col(pt,"BILL_DT"))
        for c in ["SGSTAMT","CGSTAMT","IGSTAMT","CESSAMT","QTY","PR_TRATE"]:
            pt[c] = n(col(pt,c))
        pt["PUR_VAL"] = pt["QTY"]*pt["PR_TRATE"]
        pt = pt[pt["_dt"].notna()]
        g2 = pt.groupby(pt["_dt"].dt.strftime("%Y-%m")).agg(
            purchase=("PUR_VAL","sum"),
            sgst=("SGSTAMT","sum"), cgst=("CGSTAMT","sum"),
            igst=("IGSTAMT","sum")
        ).reset_index().rename(columns={"_dt":"month"})
        g2["total_gst"] = g2["sgst"]+g2["cgst"]+g2["igst"]
        result["purchase_gst_monthly"] = g2.round(2).to_dict(orient="records")

    return result


def build_outstanding(folder: Path) -> dict:
    """Customer outstanding balances."""
    statment = load_table(folder, "STATMENT")
    account  = load_table(folder, "ACCOUNT")

    lkp = {}
    if not account.empty:
        for _, r in account.iterrows():
            name = str(r.get("AC_NAME") or "").strip()
            try:
                aid = int(r.get("ACCOID") or r.get("AC_NO") or 0)
                if aid: lkp[aid] = name
            except: pass

    result = {"generated": datetime.datetime.now().isoformat()}

    if not statment.empty:
        st = statment.copy()
        for c in ["DEBIT","AMT_RCD","AMT_BAL","DISCOUNT"]:
            st[c] = n(col(st,c))
        st["ACCOID"] = n(col(st,"ACCOID"))
        st["AC_NAME"] = st["ACCOID"].apply(
            lambda x: lkp.get(int(x), f"AC#{int(x)}"))
        result["outstanding"] = st[[
            "AC_NAME","DEBIT","AMT_RCD","AMT_BAL","DISCOUNT"
        ]].sort_values("AMT_BAL", ascending=False).round(2).to_dict(orient="records")
        result["total_outstanding"] = round(float(st["AMT_BAL"].sum()), 2)
        result["total_receivable"]  = round(float(st["DEBIT"].sum()), 2)
        result["total_collected"]   = round(float(st["AMT_RCD"].sum()), 2)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Upload to GCP Cloud Storage
# ─────────────────────────────────────────────────────────────────────────────

def upload(data, filename: str):
    try:
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(BUCKET_NAME)
        blob   = bucket.blob(filename)
        blob.upload_from_string(
            json.dumps(data, default=ser, indent=2),
            content_type="application/json"
        )
        log.info(f"  Uploaded  {filename}")
    except Exception as e:
        log.error(f"  Failed to upload {filename}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run(folder: Path):
    log.info(f"=== Medikart Agent  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
    log.info(f"Folder  : {folder}")
    log.info(f"Bucket  : {BUCKET_NAME}")

    try:
        # Day book
        log.info("Building daybook...")
        rows = build_daybook(folder)
        upload(rows, "daybook.json")

        # Products + stock
        log.info("Building products & stock...")
        products = build_products(folder)
        upload(products, "products.json")

        # GST
        log.info("Building GST...")
        gst = build_gst(folder)
        upload(gst, "gst.json")

        # Outstanding
        log.info("Building outstanding...")
        outstanding = build_outstanding(folder)
        upload(outstanding, "outstanding.json")

        # Metadata
        meta = {
            "last_updated": datetime.datetime.now().isoformat(),
            "folder":       str(folder),
            "files":        ["daybook.json","products.json","gst.json","outstanding.json"],
        }
        upload(meta, "meta.json")

        log.info("=== Done ===")

    except Exception as e:
        log.error(f"Agent error: {e}", exc_info=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Medikart Data Agent")
    ap.add_argument("--folder", required=True, help='DBF folder e.g. "D:\\CAREW"')
    ap.add_argument("--once",   action="store_true",
                    help="Run once and exit (default: run every 15 min)")
    ap.add_argument("--interval", type=int, default=15,
                    help="Refresh interval in minutes (default 15)")
    args = ap.parse_args()

    folder = Path(args.folder)
    if not folder.exists():
        print(f"Folder not found: {folder}"); exit(1)

    if args.once:
        run(folder)
    else:
        log.info(f"Running every {args.interval} minutes. Ctrl+C to stop.")
        while True:
            run(folder)
            log.info(f"Sleeping {args.interval} minutes...")
            time.sleep(args.interval * 60)
