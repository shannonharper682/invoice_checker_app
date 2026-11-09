"""Belgium Invoice Checker - Demo App v1.0
Streamlit app replicating Invoice Checker Belgium v2 notebook logic.
Tabs: Upload Invoices | Run Check | Results.
All data in ms_vulcan_gpgtopm_plab.belgium_app_demo_v1.
"""
import os, io, time, uuid, calendar, re, json, hashlib
from datetime import date, datetime, timedelta
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.sql import StatementParameterListItem

SCHEMA = "ms_vulcan_gpgtopm_plab.belgium_app_demo_v1"
FLAG_PCT = 1.0  # v1.13.1: back to the 1 % flag tolerance (the 1 EUR rule of v1.11.0 raised too many ~0 % flags, e.g. October 2026)

# v1.14.0: three flag levels, taken from the flag text:
#   issue     (red)    - numbers do not match                      e.g. "QTY", "price mismatch [Firm]"
#   technical (orange) - data missing / incomplete, what CAN be checked matches   "TECHNICAL: ..."
#   notice    (yellow) - something unusual on the invoice, numbers match            "NOTICE: ..."
FLAG_COLOURS = {"issue": "#ffcdd2", "technical": "#ffe0b2", "notice": "#fff9c4"}
def _flag_level(t):
    t=str(t or "").replace("\u26a0\ufe0f","").strip().upper()
    if not t: return ""
    if t.startswith("TECHNICAL"): return "technical"
    if t.startswith("NOTICE"): return "notice"
    return "issue"
AI_MODEL = os.environ.get("AI_MODEL", "databricks-claude-sonnet-4-5")  # v1.9.0: Ask AI (called via ai_query on the SQL warehouse)

# --- Version: bump BASE_VERSION on each release. Dev app (name ends with -dev) shows the "-dev" suffix. ---
BASE_VERSION = "1.18.3"
APP_NAME = os.environ.get("DATABRICKS_APP_NAME", "")
IS_DEV = APP_NAME.endswith("-dev")
APP_VERSION = BASE_VERSION + ("-dev" if IS_DEV else "")
VOLUME = "/Volumes/ms_vulcan_gpgtopm_plab/belgium_app_demo_v1/invoices"
DISPATCH_TABLE = "ms_atlas.dispatchnatgas_standard.hourly_v2latest"
PRICE_TABLE = "ms_atlas.ms07splitprice_standard.eex_v1latest"  # v1.14.1: was eex_v1r0 (same EEX ZTP Day Index; both end at publication 2026-07-09 as of Nov 2026)
ENDURAST_TABLE = "ms_atlas.endurast_raw.hourly_v1latest"
COUNTRY = "BE"
DOMESTIC_LOCATIONS = ("TESSENDERLO", "ANTWERP JUPITER")
HUB_INTERNAL_LOCATIONS = ("ZBHUBEE", "ZBHUBFL")
QTY_MULTIPLIER = 0.0008
TRADING_FEE_RATE = 0.00195
# v1.14.2: EEX ZTP daily spot index (EGSI). The old series Eex_EuropeanGasSpotIndex / 'ZTP,Day' / Index stops at
# publication 2026-07-09; EEX_NaturalGas_EGSI / 'ZTP,EGSI_D' / Price is identical on every overlapping day
# (Sep 2025 - Jul 2026, 0.0000 difference), continues after July and matches all 31 July 2026 bill prices.
PRICE_DATA_PACKAGE = "EEX_NaturalGas_EGSI"
PRICE_KEYS = "ZTP,EGSI_D"
PRICE_COLUMN = "Price"
# Invoice location -> Endur ng_location_id. Picks up every Endur name variant of a site
# (e.g. VIP_BENE_FLX and VIP_BENE_FLX-N-X both = BE.FLX.VIPBENE). Direction is NEVER taken from the
# location: it comes from the deal's instrument_type_name (COMM-CAP-ENTRY / COMM-CAP-EXIT).
LOCATION_MAP = {
    "ZPT":"BE.FLX.ZBEEEntry",
    "Zeebrugge":"BE.FLX.ZBHUBEEEntry-Exit",
    "VIP THE-ZTP":"BE.VIP.THE.ZTP",
    "VIP BENE":"BE.FLX.VIPBENE",
    "Virtualys":"BE.FLX.BL",
}
# Timing rule for the Endur-only check (unchanged from the old route list, where every Entry route was
# 'seasonal' and every Exit route 'longterm'): Exit is only checked for the invoice month; Entry for every
# billing month on the bill.
# v1.18.0: only capacity with Fluxys as counterparty (external legal entity) is billed by Fluxys. Secondary trades with
# another shipper (transfer of use, e.g. PRI_SEC buy from Engie at VIP BENE) are settled with that shipper and left out.
# Assignments are booked against Fluxys in Endur (e.g. 'Assignment sell Engie GY21-25'), so they stay in.
FLUXYS_LE_ID = 20504   # party_v2latest: SA FLUXYS BELGIUM (legal entity); BU = 20503 FLUXYS SA - BU
def _endur_only_checked(direction, bm, inv_month): return direction=="Entry" or bm==inv_month
_LTC_VALID = (
    "Entry at Interconnection Point (Firm)","Exit at Interconnection Point (Firm)",
    "Entry at Interconnection Point (Interruptible)","Exit at Interconnection Point (Interruptible)")
IC_GROUPS = [
    "Entry at Interconnection Point - Firm","Exit at Interconnection Point - Firm",
    "Auction Premium on Exit at Interconnection Point - Firm",
    "Auction Premium on Entry at Interconnection Point - Firm",
    "Entry at Interconnection Point - Interruptible",
    "Exit at Interconnection Point - Interruptible"]
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "a43b344f78062807")
st.set_page_config(page_title="Belgium Invoice Checker", layout="wide")

REQUIRED_SCOPES = {"sql"}            # hard requirement
FILE_SCOPES = {"files", "files.files"}  # either name is fine (files.files is deprecated alias)

def _token_scopes(token):
    """Read the scope claim from the forwarded OAuth token (no verification, display only)."""
    import base64, json
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return set(json.loads(base64.urlsafe_b64decode(payload)).get("scope", "").split())
    except Exception:
        return None

def _get_user_token():
    token = st.context.headers.get("x-forwarded-access-token")
    if not token:
        st.error("User authorization not available. Enable the sql and files.files scopes in app settings.")
        st.stop()
    scopes = _token_scopes(token)
    if scopes is not None:
        missing = sorted(REQUIRED_SCOPES - scopes)
        if not missing and not (scopes & FILE_SCOPES) and not st.session_state.get("_files_warned"):
            st.session_state["_files_warned"] = True
            st.warning(f"Token has no files scope (has: `{' '.join(sorted(scopes))}`). Uploads and Run Check may fail.")
        if missing:
            st.error(
                f"Your login token is missing the scope(s): **{', '.join(missing)}**.\n\n"
                f"Token currently has: `{' '.join(sorted(scopes))}`\n\n"
                "The app requests these scopes, but your browser is still sending an older token. "
                "Close all tabs of this app, then open it in a new private/incognito window "
                "(or clear cookies for *.databricksapps.com) and accept the consent prompt."
            )
            st.stop()
    return token

def _get_client():
    token = _get_user_token()
    host = os.environ.get("DATABRICKS_HOST", "")
    return WorkspaceClient(config=Config(host=host, token=token, auth_type="pat"))

def _execute(sql):
    w = _get_client()
    try:
        resp = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=sql, wait_timeout="50s")
    except Exception as e:
        if "scope" in str(e).lower():
            st.error("Missing API scope on your login token. Reopen the app in a private window and accept the consent prompt.")
            st.stop()
        raise
    state = resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
    while state in {"PENDING","RUNNING"}:
        time.sleep(1)
        resp = w.statement_execution.get_statement(resp.statement_id)
        state = resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
    if state != "SUCCEEDED":
        raise RuntimeError(f"SQL failed: {resp.status.error if resp.status else 'unknown'}")
    return resp

def run_sql(sql):
    resp = _execute(sql)
    if not resp.manifest or not resp.manifest.schema or not resp.manifest.schema.columns:
        return pd.DataFrame()
    cols = [c.name for c in resp.manifest.schema.columns]
    rows = resp.result.data_array or []
    return pd.DataFrame(rows, columns=cols)

def exec_sql(sql):
    _execute(sql)

def sql_lit(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return "NULL"
    if isinstance(v, bool): return "TRUE" if v else "FALSE"
    if isinstance(v, float) and v in (float("inf"), float("-inf")): return "CAST('Infinity' AS DOUBLE)" if v>0 else "CAST('-Infinity' AS DOUBLE)"
    if isinstance(v, (int, float)): return str(v)
    if isinstance(v, (date, datetime)): return f"'{v}'"
    return "'" + str(v).replace("'", "''") + "'"

def to_num(df, cols):
    out = df.copy()
    for c in cols:
        if c in out.columns: out[c] = pd.to_numeric(out[c], errors="coerce")
    return out

@st.cache_data(ttl=600, show_spinner=False)
def volume_read(path):
    # SDK returns a DownloadResponse; the file stream is in .contents
    resp = _get_client().files.download(path)
    return resp.contents.read()

def volume_write(path, data):
    _get_client().files.upload(path, io.BytesIO(data), overwrite=True)
    volume_read.clear()  # drop cached reads so replaced files are re-read

# === XML PARSERS ===
def parse_invoice_all(xml_bytes):
    tree = ET.parse(io.BytesIO(xml_bytes))
    rows = []
    for prod in tree.getroot().iter("CustomerProduct"):
        pname = (prod.findtext("ProductName") or "").strip()
        cap_type = "Interruptible" if "Interruptible" in pname else ("Firm" if any(k in pname for k in ("Firm","Auction","Premium")) else None)
        for il in prod.findall(".//InvoiceLine[@InvoiceLineSequence]"):
            lg = (il.findtext("InvoiceLineGroup") or "").strip()
            ln = (il.findtext("InvoiceLine") or "").strip()
            dt_ = (il.findtext("InvoiceLineDetail") or "").strip()
            bs = il.findtext("BillingStartDate")
            billing_start = pd.to_datetime(bs).date() if bs else None
            for bq in il.findall(".//BilledQuantity"):
                ai = bq.find("AdditionalInformation")
                pfi = bq.find("PriceFormulaInformation")
                es = bq.findtext("EconomicStartDate")
                gas_day = pd.to_datetime(es).date() if es else None
                ba = float(bq.findtext("BilledQuantityAmount") or 0)
                direction = ai.findtext("Direction") if ai is not None else None
                srt = ai.findtext("ServiceRateType") if ai is not None else None
                cr = ai.findtext("ContractReference") if ai is not None else None
                sh_s = int(ai.findtext("ServiceStartGasHour") or 0) if ai is not None else 0
                sh_e = int(ai.findtext("ServiceEndGasHour") or 0) if ai is not None else 0
                full_h = _gasday_hours(gas_day) if gas_day else 24   # v1.13.3: 23 / 25 h on clock-change gas days
                svc_h = (sh_e - sh_s + 1) if (sh_s > 0 and sh_e > 0) else full_h
                if svc_h <= 0: svc_h += full_h
                qe = pfi.find("QTY") if pfi is not None else None
                ue = pfi.find("UP") if pfi is not None else None
                qv = float(qe.get("QTY",0)) if qe is not None else None
                qu = qe.get("QTYUnit","") if qe is not None else None
                uv = float(ue.get("UP",0)) if ue is not None else None
                uu = ue.get("UPUnit","") if ue is not None else None
                vol = qv * svc_h if qv is not None else 0
                rows.append(dict(product_name=pname,capacity_type=cap_type,invoice_line_group=lg,
                    location=ln,detail=dt_,billing_start=billing_start,gas_day=gas_day,billing_type=(il.findtext("BillingType") or "").strip(),
                    direction=direction,service_rate_type=srt,contract_ref=cr,
                    billed_amount=ba,qty=qv,qty_unit=qu,service_hours=svc_h,volume_kwh=vol,up=uv,up_unit=uu))
    return pd.DataFrame(rows)

def parse_alloc_settlement(xml_bytes, kw):
    tree = ET.parse(io.BytesIO(xml_bytes))
    rows = []
    for p in tree.getroot().findall(".//CustomerProduct"):
        pn = (p.find("ProductName").text or "").strip()
        if kw.lower() not in pn.lower(): continue
        if "transm" in pn.lower() or "imbal" in pn.lower(): continue
        for il in p.findall(".//InvoiceLine[@InvoiceLineSequence]"):
            for bq in il.findall(".//BilledQuantity"):
                ec = bq.find("EconomicStartDate")
                am = bq.find("BilledQuantityAmount")
                pfi = bq.find("PriceFormulaInformation")
                if ec is None or am is None or pfi is None: continue
                qe = pfi.find("QTY"); ue = pfi.find("UP")
                rows.append(dict(gas_day=pd.to_datetime(ec.text).date(),
                    qty_kwh=float(qe.get("QTY")) if qe is not None else 0,
                    up_eur_kwh=float(ue.get("UP")) if ue is not None else 0,
                    amount_eur=float(am.text)))
    return pd.DataFrame(rows)

def get_alloc_period(xml_bytes, kw):
    try:
        tree = ET.parse(io.BytesIO(xml_bytes))
        for p in tree.getroot().findall(".//CustomerProduct"):
            pn = (p.find("ProductName").text or "").strip()
            if kw.lower() not in pn.lower(): continue
            if "transm" in pn.lower() or "imbal" in pn.lower(): continue
            for il in p.findall(".//InvoiceLine[@InvoiceLineSequence]"):
                bs = il.find("BillingStartDate")
                if bs is not None: return pd.to_datetime(bs.text).date()
    except Exception: pass
    return None

def parse_xml_header(xml_bytes):
    try:
        h = ET.parse(io.BytesIO(xml_bytes)).getroot().find("InvoiceDetailHeader")
        if h is None: return None
        return dict(invoice_type=h.findtext("InvoiceType"),invoice_number=h.findtext("InvoiceNumber"),
            invoice_date=h.findtext("InvoiceDate"),net_amount=h.findtext("InvoiceNetAmount"))
    except Exception: return None

def _parse_bm(s):
    if s is None: return None
    s = str(s)
    if len(s) == 7: return date(int(s[:4]),int(s[5:7]),1)
    try: return pd.to_datetime(s).date().replace(day=1)
    except: return None

# === LTC CHECK ===
def _dn(df):
    """Endur deal numbers of a group of deal rows, as one comma-separated cell (None if no deals)."""
    if df is None or len(df)==0 or "deal_number" not in df: return None
    v=sorted({int(x) for x in df["deal_number"].dropna()})
    return ", ".join(str(x) for x in v) if v else None

def _deal_eur(fr_):
    """Endur EUR for a group of deal rows = SUM over rows of (signed volume x price), so buys and sells net out.
    pv.price is EUR/MWh on OCUC/bilateral deals (>= 0.01) and EUR/kWh on PRISMA deals, so the former is /1000.
    Zero-price rows (the PRI_SEC sell-backs carved out for Virtualys net billing) are valued at the group's
    average priced unit rate, so a sell-back reduces EUR at the price that was paid for the capacity."""
    if fr_ is None or len(fr_)==0: return None
    vol=fr_["vol"].astype(float); pr=fr_["price"].astype(float)
    eur=vol*pr/pr.abs().ge(0.01).map({True:1000.0,False:1.0})
    if "wdf" in fr_: eur=eur*fr_["wdf"].astype(float)  # v1.7.2: within-day deals - Endur price is pro-rated by hours/24
    if "dsf" in fr_: eur=eur*fr_["dsf"].astype(float)  # v1.13.3: clock-change days - Fluxys bills per day, Endur per hour
    z=pr==0
    if z.any():
        den=vol[~z].sum()
        unit=(eur[~z].sum()/den) if den else 0.0
        eur=eur.where(~z,vol*unit)
    return abs(float(eur.sum()))

def _last_sunday(y,m):
    d=date(y,m,calendar.monthrange(y,m)[1]); return d-timedelta(days=(d.weekday()+1)%7)

def _gasday_hours(gd):
    """Hours in gas day gd (06:00 gd -> 06:00 gd+1 CET): 23 when the clocks go forward that night, 25 when they go back."""
    nd=gd+timedelta(days=1)
    if nd==_last_sunday(nd.year,3): return 23
    if nd==_last_sunday(nd.year,10): return 25
    return 24

def _wd_hours(sh, eh, full):
    """v1.13.3: booked hours of a within-day booking sh -> eh (local clock hours) on a gas day with `full` hours.
    The clock change happens at 02:00 local (20 h after the 06:00 gas-day start): spring 02:00-03:00 does not exist,
    autumn 02:00-03:00 happens twice. Only bookings that cover 02:00-03:00 gain / lose the hour."""
    h=(eh-sh)%24 or 24
    if full!=24:
        os_=(sh-6)%24; oe=(eh-6)%24 or 24
        if os_<=20 and oe>=21: h+=full-24
    return max(h,1)

def _pv_hours(r):
    """(booked hours, within-day price factor, day-basis factor) for one Endur profile row.
    Normal rows: gas-day hours (DST aware) across the row's days. Within-day deals (single-day deal whose
    start_time != end_time, e.g. 12 -> 6 = 18 h): only the booked hours.
    - within-day price factor: Endur pro-rates a single-id PRISMA within-day price as day price x hours/24
      (checked 159/159), so x 24 / booked hours puts it back on the full-day tariff.
    - day-basis factor (v1.13.3): Fluxys bills capacity per DAY (QTY x days x UP / 365), Endur values it per HOUR,
      so on 23 h / 25 h clock-change days Endur EUR is x (days x 24) / actual hours to compare like for like
      (checked: ZPT / Zeebrugge 28 Mar 2026 = exactly 23/24 of the bill; VIP THE-ZTP Oct 2026 = +1/744)."""
    s,e=r["pv_start"],r["pv_end"]
    nd=(e-s).days+1
    full=sum(_gasday_hours(s+timedelta(days=i)) for i in range(nd))
    if r.get("ds")==r.get("dend") and s==e and pd.notna(r.get("st_h")) and pd.notna(r.get("en_h")):
        sh,eh=int(r["st_h"]),int(r["en_h"])
        if sh!=eh:
            h=_wd_hours(sh,eh,full)
            # Only single-id PRISMA refs (PRI-<id>-ENTRY/EXIT, e.g. ZPT/Zeebrugge) carry a pro-rated price in Endur.
            # Two-id refs (PRI-<id>-<id>-..., e.g. Virtualys/VIP BENE) already hold the full per-kWh tariff.
            pr_=bool(re.match(r"^PRI-\d+-(ENTRY|EXIT)$",str(r.get("reference") or "")))
            return float(h),(24.0/h if pr_ else 1.0),1.0
    return float(full),1.0,(nd*24.0/full if full else 1.0)

def _cap_daily(il, er):
    """Gas-day breakdown for a Season capacity row: invoice lines vs Endur deals per gas day (kWh/h + EUR)."""
    rows={}
    for gd,g in il.groupby("gas_day"):
        rows[gd]=dict(gas_day=str(gd),invoice_volume=float(g["qty"].sum()),invoice_eur=float(g["billed_amount"].sum()),
            inv=", ".join(f"{c} ({int(h)}h)" if pd.notna(h) else str(c) for c,h in zip(g["contract_ref"],g["service_hours"])))
    if er is not None and len(er):
        for gd,g in er.groupby("pv_start"):
            r=rows.setdefault(gd,dict(gas_day=str(gd),invoice_volume=None,invoice_eur=None,inv=""))
            r["equinor_volume"]=float(g["cap_kwh_h"].abs().sum()); r["equinor_eur"]=_deal_eur(g)
            r["end"]=", ".join(f"{int(n)} ({int(h)}h)" for n,h in zip(g["deal_number"],g["hrs"]))
    out=[]
    for gd in sorted(rows):
        r=rows[gd]; iv,ev,ie,ee=r.get("invoice_volume"),r.get("equinor_volume"),r.get("invoice_eur"),r.get("equinor_eur")
        if iv is None: fl="In Endur, not on invoice"
        elif ev is None: fl="On invoice, not in Endur"
        elif abs(ev-iv)>1: fl="Capacity mismatch"
        elif ee is not None and ie is not None and abs(ee-ie)>0.5: fl="EUR mismatch"
        else: fl=""
        out.append(dict(gas_day=r["gas_day"],invoice_volume=iv,invoice_eur=ie,invoice_price=None,equinor_volume=ev,equinor_eur=ee,equinor_price=None,
                        flag=fl,detail="Invoice: "+(r.get("inv") or "-")+" | Endur: "+(r.get("end") or "-")))
    return out

def check_ltc(df_all, inv_month, log):
    df_ltc = df_all[df_all["product_name"].apply(lambda p: p.startswith(_LTC_VALID) or "Auction" in p or "Premium" in p)].copy()
    df_lt = df_ltc[df_ltc["invoice_line_group"].isin(IC_GROUPS)].copy()
    if len(df_lt)==0:
        log("No LTC entries."); return [],[]
    df_lt["billing_month"] = df_lt["billing_start"].apply(lambda d: d.replace(day=1) if d else None)
    # notebook: billing_months from ALL LTC products (df_all_ltc), not just IC groups
    bms = sorted(df_ltc["billing_start"].dropna().apply(lambda d: d.replace(day=1)).unique())
    bm_list = list(bms)
    me = calendar.monthrange(inv_month.year,inv_month.month)[1]
    m_end = date(inv_month.year,inv_month.month,me)
    is_auc = df_lt["detail"].str.contains("Auction|Premium",case=False,na=False)
    auc_s = df_lt[is_auc].groupby(["billing_month","direction","location"],as_index=False).agg(
        auction_eur=("billed_amount","sum"),contracts=("contract_ref",lambda x:",".join(str(v) for v in x if v))) if is_auc.any() else pd.DataFrame()
    sd = df_lt[df_lt["service_rate_type"]=="Season"]["gas_day"].dropna()
    pm1 = (inv_month.replace(day=1)-timedelta(days=1)).replace(day=1)
    nm1 = (inv_month.replace(day=28)+timedelta(days=4)).replace(day=1)
    if len(sd)>0:
        iw = sd[(sd>=pm1)&(sd<=nm1)]
        sws = iw.min() if len(iw)>0 else pm1.replace(day=10)
        swe = iw.max() if len(iw)>0 else inv_month.replace(day=9)
        cds = sorted(sd[(sd<pm1)|(sd>nm1)].unique())
    else: sws=pm1.replace(day=10); swe=inv_month.replace(day=9); cds=[]
    ams = bms[0] if bms else inv_month
    ss = min(ams,sws) if len(sd)>0 else ams
    if cds: ss = min(ss,min(cds))
    log(f"Querying Endur LTC deals ({ss} to {m_end})...")
    esql = f"""
    WITH nb AS (SELECT DISTINCT pv2.location_id FROM ms_atlas.endur_standard.deal_v2latest d2
        JOIN ms_atlas.endur_standard.profilevolume_v2latest pv2 ON d2.deal_number=pv2.deal_number
        WHERE d2.instrument_type_name IN ('COMM-CAP-EXIT','COMM-CAP-ENTRY') AND d2.buy_sell_name='Sell'
        AND d2.service_type IN ('Firm','Interruptible') AND d2.tran_status='Validated'
        AND pv2.settlement_type_id=1 AND pv2.price>0 AND LOWER(d2.reference) NOT LIKE '%conversion%'
        AND pv2.external_le_id={FLUXYS_LE_ID}),
    raw AS (SELECT d.deal_number,d.reference,d.service_type AS ect,
        CASE WHEN d.instrument_type_name='COMM-CAP-ENTRY' THEN 'Entry' ELSE 'Exit' END AS direction,
        l.location_name,l.ng_location_id,CAST(DATE_TRUNC('month',pv.start_date) AS DATE) AS billing_month,
        pv.calculated_profile_volume_kwh AS vol,pv.price,
        pv.calculated_profile_volume_kwh/((DATEDIFF(pv.end_date,pv.start_date)+1)*24) AS cap_kwh_h,
        pv.calculated_profile_volume_kwh*pv.price AS endur_eur,pv.start_date AS pv_start,pv.end_date AS pv_end,
        d.start_time AS st_h,d.end_time AS en_h,d.deal_start_date AS ds,d.deal_end_date AS dend,
        CASE WHEN DATEDIFF(d.deal_end_date,d.deal_start_date)>=360 THEN 'Yearly'
             WHEN DATEDIFF(d.deal_end_date,d.deal_start_date)>=85 THEN 'Quarterly'
             WHEN DATEDIFF(d.deal_end_date,d.deal_start_date)>=27 THEN 'Monthly' ELSE 'Daily' END AS bt,
        ROW_NUMBER() OVER (PARTITION BY d.deal_number,pv.start_date ORDER BY pv.price DESC) AS rn
        FROM ms_atlas.endur_standard.deal_v2latest d
        JOIN ms_atlas.endur_standard.profilevolume_v2latest pv ON d.deal_number=pv.deal_number
        JOIN ms_atlas.endur_standard.location_v2latest l ON pv.location_id=l.location_id
        JOIN ms_vulcan_gpgtopm_plab.hub2hub_v1.location_country_mapping cm ON pv.location_id=cm.location_id
        WHERE d.instrument_type_name IN ('COMM-CAP-EXIT','COMM-CAP-ENTRY')
        AND d.service_type IN ('Firm','Interruptible') AND d.tran_status='Validated' AND pv.settlement_type_id=1
        AND (pv.price!=0 OR (d.reference LIKE 'PRI_SEC_%' AND d.buy_sell_name='Sell'
             AND pv.location_id IN (SELECT location_id FROM nb)))
        AND pv.external_le_id={FLUXYS_LE_ID}
        AND cm.country_code='BE' AND pv.start_date>='{ss}' AND pv.end_date<='{m_end}')
    SELECT * FROM raw WHERE rn=1"""
    de = run_sql(esql)
    if len(de)==0: log("No Endur deals."); return [],[]
    de = to_num(de,["vol","price","cap_kwh_h","endur_eur"])
    de["billing_month"]=pd.to_datetime(de["billing_month"]).dt.date
    de["pv_start"]=pd.to_datetime(de["pv_start"]).dt.date
    for c_ in ("pv_end","ds","dend"): de[c_]=pd.to_datetime(de[c_]).dt.date
    de=to_num(de,["st_h","en_h"])
    # v1.7.2: capacity = volume / booked hours (within-day bookings use their own hours; DST days 23/25 h)
    hw=de.apply(_pv_hours,axis=1)
    de["hrs"]=[h for h,_,_ in hw]; de["wdf"]=[f for _,f,_ in hw]; de["dsf"]=[x for _,_,x in hw]
    ndst=int((de["dsf"]!=1).sum())
    if ndst: log(f"Endur: {ndst} profile rows contain a 23 h / 25 h clock-change day - EUR put on the Fluxys per-day basis")
    de["cap_kwh_h"]=de["vol"]/de["hrs"]
    nwd=int((de["wdf"]!=1).sum())
    if nwd: log(f"Endur: {nwd} within-day profile rows (capacity on booked hours, price re-based to full day)")
    dlt = de[de["bt"]!="Daily"].copy()
    sm = de["bt"]=="Daily"
    if len(sd)>0: sm &= ((de["pv_start"]>=sws)&(de["pv_start"]<=swe))|(de["pv_start"].isin(cds) if cds else False)
    dse = de[sm].copy()
    log(f"Endur: {len(dlt)} long-term + {len(dse)} seasonal")
    cb = df_lt[~is_auc].groupby(["billing_month","direction","location","service_rate_type","capacity_type"],as_index=False).agg(
        tariff_eur=("billed_amount","sum"),tqkh=("qty","sum"),tvk=("volume_kwh","sum"),
        nd=("gas_day","nunique"),iup=("up","first"),iuu=("up_unit","first"),
        detail=("detail",lambda x:" | ".join(sorted(set(str(v) for v in x if v)))),
        contracts=("contract_ref",lambda x:",".join(str(v) for v in x if v)),
        backbill=("billing_type",lambda x:any("back" in str(v).lower() for v in x)))
    cb["ikwh"] = cb["tqkh"]/cb["nd"]
    comp_rows,flags = [],[]
    for _,r in cb.iterrows():
        bm,d,loc=r["billing_month"],r["direction"],r["location"]
        rt,ct=r["service_rate_type"],r["capacity_type"]
        el=LOCATION_MAP.get(loc); dn=None
        edf = (dlt if rt=="LongTerm" else dse)
        edf = edf[edf["ect"]==ct]
        dim=calendar.monthrange(bm.year,bm.month)[1]
        ec=ee=ep=None; er=edf.iloc[0:0]
        if el:
            # Endur locations such as 'VIP THE-ZTP BE' hold Entry AND Exit deals -> match on direction too
            er=edf[(edf["ng_location_id"]==el)&(edf["billing_month"]==bm)&(edf["direction"]==d)]
            dn=_dn(er)
            if rt=="LongTerm" and len(er)>0:
                cs=abs(er["cap_kwh_h"].sum())  # NET capacity (matches v2 notebook)
                if cs:
                    ec=float(cs)
                    ee=_deal_eur(er)  # v1.7.1: sum of volume x price per deal (was net capacity x |cap|-weighted avg price)
                    ep=ee/(ec*dim/365) if (ee is not None and ec) else None  # implied EUR/kWh/h/year, for the price check
            elif rt!="LongTerm" and len(er)>0:
                dd=er["pv_start"].nunique()
                if dd:
                    ec=float(er["cap_kwh_h"].abs().sum())/dd
                    ee=_deal_eur(er)  # v1.7.1: signed sum (sell-backs now reduce EUR instead of adding to it)
                    vs=abs(float(er["vol"].sum())); ep=ee/vs if (ee is not None and vs) else None
        daily=None
        if rt!="LongTerm":  # v1.7.2: gas-day breakdown for daily (Season) capacity
            il_=df_lt[(~is_auc)&(df_lt["billing_month"]==bm)&(df_lt["direction"]==d)&(df_lt["location"]==loc)
                      &(df_lt["service_rate_type"]==rt)&(df_lt["capacity_type"]==ct)]
            daily=_cap_daily(il_,er)
        ip=float(r["iup"]) if rt=="LongTerm" else (float(r["tariff_eur"])/float(r["tvk"]) if r["tvk"] else None)
        pgp=(ep-ip) if (ep is not None and ip is not None) else None
        pgpp=(pgp/ip*100) if (pgp is not None and ip) else None
        aeur=0.0
        if len(auc_s)>0:
            ta=set(re.findall(r"AUC-(\d+)",r["contracts"]))
            for _2,a in auc_s.iterrows():
                if a["billing_month"]==bm and a["direction"]==d and ta&set(re.findall(r"AUC-(\d+)",a.get("contracts",""))): aeur+=a["auction_eur"]
        eq=ee
        if aeur>0 and rt=="LongTerm":
            eq=r["ikwh"]*ip*dim/365 if ip else None; ep=ip; pgp=0.0 if ip else None; pgpp=0.0 if ip else None
        comp_rows.append(dict(billing_month=str(bm),direction=d,location=loc,capacity_type=ct,
            line_type="Tariff",service_rate_type=rt,detail=r["detail"],
            invoice_kwh_h=round(r["ikwh"],2),endur_kwh_h=round(ec,2) if ec is not None else None,
            invoice_eur=round(r["tariff_eur"],2),equinor_eur=round(eq,2) if eq is not None else None,
            gap_eur=round(eq-r["tariff_eur"],2) if eq is not None else None,
            invoice_price=round(ip,9) if ip is not None else None,endur_price=round(ep,9) if ep is not None else None,
            price_gap=round(pgp,9) if pgp is not None else None,
            price_gap_pct=round(pgpp,3) if pgpp is not None else None,contracts=r["contracts"],endur_deal_numbers=dn,
            service_rate_type_=rt,daily=daily,backbill=bool(r.get("backbill"))))
        if aeur>0:
            bte=None
            if ip: bte=r["ikwh"]*ip*dim/365 if rt=="LongTerm" else r["tvk"]*ip
            ae=(ee-bte) if (ee is not None and bte is not None) else None
            comp_rows.append(dict(billing_month=str(bm),direction=d,location=loc,capacity_type=ct,
                line_type="Auction Premium",service_rate_type=rt,detail="Auction Premium",
                invoice_kwh_h=None,endur_kwh_h=None,invoice_eur=round(aeur,2),
                equinor_eur=round(ae,2) if ae is not None else None,gap_eur=round(ae-aeur,2) if ae is not None else None,price_gap_pct=None,contracts=r["contracts"],endur_deal_numbers=dn))
    def _lx(r,vol=True):  # EUR + volume picture of the comparison row, stored on every flag
        return dict(ieur=r.get("invoice_eur"),eeur=r.get("equinor_eur"),deur=r.get("gap_eur"),
                    ivol=r.get("invoice_kwh_h") if vol else None,evol=r.get("endur_kwh_h") if vol else None,
                    dn=r.get("endur_deal_numbers"))
    for _,r in pd.DataFrame(comp_rows).iterrows():
        bms,d,loc,lt=r["billing_month"],r["direction"],r["location"],r["line_type"]
        ct=r.get("capacity_type","Firm")
        nf0=len(flags)
        if lt=="Tariff":
            if r["endur_kwh_h"] is None or pd.isna(r.get("endur_kwh_h")):
                flags.append(dict(type=r["service_rate_type"],gas_day=bms,direction=d,location=loc,
                    invoice_value=r["invoice_kwh_h"],endur_value=None,gap=None,gap_pct=None,
                    unit="kWh/h",note=f"NO ENDUR MATCH [{ct}]",**_lx(r)))
            else:
                if r["endur_kwh_h"]!=r["invoice_kwh_h"]:
                    g=r["endur_kwh_h"]-r["invoice_kwh_h"]
                    gp=round(g/r["invoice_kwh_h"]*100,3) if r["invoice_kwh_h"] else None
                    if gp is not None and abs(gp)>FLAG_PCT:
                        flags.append(dict(type=r["service_rate_type"],gas_day=bms,direction=d,location=loc,
                            invoice_value=r["invoice_kwh_h"],endur_value=r["endur_kwh_h"],
                            gap=g,gap_pct=gp,unit="kWh/h",note=f"capacity rate mismatch [{ct}]",**_lx(r)))
                if r.get("price_gap_pct") is not None and not pd.isna(r.get("price_gap_pct")) and abs(r["price_gap_pct"])>FLAG_PCT:
                    flags.append(dict(type=r["service_rate_type"],gas_day=bms,direction=d,location=loc,
                        invoice_value=r.get("invoice_price"),endur_value=r.get("endur_price"),gap=r.get("price_gap"),gap_pct=r["price_gap_pct"],
                        unit="EUR/unit",note=f"price mismatch [{ct}]",**_lx(r)))
            # v1.14.0: yellow notice - unusual line, but the numbers match (no red flag raised for this row)
            if len(flags)==nf0:
                why=[]
                if ct=="Interruptible": why.append("Interruptible capacity (rarely billed - check it is expected)")
                if r.get("backbill"): why.append("BackBilling line(s) - correction of an earlier month")
                if why:
                    flags.append(dict(type=r["service_rate_type"],gas_day=bms,direction=d,location=loc,
                        invoice_value=r["invoice_kwh_h"],endur_value=r.get("endur_kwh_h"),gap=None,gap_pct=None,unit="kWh/h",
                        note="NOTICE: "+"; ".join(why)+" - numbers match",**_lx(r)))
        elif lt=="Auction Premium":
            if r.get("gap_eur") is not None and not pd.isna(r.get("gap_eur")) and r["invoice_eur"]:
                gp=round(r["gap_eur"]/r["invoice_eur"]*100,3)
                if abs(gp)>FLAG_PCT:
                    flags.append(dict(type="AuctionPremium",gas_day=bms,direction=d,location=loc,
                        invoice_value=r["invoice_eur"],endur_value=r["equinor_eur"],gap=r["gap_eur"],gap_pct=gp,
                        unit="EUR",note=f"auction premium EUR mismatch [{ct}]",**_lx(r,vol=False)))
    def _eagg(fr_,et,bm):  # Endur capacity kWh/h + EUR for a group of deal rows (same basis as matched rows)
        dim_=calendar.monthrange(bm.year,bm.month)[1]; cap=eur_=None
        if et=="LongTerm":
            cap=abs(float(fr_["cap_kwh_h"].sum()))
        else:
            nd_=fr_["pv_start"].nunique(); cap=float(fr_["cap_kwh_h"].abs().sum())/nd_ if nd_ else None
        eur_=_deal_eur(fr_)  # v1.7.1: sum of volume x price per deal
        return cap,eur_
    def _endur_flag(bm,d,loc,ect,sel,note):
        ecap=0.0; eeur=0.0; ft=None; deals=[]
        for edf_,et in ((dlt,"LongTerm"),(dse,"Season")):
            fr_=edf_[sel(edf_)&(edf_["ect"]==ect)&(edf_["billing_month"]==bm)&(edf_["direction"]==d)]
            if len(fr_)==0: continue
            cap,eur_=_eagg(fr_,et,bm)
            if cap and cap>0:
                ecap+=cap; ft=et; deals.append(fr_)
                if eur_ is not None and not pd.isna(eur_): eeur+=eur_
        if ecap>0:
            flags.append(dict(type=ft or "LongTerm",gas_day=str(bm),direction=d,location=loc,
                invoice_value=None,endur_value=round(ecap,2),gap=None,gap_pct=None,unit="kWh/h",note=note,
                evol=round(ecap,2),eeur=round(eeur,2) if eeur else None,dn=_dn(pd.concat(deals))))
    # Endur-only: every mapped location x Entry/Exit x Firm/Interruptible that is booked in Endur but not on the invoice
    for bm in bm_list:
        inv_routes=set((x["direction"],x["location"],x["capacity_type"]) for _,x in cb[cb["billing_month"]==bm].iterrows())
        for loc,ng in LOCATION_MAP.items():
            for d in ("Entry","Exit"):
                if not _endur_only_checked(d,bm,inv_month): continue
                for ect in ("Firm","Interruptible"):
                    if (d,loc,ect) in inv_routes: continue
                    _endur_flag(bm,d,loc,ect,lambda x,ng=ng: x["ng_location_id"]==ng,f"ENDUR ONLY [{ect}] (not invoiced)")
        # Belgian Endur capacity at locations not in LOCATION_MAP -> flagged so nothing is skipped silently
        known=set(LOCATION_MAP.values())
        um_=pd.concat([dlt,dse])
        um_=um_[(~um_["ng_location_id"].isin(known))&(um_["billing_month"]==bm)]
        for (lname,ng),_g in um_.groupby(["location_name","ng_location_id"]):
            for d in ("Entry","Exit"):
                if not _endur_only_checked(d,bm,inv_month): continue
                for ect in ("Firm","Interruptible"):
                    _endur_flag(bm,d,lname,ect,lambda x,ng=ng: x["ng_location_id"]==ng,f"UNMAPPED ENDUR LOCATION {lname} ({ng}) [{ect}]")
    log(f"LTC: {len(comp_rows)} comparisons, {len(flags)} flags")
    return comp_rows,flags

# === ALLOC SETTLEMENT CHECK ===
def check_alloc(bill_xml,sb_xml,ss,se,log):
    db = parse_alloc_settlement(bill_xml,"Allocation Settlement") if bill_xml else pd.DataFrame()
    ds = parse_alloc_settlement(sb_xml,"Allocation Settl") if sb_xml else pd.DataFrame()
    if len(db)==0 and len(ds)==0: log("No alloc entries."); return [],[],False
    dbd = db.groupby("gas_day",as_index=False).agg(bqk=("qty_kwh","sum"),bup=("up_eur_kwh","first"),beur=("amount_eur","sum")) if len(db)>0 else pd.DataFrame(columns=["gas_day","bqk","bup","beur"])
    dsd = ds.groupby("gas_day",as_index=False).agg(sqk=("qty_kwh","sum"),sup=("up_eur_kwh","first"),seur=("amount_eur","sum")) if len(ds)>0 else pd.DataFrame(columns=["gas_day","sqk","sup","seur"])
    dl = ",".join(f"'{d}'" for d in DOMESTIC_LOCATIONS)
    log("Querying dispatch for alloc settlement...")
    dq = run_sql(f"SELECT gas_day,location_id,SUM(preliminary_allocation) AS pk,SUM(final_allocation) AS fk FROM {DISPATCH_TABLE} WHERE country='{COUNTRY}' AND balancing_country='{COUNTRY}' AND gas_day BETWEEN '{ss}' AND '{se}' AND location_id IN ({dl}) GROUP BY gas_day,location_id")
    dq = to_num(dq,["pk","fk"]); dq["gas_day"]=pd.to_datetime(dq["gas_day"]).dt.date
    hf = dq["fk"].notna().any() and (dq["fk"].fillna(0)!=0).any()
    dq["dk"] = dq["fk"].fillna(0)-dq["pk"].fillna(0); dq.loc[dq["dk"].abs()<10,"dk"]=0
    pu = dq[dq["dk"]<0].groupby("gas_day",as_index=False).agg(epk=("dk",lambda x:abs(x.sum())))
    sa = dq[dq["dk"]>0].groupby("gas_day",as_index=False).agg(esk=("dk","sum"))
    ad = pd.DataFrame({"gas_day":pd.date_range(ss,se).date})
    dd = ad.merge(pu,on="gas_day",how="left").merge(sa,on="gas_day",how="left").fillna(0)
    ps = run_sql(f"SELECT DATE_ADD(publication_date,1) AS gas_day,publisher_column_value/1000.0 AS zp FROM {PRICE_TABLE} WHERE keys='{PRICE_KEYS}' AND publisher_column_name='{PRICE_COLUMN}' AND data_package='{PRICE_DATA_PACKAGE}' AND publication_date BETWEEN '{ss-timedelta(days=1)}' AND '{se-timedelta(days=1)}'")
    ps = to_num(ps,["zp"]); ps["gas_day"]=pd.to_datetime(ps["gas_day"]).dt.date
    ac = ad.merge(dbd,on="gas_day",how="left").merge(dsd,on="gas_day",how="left").merge(ps,on="gas_day",how="left").merge(dd,on="gas_day",how="left").fillna(0)
    # price diffs vs ZTP index (only on days with an entry)
    ac["bpd"]=None; mb_=ac["bqk"]>0
    ac.loc[mb_,"bpd"]=(pd.to_numeric(ac.loc[mb_,"bup"])-pd.to_numeric(ac.loc[mb_,"zp"])).round(6)
    ac["spd"]=None; ms_=ac["sqk"]>0
    ac.loc[ms_,"spd"]=(pd.to_numeric(ac.loc[ms_,"sup"])-pd.to_numeric(ac.loc[ms_,"zp"])).round(6)
    ac["epe"]=ac["epk"]*ac["zp"]; ac["ese"]=ac["esk"]*ac["zp"]
    ac["qd"]=(ac["esk"]-ac["epk"])-(ac["sqk"]-ac["bqk"])
    ac["ed"]=(ac["ese"]-ac["epe"])-(ac["seur"]-ac["beur"])
    ac["flag"]=""
    bpd=pd.to_numeric(ac["bpd"],errors="coerce"); spd=pd.to_numeric(ac["spd"],errors="coerce")
    ac.loc[bpd.notna()&(bpd.abs()>0.0001),"flag"]="Price mismatch (bill)"
    ac.loc[spd.notna()&(spd.abs()>0.0001),"flag"]="Price mismatch (self-bill)"
    if hf:
        ac.loc[(ac["bqk"]>0)&(ac["epk"]==0),"flag"]="Bill entry, no dispatch purchase"
        ac.loc[(ac["epk"]>10)&(ac["bqk"]==0),"flag"]="Dispatch purchase, no bill entry"
        ac.loc[(ac["sqk"]>0)&(ac["esk"]==0),"flag"]="Self-bill entry, no dispatch sale"
        ac.loc[(ac["esk"]>10)&(ac["sqk"]==0),"flag"]="Dispatch sale, no self-bill entry"
        ac.loc[(ac["flag"]=="")&(ac["qd"].abs()>10),"flag"]="Qty mismatch"
        ac.loc[(ac["flag"]=="")&(ac["ed"].abs()>1.0),"flag"]="EUR mismatch"
    af = ac[ac["flag"]!=""].copy()
    ipe=ac["beur"].sum(); ipk=ac["bqk"].sum(); ise=ac["seur"].sum(); isk=ac["sqk"].sum()
    epe=ac["epe"].sum(); epk2=ac["epk"].sum(); ese=ac["ese"].sum(); esk2=ac["esk"].sum()
    pd_=epe-ipe if hf else 0; sd_=ese-ise if hf else 0
    summary=[dict(line="Purchases",invoice_eur=float(ipe),invoice_kwh=float(ipk),equinor_eur=float(epe) if hf else None,equinor_kwh=float(epk2) if hf else None,diff_eur=float(pd_) if hf else None,diff_pct=round(pd_/ipe*100,2) if (hf and ipe) else None,status="Awaiting Final" if not hf else _gs(epe,pd_,"Dispatch")),
        dict(line="Sales (self-bill)",invoice_eur=float(ise),invoice_kwh=float(isk),equinor_eur=float(ese) if hf else None,equinor_kwh=float(esk2) if hf else None,diff_eur=float(sd_) if hf else None,diff_pct=round(sd_/ise*100,2) if (hf and ise) else None,status="Awaiting Final" if not hf else _gs(ese,sd_,"Dispatch"))]
    # gas-day breakdown for the Results tab (every day, not just flagged ones)
    def _bflag(f): return "" if ("self-bill" in f.lower() or "sale" in f.lower()) else f
    def _sflag(f): return "" if ("(bill)" in f or "purchase" in f.lower() or f.startswith("Bill entry")) else f
    summary[0]["daily"]=[dict(gas_day=str(r["gas_day"]),invoice_volume=float(r["bqk"]),invoice_eur=float(r["beur"]),
        equinor_volume=float(r["epk"]) if hf else None,equinor_eur=float(r["epe"]) if hf else None,flag=_bflag(r["flag"]))
        for _,r in ac.iterrows() if (r["bqk"] or r["epk"] or _bflag(r["flag"]))]
    summary[1]["daily"]=[dict(gas_day=str(r["gas_day"]),invoice_volume=float(r["sqk"]),invoice_eur=float(r["seur"]),
        equinor_volume=float(r["esk"]) if hf else None,equinor_eur=float(r["ese"]) if hf else None,flag=_sflag(r["flag"]))
        for _,r in ac.iterrows() if (r["sqk"] or r["esk"] or _sflag(r["flag"]))]
    # net per gas day = sales - purchases (same basis as the EUR / qty mismatch tests above)
    def _apct(d,b): return round(d/abs(b)*100,2) if b else None
    fr=[dict(gas_day=str(f["gas_day"]),invoice_value=float(f.get("beur")),equinor_value=float(f.get("epe")),diff=float(f.get("ed")),
             ieur=float(f["seur"]-f["beur"]),eeur=float(f["ese"]-f["epe"]),deur=float(f["ed"]),
             ivol=float(f["sqk"]-f["bqk"]),evol=float(f["esk"]-f["epk"]),
             diff_pct=_apct(float(f["ed"]),float(f["seur"]-f["beur"])),flag=f["flag"]) for _,f in af.iterrows()]
    log(f"Alloc: purch={ipe:,.2f}, sales={ise:,.2f}, {len(fr)} flags")
    return summary,fr,hf

# === VARIABLE FEE CHECK ===
def check_vf(df_all,ms,me,ck,log):
    vf=df_all[(df_all["location"]=="Variable Fee")&(df_all["invoice_line_group"]=="ZTP Trading")
              &(df_all["gas_day"]>=ms)&(df_all["gas_day"]<=me)].copy()  # notebook: arrears month only
    if len(vf)==0: log("No VF entries."); return None,[]
    en=vf[vf["direction"]=="Entry"].groupby("gas_day").agg(eqm=("qty","sum"),upm=("up","first"),eae=("billed_amount","sum")).reset_index()
    ex=vf[vf["direction"]=="Exit"].groupby("gas_day").agg(xqm=("qty","sum"),xae=("billed_amount","sum")).reset_index()
    vd=en.merge(ex,on="gas_day",how="outer").sort_values("gas_day").reset_index(drop=True)
    vd["tae"]=vd["eae"]+vd["xae"]  # notebook: no fill before summing (NaN if one side missing)
    up=vd["upm"].iloc[0]
    log("Reading mapping and querying dispatch for VF...")
    mb=volume_read(f"{VOLUME}/reference/mapping_new.xlsx")
    fm=pd.read_excel(io.BytesIO(mb))
    fm["VALID_FROM"]=pd.to_datetime(fm["VALID_FROM"]); fm["VALID_TO"]=pd.to_datetime(fm["VALID_TO"])
    zm=fm[fm["NODE_ID_DELIVERY"]=="ZTPH"].copy()
    dq=run_sql(f"SELECT gas_day,contract_id,contract_type,quantity_unit,SUM(nomination) AS vk FROM {DISPATCH_TABLE} WHERE country='{COUNTRY}' AND balancing_country='{COUNTRY}' AND location_id='ZTPH' AND LOWER(contract_type) NOT LIKE '%optimize%' AND LOWER(contract_type) NOT LIKE '%balance%' AND gas_day BETWEEN '{ms}' AND '{me}' AND nomination IS NOT NULL GROUP BY gas_day,contract_id,contract_type,quantity_unit")
    dq=to_num(dq,["vk"])
    if len(dq)==0: log("VF: no ZTPH dispatch -> SKIPPED"); return None,[]
    dq["gas_day"]=pd.to_datetime(dq["gas_day"]).dt.date; dq["gdt"]=pd.to_datetime(dq["gas_day"])
    mg=dq.merge(zm[["CONTRACT_GROUP_ID","SHIPPER_CODE","VALID_FROM","VALID_TO"]],left_on="contract_id",right_on="CONTRACT_GROUP_ID",how="left")
    hm=mg["CONTRACT_GROUP_ID"].notna()
    dv=(mg["VALID_FROM"].isna()|(mg["VALID_FROM"]<=mg["gdt"]))&(mg["VALID_TO"].isna()|(mg["VALID_TO"]>=mg["gdt"]))
    mg=mg[~hm|dv].copy()
    mp=mg[mg["SHIPPER_CODE"].notna()].copy(); mp["sk"]=mp["vk"]
    dd=mp.groupby(["gas_day","SHIPPER_CODE"]).agg(sk=("sk","sum")).reset_index().rename(columns={"SHIPPER_CODE":"SC"})
    log("Querying Endurast AST...")
    aq=run_sql(f"WITH r AS (SELECT shippercode AS SC,TO_DATE(time) AS gas_day,quantity,ROW_NUMBER() OVER (PARTITION BY counterparty,shippercode,time ORDER BY enqueued_time DESC) AS rn FROM {ENDURAST_TABLE} WHERE location='ZTPH Hub (H-Zone)' AND TO_DATE(time) BETWEEN '{ms}' AND '{me}') SELECT gas_day,SC,SUM(quantity) AS sk FROM r WHERE rn=1 GROUP BY gas_day,SC")
    aq=to_num(aq,["sk"]); aq["gas_day"]=pd.to_datetime(aq["gas_day"]).dt.date
    cb=pd.concat([dd[["gas_day","SC","sk"]],aq[["gas_day","SC","sk"]]],ignore_index=True)
    sd=cb.groupby(["gas_day","SC"]).agg(nk=("sk","sum")).reset_index()
    dex=sd[sd["nk"]<0].groupby("gas_day",as_index=False)["nk"].sum().assign(exk=lambda x:x["nk"].abs()).drop(columns="nk")
    vad=pd.DataFrame({"gas_day":pd.date_range(ms,me).date})
    vc=vad.merge(vd[["gas_day","eqm","xqm","eae","xae","tae"]],on="gas_day",how="left").merge(dex,on="gas_day",how="left").fillna(0)
    vc["exm"]=vc["exk"]/1000; vc["xdm"]=vc["exm"]-vc["xqm"]
    sf=vc["xqm"].replace(0,float("nan")); vc["xdp"]=(vc["xdm"]/sf*100).round(2)
    vc["exc"]=vc["exm"]*up; vc["cd"]=vc["exc"]-vc["xae"]; vc["flag"]=""
    vc.loc[vc["xdp"].abs()>1,"flag"]="\u26a0\ufe0f EXIT"
    vfl=vc[vc["flag"]!=""]
    iet=vc["xqm"].sum(); eet=vc["exm"].sum(); evdp=(eet-iet)/iet*100 if iet else 0
    igt=vc["tae"].sum(); egt=vc["exc"].sum()*2; gtd=egt-igt
    v="VERIFIED" if abs(evdp)<1 else "REVIEW REQUIRED"
    s=dict(check_month=ck,line_type="Variable Trading Fee",invoice_qty=float(iet),equinor_qty=float(eet),invoice_eur=igt,equinor_eur=egt,eur_diff=gtd,qty_diff_pct=evdp,days_flagged=len(vfl),verdict=v,
           # Entry + Exit MWh (what the EUR is charged on) - used by the Overview; Equinor side = exit x 2, same as equinor_eur
           invoice_qty_all=float(vd["eqm"].fillna(0).sum()+vd["xqm"].fillna(0).sum()),equinor_qty_all=float(eet*2))
    # gas-day breakdown (Entry + Exit, same basis as the monthly figures); flag is the exit-quantity test above
    s["daily"]=[dict(gas_day=str(r["gas_day"]),invoice_volume=float(r["eqm"]+r["xqm"]),invoice_eur=float(r["tae"]),
                     equinor_volume=float(r["exm"]*2),equinor_eur=float(r["exc"]*2),flag=r["flag"]) for _,r in vc.iterrows()]
    fr=[dict(gas_day=str(r["gas_day"]),line_type="Variable Trading Fee",inv_qty=float(r["xqm"]),eq_qty=float(r["exm"]),qty_diff=float(r["xdm"]),qty_diff_pct=float(r["xdp"]),inv_up=float(up),eq_price=float(up),ieur=float(r["xae"]),eeur=float(r["exc"]),deur=float(r["cd"]),flag=r["flag"]) for _,r in vfl.iterrows()]
    log(f"VF: invoice={igt:,.2f}, verdict={v}, {len(fr)} flags")
    return s,fr

# === ENERGY IN CASH CHECK ===
def check_eic(df_all,ms,me,ck,log):
    eic=df_all[(df_all["location"]=="Energy in Cash")&(df_all["gas_day"]>=ms)&(df_all["gas_day"]<=me)].copy()  # notebook: arrears month only
    if len(eic)==0: log("No EiC entries."); return [],[]
    dl=",".join(f"'{d}'" for d in DOMESTIC_LOCATIONS)
    hl=",".join(f"'{h}'" for h in list(DOMESTIC_LOCATIONS)+list(HUB_INTERNAL_LOCATIONS))
    log("Querying dispatch for EiC...")
    dd=run_sql(f"SELECT gas_day,SUM(preliminary_allocation) AS dk FROM {DISPATCH_TABLE} WHERE country='{COUNTRY}' AND balancing_country='{COUNTRY}' AND gas_day BETWEEN '{ms}' AND '{me}' AND location_id IN ({dl}) GROUP BY gas_day")
    dd=to_num(dd,["dk"]); dd["gas_day"]=pd.to_datetime(dd["gas_day"]).dt.date
    ic=run_sql(f"SELECT gas_day,location_id,SUM(preliminary_allocation) AS lk FROM {DISPATCH_TABLE} WHERE country='{COUNTRY}' AND balancing_country='{COUNTRY}' AND gas_day BETWEEN '{ms}' AND '{me}' AND contract_type='Flow' AND location_id NOT IN ({hl}) GROUP BY gas_day,location_id")
    ic=to_num(ic,["lk"]); ic["gas_day"]=pd.to_datetime(ic["gas_day"]).dt.date
    ie=ic[ic["lk"]>0].groupby("gas_day",as_index=False).agg(eik=("lk","sum"))
    ix=ic[ic["lk"]<0].groupby("gas_day",as_index=False).agg(xik=("lk","sum"))
    ps=run_sql(f"SELECT DATE_ADD(publication_date,1) AS gas_day,publisher_column_value/1000.0 AS pk FROM {PRICE_TABLE} WHERE keys='{PRICE_KEYS}' AND publisher_column_name='{PRICE_COLUMN}' AND data_package='{PRICE_DATA_PACKAGE}' AND publication_date BETWEEN '{ms-timedelta(days=1)}' AND '{me-timedelta(days=1)}'")
    ps=to_num(ps,["pk"]); ps["gas_day"]=pd.to_datetime(ps["gas_day"]).dt.date
    ad=pd.DataFrame({"gas_day":pd.date_range(ms,me).date})
    eq=ad.merge(dd,on="gas_day",how="left").merge(ie,on="gas_day",how="left").merge(ix,on="gas_day",how="left").merge(ps,on="gas_day",how="left")
    eq[["dk","eik","xik"]]=eq[["dk","eik","xik"]].fillna(0)  # notebook: price NOT filled
    eq["eq"]=eq["eik"].abs()*QTY_MULTIPLIER; eq["xq"]=eq["xik"].abs()*QTY_MULTIPLIER; eq["dq"]=eq["dk"].abs()*QTY_MULTIPLIER
    cm={"Entry at Interconnection Point":"eq","Exit at Interconnection Point":"xq","Exit at Domestic Point":"dq"}
    sr,fr=[],[]
    for cat,qc in cm.items():
        iv=eic[eic["invoice_line_group"]==cat].groupby("gas_day").agg(iq=("qty","sum"),iup=("up","first"),ia=("billed_amount","sum")).reset_index()
        if len(iv)==0: continue
        ec=eq[["gas_day",qc,"pk"]].copy(); ec.columns=["gas_day","eqty","ep"]
        # notebook semantics: outer merge, NO fillna; qty_diff_pct divides by raw inv_qty
        cp=iv.merge(ec,on="gas_day",how="outer").sort_values("gas_day").reset_index(drop=True)
        cp["qd"]=cp["eqty"]-cp["iq"]; cp["qdp"]=(cp["qd"]/cp["iq"]*100).round(2)
        # v1.13.2: no EEX ZTP price yet for a gas day -> NOT a price mismatch; value Equinor qty at the invoice price
        # so only the quantity is checked that day (the price table can lag behind the bill)
        cp["nop"]=cp["ep"].isna()&cp["iup"].notna()
        cp["pm"]=cp["nop"]|((cp["iup"]-cp["ep"]).abs()<0.000001)
        cp["epu"]=cp["ep"].where(~cp["nop"],cp["iup"])
        cp["ea"]=cp["eqty"]*cp["epu"]; cp["ad"]=cp["ea"]-cp["ia"]
        cp["flag"]=""; he=~((cp["eqty"].fillna(0).abs()<0.0001)&(cp["iq"].isna()))
        cp.loc[he&(cp["qdp"].abs()>1),"flag"]="\u26a0\ufe0f QTY"
        cp.loc[he&(~cp["pm"]),"flag"]=cp.loc[he&(~cp["pm"]),"flag"]+" \u26a0\ufe0f PRICE"
        # v1.14.0: orange technical flag - no EEX price for the day, quantity matches (so it is not an invoice error)
        cp.loc[he&cp["nop"]&(cp["flag"]==""),"flag"]="TECHNICAL: no EEX ZTP price for this gas day"
        iqs=iv["iq"].sum(); eqs=cp["eqty"].sum()
        it=iv["ia"].sum(); et=cp["ea"].sum(); ed=et-it
        v="VERIFIED" if (it and abs(ed/it*100)<0.1 and cp["pm"].all()) else "REVIEW REQUIRED"
        nnp=int(cp["nop"].sum())
        if v=="VERIFIED" and nnp: v=f"TECHNICAL - no EEX price on {nnp} day(s), quantities verified"
        sr.append(dict(category=cat,line_type="Energy in Cash",invoice_qty=float(iqs),equinor_qty=float(eqs),invoice_eur=it,equinor_eur=et,eur_diff=ed,qty_diff_pct=(eqs-iqs)/iqs*100 if iqs else None,days_flagged=len(cp[cp["flag"]!=""]),price_match=bool(cp["pm"].all()),verdict=v))
        sr[-1]["daily"]=[dict(gas_day=str(r["gas_day"]),invoice_volume=r["iq"],invoice_eur=r["ia"],invoice_price=r["iup"],
                              equinor_volume=r["eqty"],equinor_eur=r["ea"],equinor_price=r["ep"],flag=r["flag"],
                              detail=("No EEX ZTP price for this gas day yet - Equinor EUR valued at the invoice price, only quantity checked" if r["nop"] else None)) for _,r in cp.iterrows()]
        if cp["nop"].any(): log(f"EiC {cat}: {int(cp['nop'].sum())} gas day(s) without EEX ZTP price (from {cp.loc[cp['nop'],'gas_day'].min()}) - quantity-only check")
        for _,r in cp[cp["flag"]!=""].iterrows():
            fr.append(dict(gas_day=str(r["gas_day"]),category=cat,line_type="Energy in Cash",inv_qty=float(r["iq"]),eq_qty=float(r["eqty"]),qty_diff=float(r["qd"]),qty_diff_pct=float(r["qdp"]),inv_up=float(r["iup"]),eq_price=float(r["ep"]),ieur=float(r["ia"]),eeur=float(r["ea"]),deur=float(r["ad"]),flag=r["flag"].strip()))
    log(f"EiC: {len(sr)} categories, {len(fr)} flags")
    return sr,fr

# === PERIOD DETECTION (mirrors v2 notebook 'Parse Invoice XML' cell) ===
def detect_periods(df_all, fallback_month):
    al=df_all[df_all["product_name"].apply(lambda p:p.startswith(_LTC_VALID) or "Auction" in p or "Premium" in p)]
    bms=sorted(al["billing_start"].dropna().apply(lambda d:d.replace(day=1)).unique())
    inv_m=bms[-1] if bms else fallback_month.replace(day=1)
    m_end=date(inv_m.year,inv_m.month,calendar.monthrange(inv_m.year,inv_m.month)[1])
    # Arrears (VF + EiC): month of the EARLIEST gas day among those entries
    arr=df_all[((df_all["location"]=="Variable Fee")&(df_all["invoice_line_group"]=="ZTP Trading"))|(df_all["location"]=="Energy in Cash")]
    gd=arr["gas_day"].dropna()
    if len(gd)>0:
        am=gd.min().replace(day=1); aend=date(am.year,am.month,calendar.monthrange(am.year,am.month)[1])
    else:
        am,aend=inv_m,m_end
    ack=f"{am.year:04d}-{am.month:02d}"
    # Allocation settlement: earliest billing_start of alloc entries on the bill
    ae=df_all[df_all["product_name"].str.contains("Allocation Settlement",case=False,na=False)&~df_all["product_name"].str.contains("transm|imbal",case=False,na=False)]
    sst=ae["billing_start"].dropna().min() if len(ae)>0 else None
    if sst is not None and not pd.isna(sst):
        se=date(sst.year,sst.month,calendar.monthrange(sst.year,sst.month)[1]); sck=f"{sst.year}-{sst.month:02d}"
    else: sst=se=sck=None
    return dict(inv_m=inv_m,am=am,aend=aend,ack=ack,ss=sst,se=se,sck=sck)

# === UNIFIED SAVE ===
def _gs(eq,gap,label="Endur"):
    if eq is None or (isinstance(eq,float) and pd.isna(eq)): return f"No {label} Match"
    if gap is not None and abs(gap)<1: return "OK"
    return f"\u0394 {gap:+,.2f} EUR" if gap is not None else "Unknown"

def save_results(lc,lf,als,alf,ahf,vfs,vff,eis,eif,df_all,inv_m,ck,inv_file,ack,sck,log,rid=None,run_by=None):
    # ack = arrears month (VF + EiC, detected from gas days); sck = allocation settlement month
    ts=datetime.utcnow().isoformat(); inv_m1=(inv_m.replace(day=1)-timedelta(days=1)).replace(day=1)
    ar,af=[],[]
    def _u(bm,ty,pn,lo,di,ie=None,iv=None,ee=None,ev=None,de=None,dp=None,st_="Not Checked",dn=None):
        return dict(check_month=ck,invoice_file=inv_file,invoice_month=ck,billing_month=str(bm) if bm else None,type=ty,product_name=pn,location=lo or "",direction=di or "",invoice_eur=ie,invoice_volume=iv,equinor_eur=ee,equinor_volume=ev,diff_eur=de,diff_pct=dp,status=st_,endur_deal_numbers=dn or None,run_timestamp=ts)
    def _fl(v):
        try:
            v=float(v); return None if pd.isna(v) else v
        except (TypeError,ValueError): return None
    def _f(bm,ty,gd,lo,di,iv=None,ev=None,d=None,dp=None,u="",fd="",ie_=None,iv_=None,ee_=None,ev_=None,de_=None,dn_=None):
        # v1.3.0: flags use the same EUR + volume columns as the summaries.
        # Legacy invoice_value/equinor_value/diff/unit are no longer written (left NULL; older rows keep them).
        return dict(check_month=ck,invoice_file=inv_file,billing_month=str(bm),type=ty,gas_day=str(gd),location=lo or "",direction=di or "",
                    invoice_eur=_fl(ie_),invoice_volume=_fl(iv_),equinor_eur=_fl(ee_),equinor_volume=_fl(ev_),diff_eur=_fl(de_),
                    diff_pct=_fl(dp),flag_detail=fd,endur_deal_numbers=dn_ if isinstance(dn_,str) and dn_ else None,run_timestamp=ts)
    for r in lc:
        st_=_gs(r.get("equinor_eur"),r.get("gap_eur"))
        if st_=="OK" and (r.get("capacity_type")=="Interruptible" or r.get("backbill")):  # v1.14.0: yellow notice
            st_="OK \u00b7 NOTICE: "+("Interruptible" if r.get("capacity_type")=="Interruptible" else "")+(" BackBilling" if r.get("backbill") else "")
        ar.append(_u(r["billing_month"],f"LTC {r['line_type']}",f"{r['capacity_type']} {r.get('service_rate_type','')}",r["location"],r["direction"],ie=r["invoice_eur"],iv=r.get("invoice_kwh_h"),ee=r.get("equinor_eur"),ev=r.get("endur_kwh_h"),de=r.get("gap_eur"),dp=r.get("price_gap_pct"),st_=st_,dn=r.get("endur_deal_numbers")))
    for f in lf:
        af.append(_f(f["gas_day"],f.get("type","LTC"),f["gas_day"],f.get("location",""),f.get("direction",""),iv=f.get("invoice_value"),ev=f.get("endur_value"),d=f.get("gap"),dp=f.get("gap_pct"),u=f.get("unit",""),fd=f.get("note",""),
                     ie_=f.get("ieur"),iv_=f.get("ivol"),ee_=f.get("eeur"),ev_=f.get("evol"),de_=f.get("deur"),dn_=f.get("dn")))
    if vfs:
        ar.append(_u(vfs["check_month"],"Variable Trading Fee","ZTP Trading","ZTP","Entry+Exit",ie=vfs["invoice_eur"],iv=vfs.get("invoice_qty"),ee=vfs["equinor_eur"],ev=vfs.get("equinor_qty"),de=vfs["eur_diff"],dp=vfs["qty_diff_pct"],st_=vfs["verdict"]))
    for f in vff:
        af.append(_f(ack,"Variable Trading Fee",f["gas_day"],"ZTP","Exit",iv=f.get("inv_qty"),ev=f.get("eq_qty"),d=f.get("qty_diff"),dp=f.get("qty_diff_pct"),u="MWh",fd=f.get("flag",""),
                     ie_=f.get("ieur"),iv_=f.get("inv_qty"),ee_=f.get("eeur"),ev_=f.get("eq_qty"),de_=f.get("deur")))
    for e in eis:
        di="Entry" if "Entry" in e.get("category","") else ("Exit" if "Exit" in e.get("category","") else "")
        ar.append(_u(ack,"Energy in Cash",e.get("category",""),e.get("category",""),di,ie=e.get("invoice_eur"),iv=e.get("invoice_qty"),ee=e.get("equinor_eur"),ev=e.get("equinor_qty"),de=e.get("eur_diff"),dp=e.get("qty_diff_pct"),st_=e.get("verdict","Unknown")))
    for f in eif:
        af.append(_f(ack,"Energy in Cash",f["gas_day"],f.get("category",""),"",iv=f.get("inv_qty"),ev=f.get("eq_qty"),d=f.get("qty_diff"),dp=f.get("qty_diff_pct"),u="kWh",fd=f.get("flag",""),
                     ie_=f.get("ieur"),iv_=f.get("inv_qty"),ee_=f.get("eeur"),ev_=f.get("eq_qty"),de_=f.get("deur")))
    for a in als:
        if a["line"]=="Purchases":
            ar.append(_u(sck,"Allocation Settlement","Purchases","End User Domestic","Purchase",ie=a["invoice_eur"],iv=a.get("invoice_kwh"),ee=a.get("equinor_eur"),ev=a.get("equinor_kwh"),de=a.get("diff_eur"),dp=a.get("diff_pct"),st_=a.get("status","Unknown")))
    asb=[]
    for a in als:
        if "Sales" in a.get("line",""):
            asb.append(_u(sck,"Allocation Settlement","Sales (Self-Bill)","End User Domestic","Sale",ie=a["invoice_eur"],iv=a.get("invoice_kwh"),ee=a.get("equinor_eur"),ev=a.get("equinor_kwh"),de=a.get("diff_eur"),dp=a.get("diff_pct"),st_=a.get("status","Unknown")))
    # notebook: alloc flags go into the normal flag stream (routed by settlement month); be_alloc_flagged stays empty
    for f in alf:
        af.append(_f(sck,"Allocation Settlement",f["gas_day"],"End User Domestic","",iv=f.get("invoice_value"),ev=f.get("equinor_value"),d=f.get("diff"),dp=f.get("diff_pct"),u="EUR",fd=f.get("flag",""),
                     ie_=f.get("ieur"),iv_=f.get("ivol"),ee_=f.get("eeur"),ev_=f.get("evol"),de_=f.get("deur")))
    afv=[]
    # Fixed fees
    il=df_all["product_name"].apply(lambda p:p.startswith(_LTC_VALID) or "Auction" in p or "Premium" in p)
    iv=(df_all["location"]=="Variable Fee")&(df_all["invoice_line_group"]=="ZTP Trading")
    ie=df_all["location"]=="Energy in Cash"
    ia=df_all["product_name"].str.contains("Allocation Settlement",case=False,na=False)&~df_all["product_name"].str.contains("transm|imbal",case=False,na=False)
    um=df_all[~(il|iv|ie|ia)]
    if len(um)>0:
        for _,u in um.groupby(["product_name","location","direction"],dropna=False,as_index=False).agg(ie_=("billed_amount","sum"),iv_=("qty","sum"),bs_=("billing_start","first")).iterrows():
            bm=u["bs_"].replace(day=1) if u["bs_"] else inv_m
            # qty on a flat fee (ZTP Trading Fixed Fee, Zee Platform) is a count of 1, not a volume -> no volume.
            # Real volumes on other unmatched lines (e.g. capacity exceeding, qty 8,947) are kept.
            fv=None if (pd.isna(u["iv_"]) or abs(float(u["iv_"])-1)<1e-9) else round(float(u["iv_"]),2)
            ar.append(_u(str(bm),"Fixed Fee",u["product_name"],u["location"],u["direction"],ie=round(u["ie_"],2),iv=fv,st_="No Equinor Match"))
            af.append(_f(str(bm),"Fixed Fee",str(bm),u["location"],u["direction"] or "",iv=round(u["ie_"],2),u="EUR",fd=f"No Equinor match \u2014 {u['product_name']}",
                       ie_=round(u["ie_"],2),iv_=fv))
    def _route(rows):
        m,m1,m3,o=[],[],[],[]
        for r in rows:
            bm=_parse_bm(r.get("billing_month"))
            if bm==inv_m: m.append(r)
            elif bm==inv_m1: m1.append(r)
            elif bm is not None and bm<inv_m1: m3.append(r)
            else: o.append(r)
        return m,m1,m3,o
    mr,m1r,m3r,orr=_route(ar); mf,m1f,m3f,_=_route(af)
    def _sv(tbl,rows):
        exec_sql(f"DELETE FROM {SCHEMA}.{tbl} WHERE check_month='{ck}'")
        if not rows: return
        df=pd.DataFrame(rows); cs=df.columns.tolist()
        vs=["("+",".join(sql_lit(r[c]) for c in cs)+")" for _,r in df.iterrows()]
        for i in range(0,len(vs),200):
            chunk = ",".join(vs[i:i+200])
            exec_sql(f"INSERT INTO {SCHEMA}.{tbl} ({','.join(cs)}) VALUES {chunk}")
    log(f"Saving: be_m={len(mr)}, be_m-1={len(m1r)}, be_m-3={len(m3r)}, alloc={len(asb)}")
    _sv("be_m",mr); _sv("be_m_minus_1",m1r); _sv("be_m_minus_3",m3r); _sv("be_alloc",asb); _sv("be_other",orr)
    _sv("be_m_flagged",mf); _sv("be_m_minus_1_flagged",m1f); _sv("be_m_minus_3_flagged",m3f); _sv("be_alloc_flagged",afv)
    # --- Run history: snapshot of summaries + every flag, keyed by run_id / run_timestamp (append-only) ---
    hs,hf=[],[]
    for per,rows in (("M",mr),("M-1",m1r),("M-3",m3r),("Alloc",asb),("Other",orr)):
        for r in rows:
            h={k:r.get(k) for k in ("check_month","invoice_file","invoice_month","billing_month","type","product_name","location","direction","invoice_eur","invoice_volume","equinor_eur","equinor_volume","diff_eur","diff_pct","status","endur_deal_numbers")}
            h.update(run_id=rid,run_timestamp=ts,period=per); hs.append(h)
    for per,rows in (("M",mf),("M-1",m1f),("M-3",m3f),("Alloc",afv)):
        for r in rows:
            h={k:r.get(k) for k in ("check_month","invoice_file","billing_month","type","gas_day","location","direction","invoice_eur","invoice_volume","equinor_eur","equinor_volume","diff_eur","diff_pct","flag_detail","endur_deal_numbers")}
            h.update(flag_id=str(uuid.uuid4()),run_id=rid,run_timestamp=ts,run_by=run_by,period=per); hf.append(h)
    if rid:
        def _ins(tbl,rows):
            if not rows: return
            df=pd.DataFrame(rows); cs=df.columns.tolist()
            vs=["("+",".join(sql_lit(r[c]) for c in cs)+")" for _,r in df.iterrows()]
            for i in range(0,len(vs),200):
                exec_sql(f"INSERT INTO {SCHEMA}.{tbl} ({','.join(cs)}) VALUES {','.join(vs[i:i+200])}")
        log(f"Saving run history: {len(hs)} summary rows, {len(hf)} flags")
        _ins("be_runhistory_summary",hs); _ins("be_runhistory",hf)
    # --- Gas-day detail (be_daily): every day of the daily items, replaced on every run ---
    dly=[]
    def _dl(sec,bm,item,unit,rows):
        for r in rows or []:
            iv,ie,ev,ee=_fl(r.get("invoice_volume")),_fl(r.get("invoice_eur")),_fl(r.get("equinor_volume")),_fl(r.get("equinor_eur"))
            ip=_fl(r.get("invoice_price")) if "invoice_price" in r else (ie/iv if (ie is not None and iv) else None)
            ep=_fl(r.get("equinor_price")) if "equinor_price" in r else (ee/ev if (ee is not None and ev) else None)
            dv=(ev-iv) if (ev is not None and iv is not None) else None
            de=(ee-ie) if (ee is not None and ie is not None) else None
            dly.append(dict(check_month=ck,invoice_file=inv_file,billing_month=str(bm) if bm else None,section=sec,item=item,unit=unit,gas_day=r.get("gas_day"),
                invoice_volume=iv,invoice_eur=ie,invoice_price=ip,equinor_volume=ev,equinor_eur=ee,equinor_price=ep,
                diff_volume=dv,diff_eur=de,diff_pct=(dv/abs(iv)*100 if (dv is not None and iv) else None),
                flag=(str(r.get("flag") or "").replace("\u26a0\ufe0f","").strip() or None),detail=r.get("detail"),run_id=rid,run_timestamp=ts))
    _EIC={"Entry at Interconnection Point":"Energy in Cash - Entry IP","Exit at Domestic Point":"Energy in Cash - Exit Dom","Exit at Interconnection Point":"Energy in Cash - Exit IP"}
    for e in eis: _dl("M-3",ack,_EIC.get(e.get("category"),"Energy in Cash - "+str(e.get("category"))),"kWh",e.get("daily"))
    if vfs: _dl("M-3",ack,"ZTP Trading","MWh",vfs.get("daily"))
    for a in als:
        if a["line"]=="Purchases": _dl("M-3",sck,"Alloc Settlement Buy","kWh",a.get("daily"))
        elif "Sales" in a.get("line",""): _dl("Alloc",sck,"Alloc Settlement Sale","kWh",a.get("daily"))
    for r in lc:  # Season capacity: one item per route/direction/capacity type
        if r.get("daily"):
            bm_=_parse_bm(r.get("billing_month"))
            sec="M" if bm_==inv_m else ("M-1" if bm_==inv_m1 else "M-3")
            _dl(sec,r["billing_month"],f"Capacity {r['location']} {r['direction']} {r['capacity_type']} {r.get('service_rate_type_','Season')}","kWh/h",r["daily"])
    log(f"Saving gas-day detail: {len(dly)} rows")
    _sv("be_daily",dly)
    # --- Overview (variable costs): one row per bill item for this invoice month, replaced on every run ---
    ov=[]
    def _ov(bm,item,unit,iv,ie,ev,ee,stt):
        iv,ie,ev,ee=_fl(iv),_fl(ie),_fl(ev),_fl(ee)
        if iv is None and ie is None and ev is None and ee is None: return
        de=(ee-ie) if (ee is not None and ie is not None) else None
        ov.append(dict(check_month=ck,invoice_file=inv_file,billing_month=str(bm) if bm else None,item_group="Variable cost",item=item,volume_unit=unit,
            invoice_volume=iv,invoice_eur=ie,invoice_price=(ie/iv if (ie is not None and iv) else None),
            equinor_volume=ev,equinor_eur=ee,equinor_price=(ee/ev if (ee is not None and ev) else None),
            diff_eur=de,diff_pct=(de/abs(ie)*100 if (de is not None and ie) else None),status=stt,run_id=rid,run_timestamp=ts))
    for a in als:
        if a["line"]=="Purchases":
            _ov(sck,"Alloc Settlement Buy","kWh",a.get("invoice_kwh"),a.get("invoice_eur"),a.get("equinor_kwh"),a.get("equinor_eur"),a.get("status"))
    _EIC={"Entry at Interconnection Point":"Energy in Cash - Entry IP","Exit at Domestic Point":"Energy in Cash - Exit Dom","Exit at Interconnection Point":"Energy in Cash - Exit IP"}
    for e in eis:
        _ov(ack,_EIC.get(e.get("category"),"Energy in Cash - "+str(e.get("category"))),"kWh",e.get("invoice_qty"),e.get("invoice_eur"),e.get("equinor_qty"),e.get("equinor_eur"),e.get("verdict"))
    if vfs:
        _ov(ack,"ZTP Trading","MWh",vfs.get("invoice_qty_all"),vfs.get("invoice_eur"),vfs.get("equinor_qty_all"),vfs.get("equinor_eur"),vfs.get("verdict"))
    if ov:
        tie=sum(r["invoice_eur"] or 0 for r in ov)
        tee=sum(r["equinor_eur"] or 0 for r in ov) if any(r["equinor_eur"] is not None for r in ov) else None
        _ov(ack,"Total variable cost","EUR",None,tie,None,tee,None)
    log(f"Saving overview: {len(ov)} rows")
    _sv("be_overview",ov)
    log("All results saved.")
    counts=dict(be_m=len(mr),be_m_minus_1=len(m1r),be_m_minus_3=len(m3r),be_alloc=len(asb),flags=len(mf)+len(m1f)+len(m3f)+len(afv))
    return counts,hf,ts

# === DISPLAY HELPERS ===
def _num(v):
    try:
        f=float(v)
        return None if pd.isna(f) else f
    except (TypeError,ValueError): return None

def _n2(v):
    f=_num(v)
    return "" if f is None else f"{f:,.2f}"

def describe_flag(f):
    """One-line, human-readable description of a flag row."""
    fd=str(f.get("flag_detail") or "").replace("\u26a0\ufe0f","").strip()
    where=" ".join(x for x in [str(f.get("direction") or ""),str(f.get("location") or "")] if x).strip()
    s=f"{f.get('type','')}"+(f" | {where}" if where else "")+f" ({f.get('gas_day','')}): {fd}"
    ie,ee,dp=_n2(f.get("invoice_eur")),_n2(f.get("equinor_eur")),_n2(f.get("diff_pct"))
    iv,ev=_n2(f.get("invoice_volume")),_n2(f.get("equinor_volume"))
    if ie or ee: s+=f" \u2014 invoice EUR {ie or 'n/a'} vs Equinor EUR {ee or 'n/a'}"
    if iv or ev: s+=f"; invoice volume {iv or 'n/a'} vs Equinor volume {ev or 'n/a'}"
    if dp: s+=f" ({dp}%)"
    return s

def _select_col(help_):
    """v1.17.1: 'Select' tick-box column that works on old and new Streamlit (the app's installed version has no
    'pinned' and only named widths) - newer options are only passed when this Streamlit supports them."""
    import inspect
    p=inspect.signature(st.column_config.CheckboxColumn).parameters
    kw=dict(width=62,pinned=False) if "pinned" in p else dict(width="small")
    return st.column_config.CheckboxColumn("Select",help=help_,default=False,**kw)

def select_table(df, style_fn, key, multi=False, help_="Tick to see more information about this row"):
    """v1.16.1: table with a visible, titled 'Select' tick-box column on the left (replaces st.dataframe row selection,
    whose tick-box column has no header). Single mode keeps only the most recently ticked row.
    Returns the selected row positions (0-based, in df order)."""
    sk,vk,ik=f"{key}__sel",f"{key}__v",f"{key}__init"
    d=df.reset_index(drop=True).copy()
    init=[i for i in st.session_state.get(ik,[]) if i<len(d)]
    d.insert(0,"Select",[i in init for i in range(len(d))])
    ed=st.data_editor(style_fn(d),key=f"{key}_{st.session_state.get(vk,0)}",hide_index=True,use_container_width=True,
                      num_rows="fixed",disabled=[c for c in d.columns if c!="Select"],
                      column_config={"Select":_select_col(help_)})
    now=[i for i,v in enumerate(ed["Select"].tolist()) if v]
    prev=st.session_state.get(sk,[])
    if not multi and len(now)>1:   # keep only the newest tick
        keep=([i for i in now if i not in prev] or now)[-1:]
        st.session_state[sk]=st.session_state[ik]=keep; st.session_state[vk]=st.session_state.get(vk,0)+1; st.rerun()
    st.session_state[sk]=now
    return now

def _verdict_to_status(ds):
    """v1.18.3: VERIFIED / REVIEW REQUIRED (Variable Fees M-3, Self Bill) -> a 'status' like the capacity tables:
    'OK', 'OK \u00b7 \u0394 -7.28 EUR (within 1 %)' or '\u0394 -564.27 EUR \u00b7 qty -0.13 %'. TECHNICAL / NOTICE texts are kept."""
    ds=ds.copy()
    def _one(r):
        v=str(r.get("verdict") or ""); g=r.get("eur_diff"); q=r.get("qty_diff_pct")
        g=None if g is None or pd.isna(g) else float(g); q=None if q is None or pd.isna(q) else float(q)
        if "TECHNICAL" in v or "NOTICE" in v or v.startswith("No "): return v
        qs=f" \u00b7 qty {q:+.2f} %" if q is not None and abs(q)>=0.005 else ""
        if v.upper().startswith("VERIFIED") or v=="OK":
            return "OK" if g is None or abs(g)<1 else f"OK \u00b7 \u0394 {g:+,.2f} EUR (within {FLAG_PCT:.0f} %)"
        if g is not None: return f"\u0394 {g:+,.2f} EUR"+qs
        return v or "Unknown"
    ds["verdict"]=[_one(r) for r in ds.to_dict("records")]
    return ds.rename(columns={"verdict":"status"})

def _st_rank(v):
    """0 = OK, 1 = notice, 2 = technical, 3 = red (gap / no match / review)."""
    s=str(v or "")
    if "TECHNICAL" in s: return 2
    if "NOTICE" in s: return 1
    if "OK" in s or "VERIFIED" in s: return 0
    return 3

def _aggregate_rows(ds, collapse="capacity_type"):
    """v1.16.0: Results tables show ONE row per billing month + direction + line type + location (+ item), like the
    PDF invoice: e.g. ZPT Entry 'Firm LongTerm' + 'Firm Season' -> 'Firm Total'. Numbers are summed (percentages
    dropped), deal numbers merged. Returns (aggregated df, list of component rows per output row) so the
    drill-down can still show each underlying line."""
    recs=ds.to_dict("records")
    if len(ds)==0 or collapse not in ds.columns: return ds,[[r] for r in recs]
    num=[c for c in ds.columns if pd.api.types.is_numeric_dtype(ds[c])]
    stc=[c for c in ds.columns if "status" in c.lower() or "verdict" in c.lower()]
    key=[c for c in ds.columns if c not in num+stc+[collapse,"endur_deal_numbers"]]
    _k=lambda v: None if v is None or (isinstance(v,float) and pd.isna(v)) else v
    groups={}
    for r in recs: groups.setdefault(tuple(_k(r[c]) for c in key),[]).append(r)
    gap_c=next((c for c in ("gap_eur","eur_diff","diff_eur") if c in ds.columns),None)
    out=[]; comps=[]
    for g in groups.values():
        comps.append(g)
        if len(g)==1: out.append(g[0]); continue
        a=dict(g[0])
        for c in num:
            vals=[x[c] for x in g if pd.notna(x[c])]
            a[c]=(sum(vals) if vals else None) if "pct" not in c.lower() else None
        w={str(x[collapse]).split()[0] for x in g if x[collapse]}
        a[collapse]=f"{w.pop()} Total" if len(w)==1 else "Total"
        dl=[d.strip() for x in g for d in str(x.get("endur_deal_numbers") or "").split(",") if d.strip()]
        if "endur_deal_numbers" in a: a["endur_deal_numbers"]=", ".join(dict.fromkeys(dl)) or None
        for c in stc:
            ss=[x[c] for x in g]
            if len(set(ss))==1: a[c]=ss[0]; continue
            worst=max(ss,key=_st_rank)
            if _st_rank(worst)<3: a[c]=worst; continue
            gap=a.get(gap_c) if gap_c else None
            if any(str(s).startswith("No ") for s in ss): a[c]=f"{next(s for s in ss if str(s).startswith('No '))} (part of total)"
            elif gap is not None and abs(gap)>=1: a[c]=f"\u0394 {gap:+,.2f} EUR"
            else: a[c]="REVIEW \u00b7 lines differ"
        out.append(a)
    return pd.DataFrame(out,columns=ds.columns),comps

def _sc(val):
    if val and "TECHNICAL" in str(val): return f"background-color:{FLAG_COLOURS['technical']}"  # v1.14.0
    if val and "NOTICE" in str(val): return f"background-color:{FLAG_COLOURS['notice']}"
    if val and ("OK" in str(val) or "VERIFIED" in str(val)): return "background-color:#c8e6c9"
    if val and ("D " in str(val) or "\u0394" in str(val) or "REVIEW" in str(val) or "No" in str(val)): return "background-color:#ffcdd2"
    return ""

def styled(df, num_cols, status_cols=()):
    """Thousands separators + 2 dp on numeric columns; colour status/verdict cells."""
    sty=df.style.format({c:"{:,.2f}" for c in num_cols}, na_rep="")
    if status_cols: sty=sty.map(_sc, subset=list(status_cols))
    return sty

# --- Display column titles: "invoice_kwh_h" -> "Invoice KWH/H", "gap_eur" -> "Gap EUR", "diff_pct" -> "Diff %" ---
_UNIT_WORDS = {"eur":"EUR","kwh":"KWH","mwh":"MWH","nok":"NOK","pct":"%","id":"ID"}

def pretty_col(c):
    s = re.sub(r"(kwh|mwh)_h\b", lambda m: m.group(1) + "/h", str(c).lower())
    out = []
    for w in s.split("_"):
        if not w: continue
        if "/" in w: out.append(w.upper())            # kwh/h -> KWH/H
        elif w in _UNIT_WORDS: out.append(_UNIT_WORDS[w])
        else: out.append(w.capitalize())
    return " ".join(out)

def status_first(df):
    """Move status / verdict column(s) to the far left so they are always visible."""
    dc = [c for c in df.columns if "deal_number" in c.lower()]  # Endur deal numbers far right
    sc = [c for c in df.columns if ("status" in c.lower() or "verdict" in c.lower()) and c not in dc]
    return df[sc + [c for c in df.columns if c not in sc and c not in dc] + dc]

def for_display(df, num_cols=(), status_cols=()):
    """Status first + pretty titles. Returns (df, renamed num_cols, renamed status_cols)."""
    df = status_first(df)
    ren = {c: pretty_col(c) for c in df.columns}
    return df.rename(columns=ren), [ren[c] for c in num_cols if c in ren], [ren[c] for c in status_cols if c in ren]

def _fmt_month(m):
    try: return pd.to_datetime(m).strftime("%B %Y")
    except Exception: return str(m)

def _ts_label(t):
    try: return pd.to_datetime(t).strftime("%Y-%m-%d %H:%M:%S")+" UTC"
    except Exception: return str(t)

def render_run_summary(lr):
    st.subheader("Verification summary")
    st.caption(f"Run {lr['rid']} \u00b7 {_ts_label(lr['ts'])} \u00b7 {lr['bill']}")
    fl=lr["flags"]
    st.caption(f"Flag tolerance: {FLAG_PCT:.0f} %.")  # v1.18.1: shortened note
    # v1.14.0: red issues / orange technical issues / yellow notices
    lv=[_flag_level(f.get("flag_detail")) for f in fl]
    tec=[f for f,l in zip(fl,lv) if l=="technical"]; ntc=[f for f,l in zip(fl,lv) if l=="notice"]
    fl=[f for f,l in zip(fl,lv) if l=="issue"]
    if not fl: st.success(f"No flagged issues \u2014 every check matched within the {FLAG_PCT:.0f} % tolerance.")
    def _box(kind,head,items):
        g=pd.DataFrame(items); g["fd"]=g["flag_detail"].astype(str).str.replace("\u26a0\ufe0f","").str.strip()
        for c_ in ("location","direction"): g[c_]=g[c_].fillna("") if c_ in g else ""
        g=g.groupby(["period","type","direction","location","fd"],as_index=False).agg(n=("gas_day","size"),a=("gas_day","min"),b=("gas_day","max"))
        li="".join(f"<li><b>{x.period}</b> \u00b7 {x.type} {x.direction} {x.location}: {x.fd}"
                   +(f" \u2014 {x.n} days ({x.a} to {x.b})" if x.n>1 else f" ({x.a})")+"</li>" for x in g.itertuples())
        st.markdown(f"<div style='background:{FLAG_COLOURS[kind]};padding:10px 14px;border-radius:6px;margin-bottom:8px'>"
                    f"<b>{head}</b><ul style='margin:4px 0 0 0'>{li}</ul></div>",unsafe_allow_html=True)
    if tec: _box("technical",f"{len(tec)} technical issue(s) \u2014 data missing or incomplete. What could be checked matches, so this is not an invoice error, but some numbers are not fully verified.",tec)
    if ntc: _box("notice",f"{len(ntc)} notice(s) \u2014 something unusual on the invoice. The numbers match; just make sure it is expected.",ntc)
    if not fl: return
    st.error(f"{len(fl)} flagged issue(s) found. Review below, then add comments in the Run History tab.")
    cnt=pd.DataFrame(fl).groupby(["period","type"]).size().reset_index(name="flags")
    cnt["period"]=pd.Categorical(cnt["period"],["M","M-1","M-3","Alloc"],ordered=True)
    st.dataframe(cnt.sort_values(["period","type"]),hide_index=True)
    shown=fl[:30]
    st.markdown("\n".join(f"- **{f['period']}** \u00b7 {describe_flag(f)}" for f in shown))
    if len(fl)>30:
        with st.expander(f"Show remaining {len(fl)-30} flags"):
            st.markdown("\n".join(f"- **{f['period']}** \u00b7 {describe_flag(f)}" for f in fl[30:]))

# === ENDUR DEAL PANEL (Results tab: click a row -> raw Endur data behind it) ===
_ENDUR_TYPES = ("LongTerm","Season","AuctionPremium")

def _month_bounds(v):
    try:
        d=pd.to_datetime(str(v)[:10]).date().replace(day=1)
        return d, d.replace(day=calendar.monthrange(d.year,d.month)[1])
    except Exception: return None, None

def render_endur_panel(row, key):
    """Raw, unprocessed Endur rows (deal + profilevolume + location) for the selected table row."""
    dn=row.get("endur_deal_numbers"); typ=str(row.get("line_type") or row.get("type") or "")
    loc=row.get("location"); dirn=row.get("direction")
    ms,me=_month_bounds(row.get("billing_month") or row.get("gas_day"))
    deals=[int(x) for x in re.findall(r"\d+",dn)] if isinstance(dn,str) else []
    fallback=(not deals) and loc in LOCATION_MAP and (typ.startswith("LTC") or typ in _ENDUR_TYPES)
    with st.container(border=True):
        if not deals and not fallback:
            st.info("This row is not based on Endur capacity deals (e.g. variable fee, energy in cash, allocation or fixed fee), so there is nothing to show from Endur.")
            return
        what=f"{dirn or ''} {loc or ''}".strip()
        mlabel=ms.strftime("%B %Y") if ms else "all months"
        if deals:
            st.markdown(f"#### Endur deals for {what} \u00b7 {mlabel}")
            st.caption(f"Deal numbers: {', '.join(str(x) for x in deals)}")
            where=f"d.deal_number IN ({','.join(str(x) for x in deals)})"
        else:
            st.markdown(f"#### All Endur capacity deals at {loc} ({LOCATION_MAP[loc]}) \u00b7 {mlabel}")
            st.caption("No Endur deal numbers on this row, so this shows every Entry and Exit capacity deal at this location \u2014 useful to see why nothing matched.")
            where=f"l.ng_location_id='{LOCATION_MAP[loc]}' AND d.instrument_type_name IN ('COMM-CAP-ENTRY','COMM-CAP-EXIT')"
        per=f" AND pv.start_date<='{me}' AND pv.end_date>='{ms}'" if ms else ""  # always just the selected billing month
        q=("SELECT d.deal_number,d.reference,d.instrument_type_name,d.service_type,d.buy_sell_name,d.tran_status,"
           "d.deal_start_date,d.deal_end_date,d.start_time,d.end_time,d.trade_date,d.para_position,"
           "l.location_name,l.ng_location_id,pv.settlement_type_id,pv.start_date,pv.end_date,pv.price,"
           "pv.calculated_profile_volume_kwh,pv.calculated_profile_volume,pv.calculated_payment,"
           "pv.unit_id,pv.price_unit_id,pv.volume_type_name,el.long_name AS counterparty "
           "FROM ms_atlas.endur_standard.deal_v2latest d "
           "JOIN ms_atlas.endur_standard.profilevolume_v2latest pv ON d.deal_number=pv.deal_number "
           "JOIN ms_atlas.endur_standard.location_v2latest l ON pv.location_id=l.location_id "
           "LEFT JOIN ms_atlas.endur_standard.party_v2latest el ON pv.external_le_id=el.party_id "
           f"WHERE {where}{per} ORDER BY d.deal_number,pv.start_date,pv.settlement_type_id,pv.price LIMIT 5000")
        with st.spinner("Querying Endur..."):
            raw=run_sql(q)
        if len(raw)==0:
            st.warning("No Endur rows found for this selection.")
            _render_excluded_secondary(loc,dirn,ms,me); return
        raw=to_num(raw,["para_position","price","calculated_profile_volume_kwh","calculated_profile_volume","calculated_payment"])
        hc=["deal_number","reference","counterparty","instrument_type_name","service_type","buy_sell_name","tran_status","deal_start_date","deal_end_date","start_time","end_time","trade_date","para_position"]
        st.markdown("**Deal header** (`deal_v2latest`)")
        st.dataframe(raw[hc].drop_duplicates(),use_container_width=True,hide_index=True)
        pc=["deal_number","location_name","ng_location_id","settlement_type_id","start_date","end_date","price","calculated_profile_volume_kwh","calculated_profile_volume","calculated_payment","unit_id","price_unit_id","volume_type_name"]
        st.markdown(f"**Profile rows** (`profilevolume_v2latest`, {len(raw)} rows)")
        st.dataframe(raw[pc],use_container_width=True,hide_index=True)
        st.caption("Raw, unprocessed and live from Endur (as it is now, not as it was at the time of the run). "
                   "All rows are shown, including price = 0 rows and every settlement type; the check itself only uses "
                   "settlement_type_id = 1 with price \u2260 0 (plus zero-price PRI_SEC sells where applicable), "
                   "and only deals with Fluxys as counterparty.")
        if deals and "LongTerm" in str(row.get("capacity_type") or ""): _endur_calc(raw)
        _render_excluded_secondary(loc,dirn,ms,me)
        return raw

def _render_excluded_secondary(loc, dirn, ms, me):
    """v1.18.0: raw Endur rows of capacity deals at this location / direction / month that the check LEAVES OUT because
    the counterparty is not Fluxys (secondary trade with another shipper = transfer of use, not billed by Fluxys)."""
    if loc not in LOCATION_MAP or not ms: return
    it={"Entry":"COMM-CAP-ENTRY","Exit":"COMM-CAP-EXIT"}.get(str(dirn))
    itf=f"d.instrument_type_name='{it}'" if it else "d.instrument_type_name IN ('COMM-CAP-ENTRY','COMM-CAP-EXIT')"
    q=("SELECT d.deal_number,d.reference,el.long_name AS counterparty,d.instrument_type_name,d.service_type,d.buy_sell_name,"
       "d.tran_status,d.deal_start_date,d.deal_end_date,d.execution_venue,d.trade_date,d.para_position,"
       "l.location_name,l.ng_location_id,pv.settlement_type_id,pv.start_date,pv.end_date,pv.price,"
       "pv.calculated_profile_volume_kwh,pv.calculated_payment "
       "FROM ms_atlas.endur_standard.deal_v2latest d "
       "JOIN ms_atlas.endur_standard.profilevolume_v2latest pv ON d.deal_number=pv.deal_number "
       "JOIN ms_atlas.endur_standard.location_v2latest l ON pv.location_id=l.location_id "
       "LEFT JOIN ms_atlas.endur_standard.party_v2latest el ON pv.external_le_id=el.party_id "
       f"WHERE l.ng_location_id='{LOCATION_MAP[loc]}' AND {itf} AND d.tran_status='Validated' "
       f"AND d.service_type IN ('Firm','Interruptible') AND COALESCE(pv.external_le_id,-1)<>{FLUXYS_LE_ID} "
       f"AND pv.start_date<='{me}' AND pv.end_date>='{ms}' "
       "ORDER BY d.deal_number,pv.start_date,pv.settlement_type_id,pv.price LIMIT 2000")
    ex=run_sql(q)
    st.markdown("**Not included in the check \u2014 secondary trades with another shipper**")
    if len(ex)==0:
        st.caption(f"None: every {dirn or ''} capacity deal at {loc} this month has Fluxys as counterparty."); return
    ex=to_num(ex,["para_position","price","calculated_profile_volume_kwh","calculated_payment"])
    st.caption("Counterparty is another shipper, not Fluxys (transfer of use). These are settled with that shipper and "
               "are not on the Fluxys bill, so they are left out of the Equinor side. Raw rows, live from Endur.")
    hc=["deal_number","reference","counterparty","instrument_type_name","service_type","buy_sell_name","tran_status",
        "deal_start_date","deal_end_date","execution_venue","trade_date","para_position"]
    st.dataframe(ex[hc].drop_duplicates(),use_container_width=True,hide_index=True)
    pc=["deal_number","counterparty","location_name","settlement_type_id","start_date","end_date","price","calculated_profile_volume_kwh","calculated_payment"]
    st.dataframe(ex[pc],use_container_width=True,hide_index=True)

def _period_hours(s,e):
    return sum(_gasday_hours(s+timedelta(days=i)) for i in range((e-s).days+1))

def _endur_calc(raw):
    """v1.8.0: worked calculation behind the Equinor side - the priced rows the check uses, signed and netted."""
    c=raw[(raw["settlement_type_id"].astype(str)=="1")&(raw["price"]!=0)].copy()
    if len(c)==0: return
    c["s"]=pd.to_datetime(c["start_date"]).dt.date; c["e"]=pd.to_datetime(c["end_date"]).dt.date
    c["hours"]=[_period_hours(s,e) for s,e in zip(c["s"],c["e"])]
    c["dsf"]=[((e-s).days+1)*24.0/h if h else 1.0 for s,e,h in zip(c["s"],c["e"],c["hours"])]  # v1.13.3
    c["kwh"]=-c["calculated_profile_volume_kwh"]          # Endur stores buys negative: flip so buys are +
    c["kwh_h"]=c["kwh"]/c["hours"]
    big=c["price"].abs()>=0.01
    c["price_unit"]=big.map({True:"EUR/MWh",False:"EUR/kWh"})
    c["eur"]=c["kwh"]*c["price"]/big.map({True:1000.0,False:1.0})
    c["eur_mwh"]=c["price"].where(big,c["price"]*1000)
    dst_=bool((c["dsf"]!=1).any()); c["eur_day"]=c["eur"]*c["dsf"]
    v=c[["deal_number","reference","buy_sell_name","start_date","end_date","hours","kwh","kwh_h","price","price_unit","eur_mwh","eur"]+(["eur_day"] if dst_ else [])].copy()
    v["deal_number"]=v["deal_number"].astype(str)
    tot=dict(deal_number="Net",reference="",buy_sell_name="",start_date="",end_date="",hours=None,kwh=c["kwh"].sum(),
             kwh_h=c["kwh_h"].sum(),price=None,price_unit="",eur_mwh=(c["eur"].sum()/c["kwh"].sum()*1000 if c["kwh"].sum() else None),eur=c["eur"].sum())
    if dst_: tot["eur_day"]=c["eur_day"].sum()
    v=pd.concat([v,pd.DataFrame([tot])],ignore_index=True)
    v=v.rename(columns={"deal_number":"Deal","reference":"Reference","buy_sell_name":"Buy/Sell","start_date":"From","end_date":"To",
        "hours":"Hours","kwh":"Volume kWh","kwh_h":"Capacity kWh/h","price":"Endur price","price_unit":"Price unit","eur_mwh":"Price EUR/MWh","eur":"EUR",
        "eur_day":"EUR (Fluxys day basis)"})
    st.markdown("**How the Equinor side is calculated** (priced rows only, netted)")
    st.dataframe(v.style.format({"Hours":"{:,.0f}","Volume kWh":"{:,.0f}","Capacity kWh/h":"{:,.0f}","Endur price":"{:,.9f}","Price EUR/MWh":"{:,.6f}","EUR":"{:,.2f}","EUR (Fluxys day basis)":"{:,.2f}"},na_rep="")
                 .apply(lambda r:["font-weight:bold;background-color:#eeeeee"]*len(r) if r["Deal"]=="Net" else [""]*len(r),axis=1),
                 use_container_width=True,hide_index=True)
    st.caption("Volume = calculated_profile_volume_kwh with buys + and sells \u2212. Capacity = volume \u00f7 gas-day hours in the period. "
               "EUR = volume \u00d7 price (\u00f7 1000 when the price is EUR/MWh, i.e. OCUC/bilateral deals; PRISMA deals are EUR/kWh). "
               +("Clock-change month: Endur counts the 23 h / 25 h gas day by the hour, Fluxys bills it as a full day, so the check "
                 "compares EUR (Fluxys day basis) = EUR \u00d7 (days \u00d7 24) \u00f7 hours. " if dst_ else "")+
               "A buy and a sell of the same capacity at the same price cancel out. Zero-price PRI_SEC sell-backs (Virtualys) "
               "are not listed here; the check values them at the average price.")

# === INVOICE PANEL (Results tab: capacity row -> the Fluxys bill lines behind it + reconciliation) ===
_PF_OK=re.compile(r"^[A-Za-z0-9_*/+\-(). ]+$")

def parse_cap_lines(xml_bytes, groups=IC_GROUPS):
    """Every BilledQuantity on a bill (capacity groups by default; groups=None = all lines) with its full price
    formula (QTY, UP, BILLp, UNITp, SCmm, NYM ...)."""
    rows=[]
    for prod in ET.parse(io.BytesIO(xml_bytes)).getroot().iter("CustomerProduct"):
        pn=(prod.findtext("ProductName") or "").strip()
        for il in prod.findall(".//InvoiceLine[@InvoiceLineSequence]"):
            lg=(il.findtext("InvoiceLineGroup") or "").strip()
            if groups is not None and lg not in groups: continue
            ln=(il.findtext("InvoiceLine") or "").strip(); dt_=(il.findtext("InvoiceLineDetail") or "").strip(); bs=il.findtext("BillingStartDate")
            for bq in il.findall(".//BilledQuantity"):
                ai=bq.find("AdditionalInformation"); pfi=bq.find("PriceFormulaInformation")
                g=(lambda t,ai=ai: ai.findtext(t)) if ai is not None else (lambda t: None)
                vals={}; fml=qu=uu=None
                if pfi is not None:
                    for ch in pfi:
                        if ch.tag=="Formula": fml=ch.get("Formula"); continue
                        v=ch.get(ch.tag) if ch.get(ch.tag) is not None else ch.text
                        try: vals[ch.tag]=float(v)
                        except (TypeError,ValueError): pass
                    qe=pfi.find("QTY"); ue=pfi.find("UP")
                    qu=qe.get("QTYUnit") if qe is not None else None; uu=ue.get("UPUnit") if ue is not None else None
                rc=None
                if fml and _PF_OK.match(fml):
                    try: rc=float(eval(fml,{"__builtins__":{}},dict(vals)))  # arithmetic only (checked by _PF_OK)
                    except Exception: rc=None
                other=", ".join(f"{k}={v:g}" for k,v in vals.items() if k not in ("QTY","UP","BILLp","UNITp"))
                rows.append(dict(product=pn,group=lg,location=ln,detail=dt_,billing_start=bs,billing_type=(il.findtext("BillingType") or "").strip(),direction=g("Direction"),
                    rate_type=g("ServiceRateType"),capacity_type=g("CapacityType"),contract_ref=g("ContractReference"),
                    service_ref=g("ServiceReference"),service_start=g("ServiceStartGasDay"),service_end=g("ServiceEndGasDay"),
                    econ_start=bq.findtext("EconomicStartDate"),econ_end=bq.findtext("EconomicEndDate"),
                    qty=vals.get("QTY"),qty_unit=qu,unit_price=vals.get("UP"),up_unit=uu,bill_p=vals.get("BILLp"),unit_p=vals.get("UNITp"),
                    other=other,formula=fml,recalc_eur=rc,billed_eur=float(bq.findtext("BilledQuantityAmount") or 0),
                    cp=g("ConnectionPointName")))
    return pd.DataFrame(rows)

def _bill_files(mf):
    return run_sql(f"SELECT DISTINCT r.file_name,r.volume_path FROM {SCHEMA}.invoice_register r JOIN "
                   f"(SELECT invoice_file FROM {SCHEMA}.be_m WHERE check_month IN ({mf}) UNION "
                   f"SELECT invoice_file FROM {SCHEMA}.be_m_minus_1 WHERE check_month IN ({mf}) UNION "
                   f"SELECT invoice_file FROM {SCHEMA}.be_m_minus_3 WHERE check_month IN ({mf})) b ON r.file_name=b.invoice_file")

# === v1.12.0: invoice lines behind the daily items (Energy in Cash, ZTP Trading, Allocation Settlement) ===
def _bill_and_selfbill_files(mf):
    b=_bill_files(mf)
    if len(b)==0: return b
    s=run_sql(f"SELECT DISTINCT s.file_name,s.volume_path FROM {SCHEMA}.invoice_register r JOIN {SCHEMA}.invoice_register s "
              f"ON s.invoice_number=r.paired_invoice_number AND s.invoice_type='Self-Bill' "
              f"WHERE r.file_name IN ({','.join(sql_lit(x) for x in b['file_name'])})")
    return pd.concat([b,s],ignore_index=True) if len(s) else b

def _daily_invoice_lines(row, mf):
    """Bill / self-bill lines behind an Energy in Cash, ZTP Trading or Allocation Settlement row -> (DataFrame, message)."""
    item=_daily_item(row) or ""; bm=str(row.get("billing_month") or "")[:7]
    files=_bill_and_selfbill_files(mf)
    if len(files)==0: return pd.DataFrame(),"The bill for this month was not found in the Invoice Register."
    parts=[]
    for fn,vp in zip(files["file_name"],files["volume_path"]):
        try: d=parse_cap_lines(volume_read(vp),groups=None)
        except Exception as e: return pd.DataFrame(),f"Could not read {fn}: {e}"
        if len(d): d["bill"]=fn; parts.append(d)
    if not parts: return pd.DataFrame(),"No lines found on the bill."
    d=pd.concat(parts,ignore_index=True)
    mon=d["econ_start"].fillna(d["billing_start"]).astype(str).str[:7]
    if item.startswith("Energy in Cash"):
        m=(d["location"]=="Energy in Cash")&(d["group"]==str(row.get("item") or ""))
    elif item=="ZTP Trading":
        m=(d["group"]=="ZTP Trading")&(d["location"]=="Variable Fee")
    elif item.startswith("Alloc Settlement"):
        pl=d["product"].str.lower()
        m=pl.str.contains("allocation settl")&~pl.str.contains("transm|imbal")&pl.str.contains("sales" if "Sale" in item else "purchases")
    else: return pd.DataFrame(),"No invoice-line view for this row."
    d=d[m&(mon==bm)] if bm else d[m]
    return d,(None if len(d) else "No matching lines on the bill.")

def render_daily_invoice_panel(row, d, msg):
    with st.container():
        st.markdown(f"**Invoice lines for {row.get('item') or row.get('line_type')} \u00b7 {_fmt_month(row.get('billing_month'))}**")
        if msg: st.info(msg); return
        cols=["econ_start","cp","direction","detail","qty","qty_unit","unit_price","up_unit","other","formula","recalc_eur","billed_eur"]+(["bill"] if d["bill"].nunique()>1 else [])
        v=d.sort_values(["econ_start","cp"],na_position="last")[cols].copy()
        qu=d["qty_unit"].dropna().unique()
        tot={c:None for c in cols}; tot.update(econ_start="Total",qty=d["qty"].sum() if len(qu)<=1 else None,
                                               recalc_eur=d["recalc_eur"].sum(min_count=1),billed_eur=d["billed_eur"].sum())
        v=pd.concat([v,pd.DataFrame([tot])],ignore_index=True)
        v=v.rename(columns={"econ_start":"Gas day","cp":"Connection point","direction":"Direction","detail":"Detail","qty":"QTY","qty_unit":"QTY unit",
            "unit_price":"UP","up_unit":"UP unit","other":"Other factors","formula":"Formula","recalc_eur":"Recalculated EUR","billed_eur":"Billed EUR","bill":"Bill"})
        st.dataframe(v.style.format({"QTY":"{:,.3f}","UP":"{:,.6g}","Recalculated EUR":"{:,.2f}","Billed EUR":"{:,.2f}"},na_rep="")
                     .apply(lambda r:["font-weight:bold;background-color:#eeeeee"]*len(r) if r["Gas day"]=="Total" else [""]*len(r),axis=1),
                     use_container_width=True,hide_index=True)
        st.caption(f"{len(d)} line(s), raw from the bill XML (self-bill for allocation sales). One line per gas day and connection point; "
                   "Recalculated EUR = the bill's own formula re-applied, it should match Billed EUR to the cent. "
                   "The Gas-day breakdown adds these up per day.")

def _ask_ai_ui(title, mf, src_run, r_, raw_e_, il_, rec_, di_):
    """Ask AI button + result, kept per row (and run) for this session."""
    rid_="|".join(str(r_.get(k)) for k in ("billing_month","direction","location","line_type","capacity_type","item"))
    ak_="ai_"+hashlib.md5(f"{title}|{mf}|{src_run}|{rid_}".encode()).hexdigest()[:12]
    if st.button("Ask AI",key="btn_"+ak_,help="Sends this row, the invoice lines, the Endur / gas-day data and the Belgium checker knowledge base to an AI model and asks for the likely cause."):
        dly_=None
        if di_:
            dly_=run_sql(f"SELECT gas_day,flag,invoice_volume,invoice_price,invoice_eur,equinor_volume,equinor_price,equinor_eur,diff_volume,diff_eur,detail FROM {SCHEMA}.be_daily "
                         f"WHERE check_month IN ({mf}) AND item={sql_lit(di_)} AND billing_month={sql_lit(str(r_.get('billing_month')))} ORDER BY gas_day")
        with st.spinner("Asking AI (10-30 seconds)..."):
            try: st.session_state[ak_]=ask_ai(r_,raw_e_,il_,rec_ or None,dly_)
            except Exception as e: st.session_state[ak_]=f"AI call failed: {e}"
    if st.session_state.get(ak_):
        with st.expander("AI explanation \u25be",expanded=True):
            st.markdown(st.session_state[ak_])
            st.caption(f"AI-generated by {AI_MODEL} from the data above and BELGIUM_CHECKER_KNOWLEDGE.md. It can be wrong; check before acting. "
                       "Click Ask AI again for a fresh answer.")

def _match_cap_lines(d,row):
    loc,dirn=row.get("location"),row.get("direction"); bm=str(row.get("billing_month") or "")[:7]
    ct=str(row.get("capacity_type") or "").split(); cap=ct[0] if ct else None; rate=ct[-1] if len(ct)>1 else None
    base=d[(d["direction"]==dirn)&(d["billing_start"].astype(str).str[:7]==bm)]
    ia=base["group"].str.contains("Auction Premium")|base["detail"].str.contains("Auction|Premium",case=False,na=False)
    tar=base[~ia&(base["location"]==loc)]
    if rate:
        t2=tar[(tar["rate_type"]==rate)&((tar["capacity_type"]==cap)|tar["capacity_type"].isna())]
        if len(t2): tar=t2
    if "Auction" not in str(row.get("line_type") or ""): return tar
    # auction premium lines carry no location: link them to this location through the AUC- ids on its tariff lines (same rule as the check)
    ids=set(re.findall(r"AUC-(\d+)"," ".join(tar["contract_ref"].fillna(""))))
    auc=base[ia]
    return auc[auc["contract_ref"].fillna("").apply(lambda c: bool(ids&set(re.findall(r"AUC-(\d+)",c))))]

def _invoice_lines(row, mf):
    """Capacity lines on the bill(s) behind a Results row -> (DataFrame, message when there are none)."""
    files=_bill_files(mf)
    if len(files)==0: return pd.DataFrame(),"The bill for this month was not found in the Invoice Register."
    parts=[]
    for fn,vp in zip(files["file_name"],files["volume_path"]):
        try: d=parse_cap_lines(volume_read(vp))
        except Exception as e: return pd.DataFrame(),f"Could not read {fn}: {e}"
        if len(d): d["bill"]=fn; parts.append(d)
    d=_match_cap_lines(pd.concat(parts,ignore_index=True),row) if parts else pd.DataFrame()
    return d,(None if len(d) else "No matching capacity lines on the bill.")

def render_invoice_panel(row, d, msg):
    """v1.8.0: the raw Fluxys bill lines behind a capacity row, with the bill's formula re-applied."""
    with st.container():
        what=f"{row.get('direction') or ''} {row.get('location') or ''}".strip()
        st.markdown(f"**Invoice lines for {what} \u00b7 {_fmt_month(row.get('billing_month'))}**")
        if msg: st.info(msg); return
        cols=["contract_ref","service_ref","billing_type","service_start","service_end","econ_start","econ_end","rate_type","qty","qty_unit",
              "unit_price","up_unit","bill_p","unit_p","other","formula","recalc_eur","billed_eur"]+(["bill"] if d["bill"].nunique()>1 else [])
        v=d[cols].copy()
        tot={c:None for c in cols}; tot.update(contract_ref="Net",qty=d["qty"].sum() if d["econ_start"].nunique()<=1 else None,
                                               recalc_eur=d["recalc_eur"].sum(min_count=1),billed_eur=d["billed_eur"].sum())
        v=pd.concat([v,pd.DataFrame([tot])],ignore_index=True)
        v=v.rename(columns={"contract_ref":"Contract","service_ref":"Service ref","billing_type":"Billing type","service_start":"Service from","service_end":"Service to",
            "econ_start":"Billed from","econ_end":"Billed to","rate_type":"Rate type","qty":"QTY","qty_unit":"QTY unit","unit_price":"UP",
            "up_unit":"UP unit","bill_p":"BILLp (days)","unit_p":"UNITp","other":"Other factors","formula":"Formula",
            "recalc_eur":"Recalculated EUR","billed_eur":"Billed EUR","bill":"Bill"})
        st.dataframe(v.style.format({"QTY":"{:,.0f}","UP":"{:,.6g}","BILLp (days)":"{:,.0f}","UNITp":"{:,.0f}","Recalculated EUR":"{:,.2f}","Billed EUR":"{:,.2f}"},na_rep="")
                     .apply(lambda r:["font-weight:bold;background-color:#eeeeee"]*len(r) if r["Contract"]=="Net" else [""]*len(r),axis=1),
                     use_container_width=True,hide_index=True)
        st.caption("Raw from the bill XML (PriceFormulaInformation). Recalculated EUR = the bill's own formula re-applied to its QTY, UP and factors; "
                   "it should match Billed EUR to the cent. Negative QTY lines are capacity Fluxys moved off a contract (e.g. to an auction contract).")

def _decs(p):
    """Number of decimals a price is stored with (8 = not visibly rounded)."""
    for k in range(8):
        if abs(p-round(p,k))<1e-10: return k
    return 8

def _deal_contrib(raw, bill_eur_mwh):
    """v1.9.0: gap by Endur deal = signed MWh x (Endur EUR/MWh - bill EUR/MWh), priced settlement-type-1 rows.
    'rounded' = the Endur price is the bill tariff rounded to its own number of decimals."""
    if raw is None or len(raw)==0: return None
    c=raw[(raw["settlement_type_id"].astype(str)=="1")&(raw["price"]!=0)].copy()
    if len(c)==0: return None
    c["mwh"]=-c["calculated_profile_volume_kwh"].astype(float)/1000
    # v1.13.3: put Endur energy on the Fluxys per-day basis (24 h per day) so a 23 h / 25 h gas day is not a gap
    _s=pd.to_datetime(c["start_date"]).dt.date; _e=pd.to_datetime(c["end_date"]).dt.date
    c["mwh"]=c["mwh"]*[((e-s).days+1)*24.0/_period_hours(s,e) for s,e in zip(_s,_e)]
    c["eur_mwh"]=c["price"].where(c["price"].abs()>=0.01,c["price"]*1000)
    c["reference"]=c["reference"].fillna(""); c["buy_sell_name"]=c["buy_sell_name"].fillna("")
    g=c.groupby(["deal_number","reference","buy_sell_name","eur_mwh"],as_index=False,dropna=False)["mwh"].sum()
    g["decimals"]=g["eur_mwh"].apply(_decs)
    g["rounded"]=[(k<=5 and abs(round(bill_eur_mwh,k)-p)<1e-9) for p,k in zip(g["eur_mwh"],g["decimals"])]
    g["gap_eur"]=g["mwh"]*(g["eur_mwh"]-bill_eur_mwh)
    return g

def _reconcile(row,d,raw=None,show=True):
    """Split gap_eur into a volume effect and a price effect (LongTerm, yearly tariff), and the price effect by Endur deal.
    Returns False when not applicable. The comment box is rule-based (fixed rules below), not AI-generated."""
    if len(d)==0 or "LongTerm" not in str(row.get("capacity_type") or "") or "Auction" in str(row.get("line_type") or ""): return False
    ie,ee,iv,ev=(row.get(k) for k in ("invoice_eur","equinor_eur","invoice_kwh_h","endur_kwh_h"))
    if any(x is None or pd.isna(x) for x in (ie,ee,iv,ev)) or not ev: return False
    lt=d[d["bill_p"].notna()&d["qty"].notna()&d["up_unit"].fillna("").str.endswith("/year")]
    ms,me=_month_bounds(row.get("billing_month"))
    if len(lt)==0 or ms is None: return False
    inv_mwh=float((lt["qty"]*lt["bill_p"]*24).sum())/1000
    end_mwh=float(ev)*((me-ms).days+1)*24/1000  # v1.13.3: Fluxys per-day basis (24 h/day) on both sides
    if not inv_mwh: return False
    ip=float(ie)/inv_mwh; ep=float(ee)/end_mwh; yr=float(lt["unit_p"].dropna().iloc[0]) if lt["unit_p"].notna().any() else 365.0
    ups=sorted(lt["unit_price"].dropna().unique())
    vol_eff=(end_mwh-inv_mwh)*ip; pr_eff=end_mwh*(ep-ip); gap=float(ee)-float(ie)
    rec=dict(invoice_energy_mwh=round(inv_mwh,3),endur_energy_mwh=round(end_mwh,3),invoice_price_eur_mwh=round(ip,7),
             endur_price_eur_mwh=round(ep,7),volume_effect_eur=round(vol_eff,2),price_effect_eur=round(pr_eff,2),gap_eur=round(gap,2),
             rounding_limit_eur=round(end_mwh*0.00005,2))
    bill=(ups[0]*1000/(yr*24)) if len(ups)==1 else None
    dc=_deal_contrib(raw,bill) if bill else None
    if dc is not None:
        rec["bill_tariff_eur_mwh"]=round(bill,7)
        rec["gap_by_deal"]=[dict(deal=int(r.deal_number),reference=r.reference,buy_sell=r.buy_sell_name,signed_mwh=round(r.mwh,3),
            endur_eur_mwh=r.eur_mwh,price_decimals=int(r.decimals),is_bill_tariff_rounded=bool(r.rounded),gap_eur=round(r.gap_eur,2)) for r in dc.itertuples()]
        rec["gap_not_explained_by_deals_eur"]=round(pr_eff-float(dc["gap_eur"].sum()),2)
    t=pd.DataFrame([
        dict(Measure="Capacity (kWh/h)",Invoice=f"{float(iv):,.0f}",Endur=f"{float(ev):,.0f}",Difference=f"{float(ev)-float(iv):,.0f}"),
        dict(Measure="Energy basis (MWh)",Invoice=f"{inv_mwh:,.3f}",Endur=f"{end_mwh:,.3f}",Difference=f"{end_mwh-inv_mwh:,.3f}"),
        dict(Measure="Price (EUR/MWh)",Invoice=f"{ip:,.6f}",Endur=f"{ep:,.6f}",Difference=f"{ep-ip:,.6f}"),
        dict(Measure=f"Price (EUR/kWh/h/year, {yr:.0f} days)",Invoice=f"{ip*yr*24/1000:,.5f}",Endur=f"{ep*yr*24/1000:,.5f}",Difference=f"{(ep-ip)*yr*24/1000:,.5f}"),
        dict(Measure="EUR",Invoice=f"{float(ie):,.2f}",Endur=f"{float(ee):,.2f}",Difference=f"{gap:,.2f}")])
    if not show: return rec  # v1.9.1: numbers only (fed to Ask AI); the Invoice vs Endur section is no longer shown
    with st.container():
        st.dataframe(t,use_container_width=True,hide_index=True)
        tariff=(f" Bill tariff: {ups[0]:g} EUR/kWh/h/year = {ups[0]*1000/(yr*24):.6f} EUR/MWh." if len(ups)==1 else "")
        st.markdown(f"* **Volume effect:** {vol_eff:,.2f} EUR (energy difference \u00d7 invoice price)\n"
                    f"* **Price effect:** {pr_eff:,.2f} EUR (Endur energy \u00d7 price difference)\n"
                    f"* **Total:** {gap:,.2f} EUR")
        if dc is not None:
            v=dc[["deal_number","reference","buy_sell_name","mwh","eur_mwh","decimals","rounded","gap_eur"]].copy()
            v["deal_number"]=v["deal_number"].astype(str)
            rest=pr_eff-float(dc["gap_eur"].sum())
            if abs(rest)>=0.01: v=pd.concat([v,pd.DataFrame([dict(deal_number="Other",reference="not explained by priced deals (zero-price rows, cent rounding on the bill)",gap_eur=rest)])],ignore_index=True)
            v=pd.concat([v,pd.DataFrame([dict(deal_number="Total",gap_eur=pr_eff)])],ignore_index=True)
            v=v.rename(columns={"deal_number":"Deal","reference":"Reference","buy_sell_name":"Buy/Sell","mwh":"Signed MWh","eur_mwh":"Endur EUR/MWh",
                                "decimals":"Price decimals","rounded":"= bill tariff rounded","gap_eur":"Price effect EUR"})
            st.markdown(f"**Price effect by Endur deal** (bill tariff {bill:.7f} EUR/MWh)")
            st.dataframe(v.style.format({"Signed MWh":"{:,.3f}","Endur EUR/MWh":"{:,.9g}","Price decimals":"{:.0f}","Price effect EUR":"{:,.2f}"},na_rep="")
                         .apply(lambda r:["font-weight:bold;background-color:#eeeeee"]*len(r) if r["Deal"]=="Total" else [""]*len(r),axis=1),
                         use_container_width=True,hide_index=True)
        if abs(gap)<0.01: st.success("No difference."); return rec
        if abs(vol_eff)<0.01 and dc is not None:
            rnd=dc["rounded"]; cr=float(dc.loc[rnd,"gap_eur"].sum()); cn=float(dc.loc[~rnd,"gap_eur"].sum())
            if rnd.any() and abs(cn)<0.5 and abs(cr-pr_eff)<0.5:
                ex=", ".join(f"{int(r.deal_number)} at {r.eur_mwh:g}" for r in dc[rnd].drop_duplicates("deal_number").head(4).itertuples())
                st.info(f"Volumes match; the whole gap is price rounding. Endur holds the bill tariff rounded to "
                        f"{'/'.join(str(k) for k in sorted(set(dc.loc[rnd,'decimals'])))} decimals (deal {ex} EUR/MWh vs {bill:.6f} on the bill). "
                        f"Not a wrong tariff; it only goes away if Endur stores more decimals.{tariff}")
            else:
                top=dc.loc[dc["gap_eur"].abs().sort_values(ascending=False).index].head(3)
                ex="; ".join(f"{int(r.deal_number)} ({r.reference}) {r.eur_mwh:g} EUR/MWh: {r.gap_eur:,.2f} EUR" for r in top.itertuples() if abs(r.gap_eur)>=0.01)
                st.warning(f"Volumes match; the gap is price and is not just rounding. Biggest contributions: {ex or 'none from priced deals'}. "
                           f"Check the tariff on those Endur deals.{tariff}")
        elif abs(vol_eff)<0.01 and abs(pr_eff)<=end_mwh*0.00005+0.01:
            st.info(f"Volumes match; the whole gap is price. It is within the rounding of a 4-decimal EUR/MWh price "
                    f"(up to {end_mwh*0.00005:,.2f} EUR on this volume), so it is most likely Endur holding a rounded price, not a wrong tariff.{tariff}")
        elif abs(vol_eff)<0.01:
            st.warning(f"Volumes match; the gap is price and too large to be rounding \u2014 check the tariff on the Endur deal.{tariff}")
        elif abs(pr_eff)<0.01:
            st.warning("Prices match; the gap is volume \u2014 compare the Endur deals and Invoice lines sections.")
        else:
            st.warning(f"Both volume and price differ \u2014 see the effects above.{tariff}")
        st.caption("Invoice energy = \u03a3 QTY \u00d7 BILLp \u00d7 24 h. Endur energy = Endur kWh/h \u00d7 days \u00d7 24 h (Fluxys bills per day, so 23 h / 25 h clock-change days count as 24 h on both sides). "
                   "Volume effect + price effect = the gap exactly. The comment above is chosen by fixed rules on these numbers.")
    return rec

# === ASK AI (v1.9.0): send the row, Endur rows, bill lines, reconciliation + knowledge base to an LLM ===
_APP_DIR=os.path.dirname(os.path.abspath(__file__))

@st.cache_data(show_spinner=False)
def _knowledge():
    try:
        with open(os.path.join(_APP_DIR,"BELGIUM_CHECKER_KNOWLEDGE.md"),encoding="utf-8") as f: return f.read()
    except Exception: return "(knowledge base file not found)"

def _ai_query(prompt):
    """ai_query on the app's SQL warehouse, as the signed-in user; the prompt is a bound parameter (no escaping issues)."""
    w=_get_client()
    resp=w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID,statement=f"SELECT ai_query('{AI_MODEL}', :prompt) AS r",
        parameters=[StatementParameterListItem(name="prompt",value=prompt,type="STRING")],wait_timeout="50s")
    state=resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
    t0=time.time()
    while state in {"PENDING","RUNNING"} and time.time()-t0<180:
        time.sleep(2); resp=w.statement_execution.get_statement(resp.statement_id)
        state=resp.status.state.value if resp.status and resp.status.state else "UNKNOWN"
    if state!="SUCCEEDED": raise RuntimeError(f"AI call failed: {resp.status.error if resp.status else state}")
    return (resp.result.data_array or [[""]])[0][0] or ""

def _csv(df,cols=None,n=250):
    if df is None or len(df)==0: return "(none)"
    d=df[[c for c in (cols or df.columns) if c in df.columns]]
    extra=f"\n... {len(d)-n} more rows not shown" if len(d)>n else ""
    return d.head(n).to_csv(index=False)+extra

def ask_ai(row, raw_e, il, rec, daily):
    keep=["billing_month","direction","location","line_type","item","capacity_type","invoice_kwh_h","endur_kwh_h","invoice_eur","equinor_eur",
          "gap_eur","eur_diff","qty_diff_pct","status","verdict","endur_deal_numbers"]
    rj={k:(None if (isinstance(row.get(k),float) and pd.isna(row.get(k))) else row.get(k)) for k in keep if k in row}
    ecols=["deal_number","reference","instrument_type_name","service_type","buy_sell_name","tran_status","deal_start_date","deal_end_date",
           "start_time","end_time","location_name","ng_location_id","settlement_type_id","start_date","end_date","price",
           "calculated_profile_volume_kwh","calculated_payment"]
    icols=["contract_ref","service_ref","cp","direction","billing_type","detail","rate_type","capacity_type","service_start","service_end","econ_start","econ_end",
           "qty","qty_unit","unit_price","up_unit","bill_p","unit_p","other","formula","recalc_eur","billed_eur"]
    prompt=f"""You are helping a gas transport analyst at Equinor check a Fluxys Belgium invoice against Equinor's own data
(Endur capacity bookings for capacity rows; dispatch allocations, EEX ZTP prices and shipper volumes for Energy in Cash,
ZTP Trading and Allocation Settlement rows - see the knowledge base). For daily items the GAS-DAY BREAKDOWN is the comparison per day.
Use the KNOWLEDGE BASE to understand how the checker lines things up. Look at the ROW, the RECONCILIATION, the INVOICE LINES and the ENDUR ROWS
and explain the most likely reason for any difference. Also point out anything unusual even if the gap is zero (BackBilling lines,
Interruptible or over-nomination lines, negative/transfer lines, within-day bookings, clock-change month, PRI_SEC trades, auction premiums,
prices that look rounded or out of date, deals not Validated, cases the knowledge base says are not handled).
Rules: use only the data given; quote the actual deal numbers, contracts and numbers; show a short calculation where it helps;
do not invent data; if the data cannot explain the gap, say so and say what to look at next.
RECONCILIATION.gap_by_deal is exact arithmetic (signed MWh x (Endur EUR/MWh - bill tariff EUR/MWh)) and sums to the price effect;
use it rather than doing your own arithmetic. is_bill_tariff_rounded=true means the Endur price is the bill tariff rounded to
price_decimals decimals. The "Known cases" in the knowledge base can be out of date: if this row's data disagrees, trust the data and say so.
Answer in markdown, max about 200 words, with exactly these parts:
**Likely cause:** one or two sentences.
**Evidence:** 2-5 bullets.
**What to check or fix:** 1-3 bullets.
**Confidence:** High, Medium or Low, with a few words why.

=== KNOWLEDGE BASE ===
{_knowledge()}

=== ROW (Results table, amounts in EUR, capacity in kWh/h) ===
{json.dumps(rj,default=str)}

=== RECONCILIATION (LongTerm only; volume effect + price effect = gap) ===
{json.dumps(rec,default=str) if rec else "(not applicable for this row)"}

=== INVOICE LINES (from the bill XML; recalc_eur = bill formula re-applied) ===
{_csv(il,icols)}

=== ENDUR ROWS (deal + profilevolume, all settlement types, live from Endur) ===
{_csv(raw_e,ecols)}

=== GAS-DAY BREAKDOWN (Season rows only) ===
{_csv(daily,None,70)}
"""
    return _ai_query(prompt)

# === GAS-DAY PANEL (Results tab: select a daily item -> every gas day, invoice vs Equinor) ===
_EIC_ITEMS={"Entry at Interconnection Point":"Energy in Cash - Entry IP","Exit at Domestic Point":"Energy in Cash - Exit Dom","Exit at Interconnection Point":"Energy in Cash - Exit IP"}
_DAILY_NUM=["invoice_volume","invoice_eur","invoice_price","equinor_volume","equinor_eur","equinor_price","diff_volume","diff_eur","diff_pct"]

def _daily_item(row):
    """be_daily item for a summary row, or None if the row has no gas-day granularity (capacity, fixed fees)."""
    t=str(row.get("line_type") or row.get("type") or ""); it=str(row.get("item") or "")
    if t=="Energy in Cash": return _EIC_ITEMS.get(it,"Energy in Cash - "+it)
    if t=="Variable Trading Fee": return "ZTP Trading"
    if t=="Allocation Settlement": return "Alloc Settlement Sale" if "Sale" in it else "Alloc Settlement Buy"
    cty=str(row.get("capacity_type") or "")
    if t=="LTC Tariff" and "Season" in cty: return f"Capacity {row.get('location')} {row.get('direction')} {cty}"
    return None

def render_daily_panel(row, mf, key):
    item=_daily_item(row); bm=row.get("billing_month")
    with st.container(border=True):
        st.markdown(f"#### Gas-day breakdown \u00b7 {item} \u00b7 {_fmt_month(bm) if bm else ''}")
        q=(f"SELECT * FROM {SCHEMA}.be_daily WHERE check_month IN ({mf}) AND item={sql_lit(item)}"
           +(f" AND billing_month={sql_lit(str(bm))}" if bm else "")+" ORDER BY gas_day")
        d=run_sql(q)
        if len(d)==0:
            st.info("No gas-day detail saved for this month yet. Re-run the check for this month (gas-day detail is saved from v1.7.0 onwards).")
            return
        d=to_num(d,_DAILY_NUM); unit=str(d["unit"].dropna().iloc[0]) if d["unit"].notna().any() else ""
        d["flag"]=d["flag"].fillna("")
        d["lvl"]=d["flag"].map(_flag_level)
        nflag=int((d["flag"]!="").sum()); ni=int((d["lvl"]=="issue").sum()); nt=int((d["lvl"]=="technical").sum()); nn=int((d["lvl"]=="notice").sum())
        lab=", ".join(x for x in [f"{ni} issue(s)" if ni else "",f"{nt} technical" if nt else "",f"{nn} notice(s)" if nn else ""] if x) or "0"
        only=st.toggle(f"Only flagged days ({lab})",value=False,key=key+"_only")
        cap_=item.startswith("Capacity")
        if "detail" not in d.columns: d["detail"]=None
        v=d[["flag","gas_day"]+_DAILY_NUM+["detail"]].copy()
        if only: v=v[v["flag"]!=""]
        if v["detail"].isna().all(): v=v.drop(columns=["detail"])  # v1.13.2: also shown for EiC days without a price
        if len(v):
            s_=lambda c: v[c].sum(min_count=1)
            iv,ie,ev,ee=s_("invoice_volume"),s_("invoice_eur"),s_("equinor_volume"),s_("equinor_eur")
            if cap_:  # capacity: the summary shows average kWh/h per booked day, EUR is a total
                iv,ev=v["invoice_volume"].mean(),v["equinor_volume"].mean()
            dv=(ev-iv) if pd.notna(ev) and pd.notna(iv) else None
            tot=dict(flag="",gas_day=("Average kWh/h \u00b7 total EUR" if cap_ else "Total"),invoice_volume=iv,invoice_eur=ie,invoice_price=(ie/iv if pd.notna(ie) and iv else None),
                     equinor_volume=ev,equinor_eur=ee,equinor_price=(ee/ev if pd.notna(ee) and ev else None),
                     diff_volume=dv,diff_eur=(ee-ie if pd.notna(ee) and pd.notna(ie) else None),diff_pct=(dv/abs(iv)*100 if dv is not None and iv else None))
            v=pd.concat([v,pd.DataFrame([tot])],ignore_index=True)
        disp,ncd,_=for_display(v,_DAILY_NUM)
        fmt={c:("{:,.6f}" if "Price" in c else "{:,.2f}") for c in ncd}
        def _rs(r):
            if str(r["Flag"] or "")!="": return [f"background-color:{FLAG_COLOURS.get(_flag_level(r['Flag']),'#ffcdd2')}"]*len(r)
            if str(r["Gas Day"]).startswith(("Total","Average")): return ["font-weight:bold;background-color:#eeeeee"]*len(r)
            return [""]*len(r)
        st.dataframe(disp.style.format(fmt,na_rep="").apply(_rs,axis=1),use_container_width=True,hide_index=True)
        st.caption(f"Volumes in {unit}, prices in EUR per {unit}. Red = numbers do not match; orange = technical issue (data missing, what can be checked matches); "
                   "yellow = notice (unusual, numbers match). The Total row adds up to the monthly figure in the summary"
                   +(" (ZTP Trading: Entry + Exit; Equinor = exit x 2)." if item=="ZTP Trading" else ".")
                   +" Always shows the latest run for the month.")

# === EXCEL EXPORT ===
def build_results_excel(sections, month_label, daily_df=None):
    """Three sheets: 'Monthly summary' (every section's summary table), 'Gas-day detail' (every gas day of the
    daily items) and 'Flagged' (every section's flagged issues). Columns match the Results tab."""
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    hdr = {"summary": PatternFill("solid", fgColor="2196F3"), "daily": PatternFill("solid", fgColor="607D8B"), "flagged": PatternFill("solid", fgColor="FF5722")}
    ok_fill = PatternFill("solid", fgColor="C8E6C9"); bad_fill = PatternFill("solid", fgColor="FFCDD2")
    def block(xw, ws, sheet, row, label, df, kind, empty):
        ws.cell(row=row, column=1, value=label).font = Font(bold=True, size=12); row += 1
        if df is None or len(df) == 0:
            ws.cell(row=row, column=1, value=empty).font = Font(italic=True); return row + 3
        df.to_excel(xw, sheet_name=sheet, index=False, startrow=row - 1)
        for ci, col in enumerate(df.columns, start=1):
            if pd.api.types.is_numeric_dtype(df[col]):
                f_ = "#,##0.000000" if "price" in str(col).lower() else "#,##0.00"
                for ri in range(len(df)): ws.cell(row=row + 1 + ri, column=ci).number_format = f_
            c = ws.cell(row=row, column=ci); c.fill = hdr[kind]; c.font = Font(bold=True, color="FFFFFF"); c.alignment = Alignment(wrap_text=True)
            lc = str(col).lower()
            if "status" in lc or "verdict" in lc:
                for ri in range(len(df)):
                    v = str(df.iloc[ri][col] or "")
                    if "OK" in v or "VERIFIED" in v: ws.cell(row=row + 1 + ri, column=ci).fill = ok_fill
                    elif "D " in v or "REVIEW" in v or "No" in v: ws.cell(row=row + 1 + ri, column=ci).fill = bad_fill
        if kind == "daily" and "Flag" in df.columns:  # whole row red for flagged gas days
            for ri in range(len(df)):
                if str(df.iloc[ri]["Flag"] or "") not in ("", "nan", "None"):
                    for ci in range(1, len(df.columns) + 1): ws.cell(row=row + 1 + ri, column=ci).fill = bad_fill
        return row + len(df) + 3
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xw:
        for sheet, kind in (("Monthly summary", "summary"), ("Gas-day detail", "daily"), ("Flagged", "flagged")):
            pd.DataFrame().to_excel(xw, sheet_name=sheet, index=False)
            ws = xw.sheets[sheet]
            ws.cell(row=1, column=1, value=f"{sheet} - {month_label}").font = Font(bold=True, size=14)
            row = 3
            if kind == "summary":
                for title, caption, sdf, ftitle, fdf in sections:
                    row = block(xw, ws, sheet, row, f"{title} \u2014 {caption}", sdf, kind, "No data.")
            elif kind == "flagged":
                for title, caption, sdf, ftitle, fdf in sections:
                    row = block(xw, ws, sheet, row, ftitle, fdf, kind, "No flagged issues.")
            else:
                ws.cell(row=2, column=1, value="Every gas day of Energy in Cash, ZTP Trading and Allocation Settlement (latest run). Red = flagged day.").font = Font(italic=True, color="666666")
                if daily_df is None or len(daily_df) == 0:
                    row = block(xw, ws, sheet, row, "Gas-day detail", None, kind, "No gas-day detail saved yet - re-run the month.")
                else:
                    for item, g in daily_df.groupby("Item", sort=False):
                        row = block(xw, ws, sheet, row, str(item), g.drop(columns=["Item"]), kind, "")
            for ci in range(1, ws.max_column + 1):
                width = max((len(str(ws.cell(row=r, column=ci).value or "")) for r in range(3, ws.max_row + 1)), default=8)
                ws.column_dimensions[get_column_letter(ci)].width = min(max(width + 2, 10), 60)
    return buf.getvalue()

# === STREAMLIT UI ===
st.title("Belgium Invoice Checker" + (" (DEV)" if IS_DEV else ""))
st.caption(f"Version {APP_VERSION}")
t1,t2,t3,t5,t4,t6 = st.tabs(["Upload Invoices","Run Check","Results","Overview","Run History","Info"])

def _user_email():
    h=st.context.headers
    return h.get("x-forwarded-email") or h.get("x-forwarded-preferred-username") or "app_user"

# === INVOICE REGISTER (v1.10.0): classify uploads by CONTENT (any file name, any month), store by invoice number,
# pair each bill with its self-bill via the allocation settlement month ===
def _m1(d):
    if d is None or (isinstance(d,float) and pd.isna(d)): return None
    return pd.to_datetime(d).date().replace(day=1)

def read_invoice_meta(raw):
    """(meta, error). Everything comes from inside the XML; the file name is never used."""
    try: root=ET.parse(io.BytesIO(raw)).getroot()
    except Exception: return None,"not a valid XML file"
    h=root.find("InvoiceDetailHeader")
    if root.tag!="InvoiceDetail" or h is None:
        return None,("not a Fluxys invoice DETAIL XML (no InvoiceDetailHeader). If this is the INVXML (summary) file, "
                     "please download and upload the INVDETXML instead")
    it=(h.findtext("InvoiceType") or "").strip(); num=(h.findtext("InvoiceNumber") or "").strip()
    if it not in ("Bill","Self-Bill"): return None,f"unknown InvoiceType '{it}' (expected Bill or Self-Bill)"
    if not re.fullmatch(r"[A-Za-z0-9_-]+",num): return None,f"missing or invalid InvoiceNumber '{num}'"
    try: idate=date.fromisoformat((h.findtext("InvoiceDate") or "")[:10])
    except Exception: return None,"missing or invalid InvoiceDate"
    try: na=float(h.findtext("InvoiceNetAmount") or 0)
    except Exception: na=0.0
    m=dict(invoice_type=it,invoice_number=num,invoice_date=idate,invoice_month=idate.replace(day=1),net_amount=na,
           document_type=(h.findtext("DocumentType") or "").strip() or "Invoice",applies_to_month=None,alloc_settlement_month=None,
           has_backbilling=any((il.findtext("BillingType") or "").strip()=="BackBilling" for il in root.iter("InvoiceLine")),
           file_lines=raw.count(b"\n")+1,detail_rows=sum(1 for _ in root.iter("BilledQuantity")))
    if it=="Bill":
        P=detect_periods(parse_invoice_all(raw),idate)   # same period logic as Run Check
        m["applies_to_month"]=_m1(P["inv_m"]); m["alloc_settlement_month"]=_m1(P["ss"])
    else:
        m["alloc_settlement_month"]=_m1(get_alloc_period(raw,"Allocation Settl"))
    return m,None

def _meta_sql(m):
    return (f"document_type={sql_lit(m['document_type'])},applies_to_month={sql_lit(m['applies_to_month'])},"
            f"alloc_settlement_month={sql_lit(m['alloc_settlement_month'])},has_backbilling={sql_lit(bool(m['has_backbilling']))},"
            f"file_lines={int(m['file_lines'])}")

def _detail_check(fname, m):
    """v1.10.2 soft check: was the INVDETXML (detail) uploaded and not the INVXML (summary)? Returns the reasons or None."""
    why=[]; kind="bill" if m["invoice_type"]=="Bill" else "self-bill"
    if re.search(r"invxml",fname,re.I) and not re.search(r"invdetxml",fname,re.I): why.append("the file name says InvXml")
    if m["detail_rows"]==0: why.append("it has no detail (BilledQuantity) rows")
    ref=run_sql(f"SELECT file_lines FROM {SCHEMA}.invoice_register WHERE invoice_type={sql_lit(m['invoice_type'])} "
                f"AND file_lines IS NOT NULL AND invoice_number<>{sql_lit(m['invoice_number'])}")
    if len(ref)>=2:
        med=float(pd.to_numeric(ref["file_lines"]).median())
        if m["file_lines"]<0.5*med: why.append(f"it has {m['file_lines']:,} lines while other {kind}s have about {med:,.0f}")
    return "; ".join(why) or None

def _backfill_register():
    """Rows uploaded before v1.10.0 (or by an older app version): read the stored file once and fill the content columns."""
    r=run_sql(f"SELECT invoice_number,volume_path FROM {SCHEMA}.invoice_register WHERE document_type IS NULL "
              f"OR (file_lines IS NULL AND document_type<>'Unreadable')")
    for _,x in r.iterrows():
        try: m,err=read_invoice_meta(volume_read(x["volume_path"]))
        except Exception: m=None
        sets=_meta_sql(m) if m else "document_type='Unreadable'"
        exec_sql(f"UPDATE {SCHEMA}.invoice_register SET {sets} WHERE invoice_number={sql_lit(x['invoice_number'])}")

def _minus_months(d, n):
    d=_m1(d)
    if d is None: return None
    y,m=d.year,d.month-n
    while m<=0: m+=12; y-=1
    return date(y,m,1)

def _pair_register():
    """v1.13.0: bill <-> self-bill pairing, independent of upload order (re-done for every row after every upload).
    1) same allocation settlement month on both (closest invoice date wins);
    2) bill without allocation lines: self-bill with the SAME invoice date (Fluxys issues both together);
    3) otherwise: self-bill whose settlement month = bill month - 3.
    Self-bills without allocation settlement sales are never paired automatically. Only writes changes.
    v1.15.0: manual pairs (pair_method='manual', set with the Pair button on the Upload tab) are kept and never re-done."""
    r=run_sql(f"SELECT invoice_number,invoice_type,invoice_date,applies_to_month,alloc_settlement_month,paired_invoice_number,pair_method FROM {SCHEMA}.invoice_register")
    if len(r)==0: return
    b=r[r["invoice_type"]=="Bill"]; s=r[(r["invoice_type"]=="Self-Bill")&r["alloc_settlement_month"].notna()]
    want={}; how={}; used=set()
    ty=dict(zip(r["invoice_number"],r["invoice_type"]))
    for _,x in r[(r["invoice_type"]=="Bill")&(r["pair_method"]=="manual")].iterrows():   # 0) manual pairs first
        bn,sn=x["invoice_number"],x["paired_invoice_number"]
        if sn and ty.get(sn)=="Self-Bill" and sn not in used:
            want[bn]=sn; want[sn]=bn; how[bn]=how[sn]="manual"; used.add(sn)
    def _take(bn,c,method):
        c=c[~c["invoice_number"].isin(used)]
        if len(c)==0: return False
        bd=pd.to_datetime(b.loc[b["invoice_number"]==bn,"invoice_date"].iloc[0])
        c=c.assign(dd=(pd.to_datetime(c["invoice_date"])-bd).abs()).sort_values("dd")
        sn=c.iloc[0]["invoice_number"]; used.add(sn)
        want[bn]=sn; want[sn]=bn; how[bn]=how[sn]=method; return True
    for _,x in b.iterrows():   # 1) settlement month on both
        if x["alloc_settlement_month"] and x["invoice_number"] not in want: _take(x["invoice_number"],s[s["alloc_settlement_month"]==x["alloc_settlement_month"]],"same settlement month")
    for _,x in b.iterrows():   # 2) + 3) bills without allocation lines
        bn=x["invoice_number"]
        if bn in want or x["alloc_settlement_month"]: continue
        if _take(bn,s[s["invoice_date"]==x["invoice_date"]],"same invoice date"): continue
        m3=_minus_months(x["applies_to_month"],3)
        if m3: _take(bn,s[s["alloc_settlement_month"].apply(_m1)==m3],"settlement month = bill month - 3")
    for _,x in r.iterrows():
        n=x["invoice_number"]; w=want.get(n); hw=how.get(n)
        if w!=(x["paired_invoice_number"] or None) or hw!=(x["pair_method"] or None):
            exec_sql(f"UPDATE {SCHEMA}.invoice_register SET paired_invoice_number={sql_lit(w)},pair_method={sql_lit(hw)} WHERE invoice_number={sql_lit(n)}")

def _manual_pair(bn, sn):
    """v1.15.0: pair bill bn with self-bill sn by hand. Old partners of either side are released (in one statement),
    then the automatic pairing is re-run so a released invoice can find another partner."""
    b_,s_=sql_lit(bn),sql_lit(sn)
    exec_sql(f"UPDATE {SCHEMA}.invoice_register SET "
             f"paired_invoice_number=CASE WHEN invoice_number={b_} THEN {s_} WHEN invoice_number={s_} THEN {b_} ELSE NULL END, "
             f"pair_method=CASE WHEN invoice_number IN ({b_},{s_}) THEN 'manual' ELSE NULL END "
             f"WHERE invoice_number IN ({b_},{s_}) OR paired_invoice_number IN ({b_},{s_})")
    _pair_register()

def _manual_unpair(bn, sn):
    """v1.15.1: remove a manual pair, then re-run the automatic pairing for both invoices.
    Returns {invoice_number: (new partner or None, pair_method)} for the two invoices."""
    b_,s_=sql_lit(bn),sql_lit(sn)
    exec_sql(f"UPDATE {SCHEMA}.invoice_register SET paired_invoice_number=NULL,pair_method=NULL "
             f"WHERE invoice_number IN ({b_},{s_}) AND pair_method='manual'")
    _pair_register()
    r=run_sql(f"SELECT invoice_number,paired_invoice_number,pair_method FROM {SCHEMA}.invoice_register WHERE invoice_number IN ({b_},{s_})")
    return {x["invoice_number"]:(x["paired_invoice_number"] or None,x["pair_method"] or None) for _,x in r.iterrows()}

def _unpair_box(bn, sn):
    """v1.15.1: Unpair button for a manual bill / self-bill pair."""
    with st.container(border=True):
        st.markdown(f"Bill **{bn}** and self-bill **{sn}** are paired **by hand**. Unpair them?")
        st.caption("Both go back to the automatic pairing (same settlement month \u2192 same invoice date \u2192 settlement = bill month - 3), "
                   "so they may be paired again automatically, or with other invoices, if the rules match.")
        if st.button("Unpair",key=f"unpair_{bn}_{sn}"):
            res=_manual_unpair(bn,sn)
            def _d(n,kind):
                p,h=res.get(n,(None,None))
                return f"{kind} {n}: "+(f"now paired automatically with {p} ({h})" if p else "not paired")
            st.session_state["pair_msg"]=f"Unpaired bill {bn} and self-bill {sn}. "+_d(bn,"Bill")+"; "+_d(sn,"self-bill")+"."
            st.session_state["reg_sel_k"]=st.session_state.get("reg_sel_k",0)+1   # clears the row selection
            st.rerun()

def _pair_selection(meta, rows):
    """v1.15.0: Pair button for two selected Invoice Register rows (must be one bill + one self-bill).
    v1.15.1: Unpair button when the selection is a manual pair.
    v1.15.2: buttons only appear once BOTH rows are selected (a single click never shows Pair/Unpair)."""
    _s=lambda v: v if isinstance(v,str) and v else None   # NULL / NaN / "" -> None
    if len(rows)==1:
        st.caption("Select one more row (a bill and a self-bill) to pair or unpair them."); return
    if len(rows)>2:
        st.info("Select exactly two rows \u2014 one bill and one self-bill \u2014 to pair them."); return
    a,b_=[{k:_s(v) for k,v in meta.iloc[i].to_dict().items()} for i in rows]
    if {a["invoice_type"],b_["invoice_type"]}!={"Bill","Self-Bill"}:
        why=f" (you selected two {a['invoice_type'].lower()}s)" if a["invoice_type"] and a["invoice_type"]==b_["invoice_type"] else ""
        st.warning(f"Cannot pair: select one **bill** and one **self-bill**{why}."); return
    bl,sb=(a,b_) if a["invoice_type"]=="Bill" else (b_,a)
    bn,sn=bl["invoice_number"],sb["invoice_number"]
    if bl["paired_invoice_number"]==sn:
        if bl["pair_method"]=="manual": _unpair_box(bn,sn); return
        st.info(f"Bill {bn} and self-bill {sn} are already paired ({bl['pair_method'] or 'automatic'})."); return
    nt=[]
    if bl["paired_invoice_number"]: nt.append(f"bill {bn} is now paired with self-bill {bl['paired_invoice_number']} \u2014 that pairing will be removed")
    if sb["paired_invoice_number"]: nt.append(f"self-bill {sn} is now paired with bill {sb['paired_invoice_number']} \u2014 that pairing will be removed")
    if not sb["alloc_settlement_month"]: nt.append("the self-bill has no allocation settlement sales, so the check will not use it")
    elif bl["alloc_settlement_month"] and bl["alloc_settlement_month"]!=sb["alloc_settlement_month"]:
        nt.append(f"allocation settlement months differ (bill {bl['alloc_settlement_month']}, self-bill {sb['alloc_settlement_month']})")
    with st.container(border=True):
        st.markdown(f"Pair **bill {bn}** with **self-bill {sn}**?")
        for n_ in nt: st.markdown("\u26a0\ufe0f "+n_[0].upper()+n_[1:]+".")
        if st.button("Pair",key=f"pair_{bn}_{sn}",type="primary"):
            _manual_pair(bn,sn)
            st.session_state["pair_msg"]=f"Paired bill {bn} with self-bill {sn} (manual). This pairing is kept when new files are uploaded."
            st.session_state["reg_sel_k"]=st.session_state.get("reg_sel_k",0)+1   # clears the row selection
            st.rerun()

def _upload_one(fname, raw):
    """Store one uploaded XML. Returns (level, message)."""
    m,err=read_invoice_meta(raw)
    if m is None: return "error",f"**Not saved \u2014 {fname}**: {err}.",None
    num=m["invoice_number"]; sub="bill" if m["invoice_type"]=="Bill" else "self_bill"
    dest=f"{VOLUME}/{sub}/{num}.xml"   # stored by invoice number
    # v1.10.1: replace what is already there for the SAME invoice number, or the same month and type
    # (bill: invoice month it applies to; self-bill: allocation settlement month) - whatever the file name was
    kc,kv=("applies_to_month",m["applies_to_month"]) if m["invoice_type"]=="Bill" else ("alloc_settlement_month",m["alloc_settlement_month"])
    cond=f"invoice_number={sql_lit(num)}"+(f" OR (invoice_type={sql_lit(m['invoice_type'])} AND {kc}={sql_lit(kv)})" if kv else "")
    prev=run_sql(f"SELECT invoice_number,file_name,volume_path,uploaded_at FROM {SCHEMA}.invoice_register WHERE {cond}")
    volume_write(dest,raw)
    for op in (prev["volume_path"] if len(prev) else []):
        if op and op!=dest and op.startswith(VOLUME):
            try: _get_client().files.delete(op)
            except Exception: pass
    exec_sql(f"DELETE FROM {SCHEMA}.invoice_register WHERE {cond}")
    exec_sql(f"INSERT INTO {SCHEMA}.invoice_register (file_name,volume_path,invoice_type,invoice_number,invoice_date,invoice_month,net_amount_eur,"
             f"file_size_bytes,uploaded_by,uploaded_at,document_type,applies_to_month,alloc_settlement_month,has_backbilling,file_lines) VALUES "
             f"({sql_lit(fname)},{sql_lit(dest)},{sql_lit(m['invoice_type'])},{sql_lit(num)},{sql_lit(m['invoice_date'])},{sql_lit(m['invoice_month'])},"
             f"{m['net_amount']},{len(raw)},{sql_lit(_user_email())},CURRENT_TIMESTAMP(),{sql_lit(m['document_type'])},{sql_lit(m['applies_to_month'])},"
             f"{sql_lit(m['alloc_settlement_month'])},{sql_lit(bool(m['has_backbilling']))},{int(m['file_lines'])})")
    what=(f"Bill {num} \u00b7 applies to **{_fmt_month(m['applies_to_month'])}**" if m["invoice_type"]=="Bill"
          else f"Self-bill {num} \u00b7 allocation settlement **{_fmt_month(m['alloc_settlement_month'])}**" if m["alloc_settlement_month"]
          else f"Self-bill {num} \u00b7 no allocation settlement found")
    msg=f"**Saved \u2014 {fname}** \u2192 {what} \u00b7 EUR {m['net_amount']:,.2f}"
    notes=[]
    for _,p in prev.iterrows():
        same="same invoice" if p["invoice_number"]==num else f"{'bill' if m['invoice_type']=='Bill' else 'self-bill'} {p['invoice_number']} for the same month"
        notes.append(f"replaced {same}, uploaded before as '{p['file_name']}' on {_ts_label(p['uploaded_at'])}")
    if m["document_type"]!="Invoice": notes.append(f"document type is '{m['document_type']}'")
    if m["has_backbilling"]: notes.append("contains BackBilling (correction) lines")
    return "success", msg+(" \u2014 "+"; ".join(notes) if notes else ""), num

# v1.17.0: Info tab - how the check works (short; keep in line with BELGIUM_CHECKER_KNOWLEDGE.md)
INFO_SECTIONS = [
("1. The process in short", """
1. **Upload** the Fluxys bill XML (and self-bill XML). The app reads type, invoice number and months from inside the file.
2. **Pair** each bill with its self-bill (automatic, or by hand on the Upload tab).
3. **Run Check**: every bill line is matched to Equinor data (Endur deals, dispatch allocations, EEX prices).
4. **Results**: one table per billing period, with status, EUR gap and drill-down per row.
"""),
("2. Data sources", """
| Source | Used for |
| --- | --- |
| Fluxys **bill** XML | All capacity, fixed fees, Energy in Cash, ZTP Trading fee, Allocation Settlement purchases |
| Fluxys **self-bill** XML | Allocation Settlement **sales** only |
| **Endur** (deal + profile volume + location) | Capacity bookings: `COMM-CAP-ENTRY/EXIT`, Firm or Interruptible, Validated, Belgian locations |
| **Dispatch** (preliminary allocation) | Energy in Cash quantities, Allocation Settlement quantities |
| **Endurast** + shipper mapping | ZTP Trading exit quantities |
| **EEX** ZTP daily spot index (EGSI) | Energy in Cash and Allocation Settlement prices |
"""),
("3. Dates: which month is which", """
* **Invoice month (M)** = the latest capacity billing month on the bill (not the invoice date).
* **Capacity M** = capacity with billing month = M. **Capacity M-1** = billing month = the month before.
* **Season (daily) bookings are billed roughly 10th-to-9th**: the bill dated ~10th carries the daily bookings since
  the previous bill, so the end of last month lands in **M-1** and the first days of this month in **M**.
  * The app does not assume fixed dates: it takes the window from the Season gas days on the bill
    (between the 1st of M-1 and the 1st of M+1). Default if none: 10th of M-1 to 9th of M.
  * Season gas days outside that window are treated as **corrections** and looked up individually in Endur.
  * Blind spot: a daily booking after the last gas day on the bill (e.g. the 10th) is not searched, as Fluxys bills it next month.
* **LongTerm** (yearly / quarterly / monthly) = any Endur deal active in the billing month.
* **Variable Fees M-3** (Energy in Cash, ZTP Trading): billed ~3 months in arrears; the month = month of the
  earliest gas day on those lines (e.g. October bill = July gas days).
* **Allocation Settlement**: month = earliest billing start of those lines; the self-bill is matched on the same month.
* **Gas day** = 06:00 to 06:00 (CET/CEST).
"""),
("4. Bill / self-bill pairing", """
Tried in this order, independent of upload order:
1. **Manual** pair (set on the Upload tab) - always kept.
2. **Same allocation settlement month** in both files.
3. **Same invoice date**.
4. Self-bill settlement month = bill month - 3.

Self-bills without allocation settlement sales are never paired automatically (they are not used by the check).
"""),
("5. How each item is checked", """
* **Capacity LongTerm**: bill QTY (net of negative lines) vs Endur **net** kWh/h; bill EUR vs Endur volume x price.
  Bill formula: QTY x days billed x tariff / 365.
* **Capacity Season**: per gas day (Gas-day breakdown). Endur = average kWh/h per booked day.
* Matching key: billing month x direction x location x LongTerm/Season x Firm/Interruptible.
  Direction comes from the Endur instrument type (Entry/Exit), never the location name.
* **Results tables** show one row per billing month, direction, location and line type (e.g. Firm LongTerm + Firm Season
  = Firm Total), as on the PDF. Ticking a total shows each underlying line.
* **Energy in Cash**: dispatch kWh per gas day x 0.0008 vs bill QTY; price = EEX ZTP index (published the day before).
  Entry IP = positive flows, Exit IP = negative flows (hub-internal points excluded), Exit Dom = Tessenderlo + Antwerp Jupiter.
* **ZTP Trading fee**: exit MWh x 2 (entry = exit) x 0.00195 EUR/MWh.
* **Allocation Settlement**: bill purchases and self-bill sales vs dispatch, priced at the ZTP index.
* **Fixed fees** (Zee Platform, ZTP Trading fixed fee): shown for completeness, no Endur match expected.
"""),
("6. Special cases", """
* **Back-billing** (`BillingType` = BackBilling): a correction for an earlier month. Checked like normal lines;
  if numbers match it shows a yellow NOTICE. Gaps here are compared against what changed in that earlier month.
* **Interruptible**: rarely billed. Matched separately from Firm; a match shows a yellow NOTICE.
  *Overnominated Interruptible/Backhaul* is an over-nomination **charge**, not a booking - "No Endur Match" is normal.
* **Auction premium**: separate bill lines with no location; linked to a location through the AUC- contract numbers.
  The tariff row is set to 0 gap and any Endur price difference shows on the premium row.
* **Negative QTY lines**: capacity Fluxys moved off a contract (e.g. OCUC converted to an AUC- contract);
  netted with the matching positive line.
* **Counterparty must be Fluxys**: only Endur capacity with SA Fluxys Belgium as counterparty is compared.
  Secondary trades with another shipper (transfer of use, e.g. PRI_SEC buy from Engie at VIP BENE, Oct 2026) are
  settled with that shipper, not Fluxys - left out, but listed under *Endur deals* in the drill-down.
  Assignments are booked against Fluxys in Endur, so they stay in.
* **VIP BENE (Exit)**: Fluxys bills the **gross** OCUC holding - secondary sells (PRI_SEC) are **not** netted.
* **Virtualys (Exit)**: Fluxys bills the **net** after sell-backs - PRI_SEC sell-backs **are** netted (also zero-price ones).
* **VIP THE-ZTP**: one Endur location holds Entry and Exit deals - always split by direction.
* **Within-day bookings**: capacity on booked hours only. Single-id PRISMA refs (`PRI-<id>-ENTRY/EXIT`, ZPT / Zeebrugge)
  have Endur price pro-rated by hours/24 - re-based to a full day. Two-id refs (Virtualys, VIP BENE) are not.
* **Clock-change days** (last Sunday of March = 23 h, October = 25 h): Fluxys bills per **day**, Endur per **hour**.
  Endur EUR is put on the per-day basis.
* **Endur price rounding**: Endur often holds the tariff rounded to 3-4 decimals (EUR/MWh) - gives small gaps
  proportional to volume. See *Price effect by Endur deal* in the drill-down.
* **Price units in Endur**: EUR/MWh on OCUC/bilateral deals, EUR/kWh on PRISMA deals.
* **No EEX price yet** for a gas day: only the quantity is checked (orange TECHNICAL, not an error).
* **Endur only**: Endur capacity not on the bill is flagged (Entry every month, Exit for the invoice month only).
  Belgian Endur locations not mapped to a bill location are flagged as UNMAPPED.
"""),
("7. Status colours and tolerances", """
| Colour | Meaning |
| --- | --- |
| Green **OK** | Matches (EUR gap < 1 EUR) |
| Red **Δ / No match / REVIEW** | Numbers do not match, or a line has no counterpart |
| Orange **TECHNICAL** | Data missing; what could be checked matches |
| Yellow **NOTICE** | Unusual line (Interruptible, BackBilling), numbers match |

* Gas-day flags are raised above **1 %** (capacity, price, quantity) - small EUR gaps can show without a flag.
* Allocation Settlement: quantity > 10 kWh or EUR > 1 EUR.
"""),
]

with t6:
    st.header("How the check works")
    st.caption("Short reference for the Run Check logic. Open a section for details.")
    for i_,(h_,b_) in enumerate(INFO_SECTIONS):
        with st.expander(h_,expanded=(i_==0)): st.markdown(b_)

with t1:
    st.header("Upload Invoice XML Files")
    st.caption("Upload the Fluxys bill and self-bill XMLs exactly as downloaded \u2014 any file name, any month. "
               "Files are saved automatically as soon as you add them (no separate save button). "
               "The app reads the type, invoice number and month from inside each file and pairs each bill with its self-bill. "
               "Uploading a bill (or self-bill) for a month that is already in the register replaces the old one.")
    # v1.10.3: process the files once, then clear the uploader and show one result box + highlight the rows in the register
    uk=st.session_state.setdefault("upl_key",0)
    files=st.file_uploader("Choose XML files",type=["xml"],accept_multiple_files=True,key=f"upl_{uk}")
    last=st.session_state.setdefault("upload_last",[]); newn=st.session_state.setdefault("upload_new",[])
    pend=st.session_state.setdefault("upload_pending",{})
    if files:
        _backfill_register()   # older rows need their month filled in before we can match on it
        res=[]; nn=[]
        with st.spinner(f"Saving {len(files)} file(s)..."):
            for f in files:
                raw=f.getvalue(); k=hashlib.md5(raw).hexdigest()
                m_,_e=read_invoice_meta(raw)
                w_=_detail_check(f.name,m_) if m_ else None
                if w_: pend[k]=dict(name=f.name,raw=raw,why=w_); continue   # looks like an INVXML -> ask first
                lv,ms_,num=_upload_one(f.name,raw); res.append((lv,ms_))
                if num: nn.append(num)
            _pair_register()
        st.session_state["upload_last"]=res; st.session_state["upload_new"]=nn
        st.session_state["upl_key"]=uk+1; st.rerun()   # new uploader key = empty uploader
    for k,p in list(pend.items()):
        with st.container(border=True):
            st.warning(f"**{p['name']}**: did you upload the **INVDETXML** and not the INVXML? {p['why'][0].upper()+p['why'][1:]}. "
                       "The check needs the INVDETXML (invoice detail) file from the TSO. This file has NOT been saved yet.")
            c1,c2,_=st.columns([1.4,1.6,5])
            if c1.button("Proceed with upload",key="pu_"+k):
                _backfill_register(); lv,ms_,num=_upload_one(p["name"],p["raw"]); _pair_register(); pend.pop(k,None)
                st.session_state["upload_last"]=[(lv,ms_)]; st.session_state["upload_new"]=[num] if num else []; st.rerun()
            if c2.button("Cancel and re-upload",key="pc_"+k):
                pend.pop(k,None); st.session_state["upload_last"]=[("info",f"**Not saved \u2014 {p['name']}**. Upload the INVDETXML instead.")]
                st.session_state["upload_new"]=[]; st.rerun()
    if last:
        with st.container(border=True):
            ok=sum(1 for l,_ in last if l=="success")
            if ok: st.success(f"**Upload successful** \u2014 {ok} file(s) saved to the Invoice Register (highlighted in green below).")
            for l,m in last: st.markdown(("\u2713 " if l=="success" else "\u2717 ")+m)
            if st.button("Clear message",key="upl_clear"):
                st.session_state["upload_last"]=[]; st.session_state["upload_new"]=[]; st.rerun()
    if not st.session_state.get("reg_synced"):  # once per session (plus after uploads): fill old rows + refresh pairing
        _backfill_register(); _pair_register(); st.session_state["reg_synced"]=True
    st.subheader("Invoice Register")
    if st.session_state.get("pair_msg"):
        st.success(st.session_state["pair_msg"])
        if st.button("Clear message",key="pair_clear"): st.session_state["pair_msg"]=""; st.rerun()
    reg=run_sql(f"SELECT file_name,invoice_type,invoice_number,invoice_date,invoice_month,applies_to_month,alloc_settlement_month,paired_invoice_number,pair_method,"
                f"document_type,has_backbilling,net_amount_eur,uploaded_by,uploaded_at FROM {SCHEMA}.invoice_register")
    if len(reg)>0:
        bm_={n:a for n,a,t in zip(reg["invoice_number"],reg["applies_to_month"],reg["invoice_type"]) if t=="Bill"}
        def _row(x):
            p=x["paired_invoice_number"]
            if x["invoice_type"]=="Bill":
                ap=x["applies_to_month"]
                hw_=f" ({x['pair_method']})" if p and x["pair_method"] else ""
                # v1.18.2: say 'Paired' on the bill too (was 'Ready', unclear whether pairing worked)
                pr=("Paired \u00b7 self-bill "+p+hw_) if p else ("Not paired \u00b7 self-bill missing (Sales H Grid skipped)" if x["alloc_settlement_month"] else "Not paired \u00b7 no self-bill needed")
            else:
                ap=bm_.get(p) if p else None
                hw_=f" ({x['pair_method']})" if p and x["pair_method"] else ""
                pr=((("Paired \u00b7 bill "+p+hw_) if p else "Waiting for its bill") if x["alloc_settlement_month"]
                    else ("Paired \u00b7 bill "+p+hw_+" \u00b7 not used by the check (no allocation settlement sales)") if p
                    else "Not used by the check (no allocation settlement sales)")
            nt=[]
            if x["document_type"] not in (None,"","Invoice"): nt.append(x["document_type"])
            if str(x["has_backbilling"]).lower()=="true": nt.append("BackBilling lines")
            lab=_fmt_month(ap) if ap else (f"{_fmt_month(x['invoice_month'])} (invoice date)" if x["invoice_month"] else "")
            return pd.Series(dict(applies_to=lab,_ap=ap or x["invoice_month"] or "",pairing=pr,notes=", ".join(nt)))
        reg=pd.concat([reg,reg.apply(_row,axis=1)],axis=1)
        reg["alloc_settlement_month"]=reg["alloc_settlement_month"].apply(lambda v:_fmt_month(v) if v else "")
        reg["_ts"]=pd.to_datetime(reg["uploaded_at"],errors="coerce",utc=True)
        reg=reg.sort_values(["_ts","invoice_type"],ascending=[False,True],na_position="last")  # v1.10.4: most recently uploaded first
        reg["uploaded_at"]=reg["uploaded_at"].apply(lambda v:_ts_label(v) if v else "")
        meta=reg[["invoice_number","invoice_type","paired_invoice_number","pair_method","alloc_settlement_month"]].reset_index(drop=True)  # v1.15.0: for the Pair button
        reg=reg[["file_name","invoice_type","invoice_number","applies_to","pairing","alloc_settlement_month","invoice_date","notes",
                 "net_amount_eur","uploaded_by","uploaded_at"]]
        hl=reg["invoice_number"].isin(set(newn)).tolist()
        reg=to_num(reg,["net_amount_eur"]).reset_index(drop=True); reg,_,_=for_display(reg)
        fm={c:"{:,.2f}" for c in reg.columns if "net" in c.lower()}
        st.caption("Tick **Select** on one bill and one self-bill to pair or unpair them.")
        rows_=select_table(reg,lambda d:d.style.format(fm,na_rep="").apply(lambda r:["background-color:#c8e6c9"]*len(r) if hl[r.name] else [""]*len(r),axis=1),
                           f"reg_sel_{st.session_state.get('reg_sel_k',0)}_{abs(hash(tuple(meta['invoice_number'])))%10**8}",multi=True,help_="Tick one bill and one self-bill to pair / unpair them")
        if rows_: _pair_selection(meta,rows_)
        st.caption("Newest uploads at the top; green = just uploaded. Applies to = the invoice month the bill is checked as (latest capacity billing month on the bill); "
                   "a self-bill shows its paired bill's month, or its invoice-date month if it has no bill. "
                   "Bill and self-bill are paired on the allocation settlement month inside the files. "
                   "If the automatic pairing is wrong or missing, tick one bill and one self-bill and click Pair (shown as 'manual'). "
                   "To undo a manual pair, tick both of its rows and click Unpair (they go back to the automatic pairing).")
    else: st.info("No invoices uploaded yet.")

with t2:
    st.header("Run Invoice Check")
    bills=run_sql(f"SELECT DISTINCT invoice_month,file_name,volume_path,invoice_number,paired_invoice_number,invoice_date FROM {SCHEMA}.invoice_register WHERE invoice_type='Bill' ORDER BY invoice_month,invoice_date")
    if len(bills)==0: st.info("No Bill invoices. Upload first.")
    else:
        ms=sorted(bills["invoice_month"].unique()); sm=st.selectbox("Invoice month",ms,index=len(ms)-1,format_func=_fmt_month)
        bsel=bills[bills["invoice_month"]==sm]
        if len(bsel)>1:  # v1.10.0: more than one bill for the month (e.g. a correction) -> choose
            bi=st.selectbox("Bill",list(range(len(bsel))),index=len(bsel)-1,
                            format_func=lambda i:f"{bsel.iloc[i]['invoice_number']} \u00b7 dated {bsel.iloc[i]['invoice_date']} \u00b7 {bsel.iloc[i]['file_name']}")
            br=bsel.iloc[bi]
        else: br=bsel.iloc[0]
        bp=br["volume_path"]; bf=br["file_name"]
        st.write(f"**Bill:** {bf} ({br['invoice_number']})")
        # Periods detected from the bill itself (same as v2 notebook)
        P=detect_periods(parse_invoice_all(volume_read(bp)), pd.to_datetime(sm).date())
        st.caption(f"Invoice month (M): {P['inv_m']} | Arrears VF/EiC: {P['ack']} | Alloc settlement: {P['sck'] or 'none on this bill'}")
        # v1.10.0: self-bill comes from the pairing saved at upload (allocation settlement month read from the files); override possible
        sbs=run_sql(f"SELECT file_name,volume_path,invoice_number,alloc_settlement_month FROM {SCHEMA}.invoice_register WHERE invoice_type='Self-Bill' ORDER BY invoice_date")
        sp=sf=None; di=0
        so=["None"]+[f"{n} \u00b7 settlement {_fmt_month(a) if a else '?'} \u00b7 {fn}" for n,a,fn in zip(sbs["invoice_number"],sbs["alloc_settlement_month"],sbs["file_name"])]
        if len(sbs):
            pn=br.get("paired_invoice_number")
            hit=[i for i,n in enumerate(sbs["invoice_number"]) if pn and n==pn]
            if not hit and P["ss"] is not None:
                hit=[i for i,a in enumerate(sbs["alloc_settlement_month"]) if a and _m1(a)==_m1(P["ss"])]
            if hit: di=hit[-1]+1
        ssel=st.selectbox("Self-bill",list(range(len(so))),index=di,format_func=lambda i:so[i],
                          help="Picked automatically from the register (same allocation settlement month as the bill). Change only if needed.")
        sba=None
        if ssel>0: sp=sbs.iloc[ssel-1]["volume_path"]; sf=sbs.iloc[ssel-1]["file_name"]; sba=sbs.iloc[ssel-1]["alloc_settlement_month"]
        def _sb_period(P_):  # v1.13.0: bill without allocation lines -> settlement period from the self-bill, so its sales are still checked
            if P_["ss"] is None and sba:
                s_=_m1(sba); P_.update(ss=s_,se=date(s_.year,s_.month,calendar.monthrange(s_.year,s_.month)[1]),sck=f"{s_.year}-{s_.month:02d}",ss_from_sb=True)
            return P_
        P=_sb_period(P)
        if P.get("ss_from_sb"): st.caption(f"Allocation settlement {_fmt_month(P['ss'])} taken from the self-bill (this bill has no allocation purchase lines); the self-bill sales are checked, purchases are zero.")
        if not sp and P["ss"] is not None: st.warning("No self-bill selected for this bill's allocation settlement (sales settlement will be zero).")
        if st.button("Run Check",type="primary"):
            la=st.empty(); logs=[]
            def log(m): logs.append(f"{datetime.utcnow().strftime('%H:%M:%S')} - {m}"); la.text_area("Progress","\n".join(logs),height=200)
            with st.spinner("Running..."):
                rid=str(uuid.uuid4())[:8]; started=datetime.utcnow()
                log(f"Reading bill: {bf}"); bb=volume_read(bp); df_all=parse_invoice_all(bb)
                log(f"Parsed {len(df_all)} entries.")
                P=_sb_period(detect_periods(df_all, pd.to_datetime(sm).date()))
                inv_m=P["inv_m"]; ckm=str(inv_m)
                am,aend,ack=P["am"],P["aend"],P["ack"]
                log(f"Invoice month {inv_m}; arrears {am}..{aend}; settlement {P['ss']}..{P['se']}")
                log("Step 1/5: LTC..."); lc,lfl=check_ltc(df_all,inv_m,log)
                log("Step 2/5: Alloc Settlement...")
                if P["ss"] is not None:
                    sbb=volume_read(sp) if sp else None
                    als,alf,ahf=check_alloc(bb,sbb,P["ss"],P["se"],log)
                else:
                    log("No allocation settlement entries on this bill - skipping"); als,alf,ahf=[],[],False
                log("Step 3/5: Variable Fee..."); vfs,vff=check_vf(df_all,am,aend,ack,log)
                log("Step 4/5: Energy in Cash..."); eis,eif=check_eic(df_all,am,aend,ack,log)
                log("Step 5/5: Saving..."); counts,run_flags,run_ts=save_results(lc,lfl,als,alf,ahf,vfs,vff,eis,eif,df_all,inv_m,ckm,bf,ack,P["sck"],log,rid=rid,run_by=_user_email())
                exec_sql(f"UPDATE {SCHEMA}.invoice_register SET last_checked_at=CURRENT_TIMESTAMP(),last_check_status='COMPLETE' WHERE file_name='{bf.replace(chr(39),chr(39)+chr(39))}'")
                fin=datetime.utcnow(); tr=sum(counts.values())
                exec_sql(f"INSERT INTO {SCHEMA}.run_log (run_id,invoice_file,selfbill_file,invoice_month,started_at,finished_at,run_by,status,message,rows_written) VALUES ('{rid}','{bf.replace(chr(39),chr(39)+chr(39))}',{sql_lit(sf)},'{inv_m}','{started.isoformat()}','{fin.isoformat()}','app_user','SUCCESS','be_m={counts['be_m']},m-1={counts['be_m_minus_1']},m-3={counts['be_m_minus_3']},alloc={counts['be_alloc']},flags={counts['flags']}',{tr})")
                log(f"Done. Run {rid}: {tr} rows saved.")
                st.success(f"Check complete. {tr} rows written. Full detail in the Results tab; comments in Run History.")
                for k in [k for k in st.session_state if str(k).startswith("xlsx_")]: del st.session_state[k]  # drop stale Excel exports
                st.session_state["last_run"]=dict(rid=rid,ts=run_ts,bill=bf,month=str(sm),flags=run_flags)
        lr=st.session_state.get("last_run")
        if lr and lr.get("month")==str(sm):
            st.divider(); render_run_summary(lr)

with t3:
    st.header("Invoice Check Results")
    st.caption("Replicates the Belgium Invoice PDF Format dashboard layout.")
    md=run_sql(f"SELECT DISTINCT check_month FROM {SCHEMA}.be_m UNION SELECT DISTINCT check_month FROM {SCHEMA}.be_m_minus_3 UNION SELECT DISTINCT check_month FROM {SCHEMA}.be_alloc ORDER BY check_month DESC")
    am=sorted(md["check_month"].tolist(),reverse=True) if len(md)>0 else []
    if not am: st.info("No results yet. Run a check first.")
    else:
        fmt=_fmt_month
        mm={fmt(m):m for m in am}
        sl=st.multiselect("Invoice Month",list(mm.keys()),default=[list(mm.keys())[0]])
        sms=[mm[l] for l in sl]
        if not sms: st.warning("Select at least one month.")
        else:
            mf=",".join(f"'{m}'" for m in sms)
            # --- Run selector: latest run by default, previous runs from run history ---
            src_run=None; run_lbl=""
            if len(sms)==1:
                runs=run_sql(f"SELECT run_id, MAX(run_timestamp) AS ts FROM {SCHEMA}.be_runhistory_summary WHERE check_month='{sms[0]}' GROUP BY run_id ORDER BY ts DESC")
                if len(runs)>0:
                    labels=[_ts_label(t)+(" (latest)" if i==0 else "")+f"  \u00b7  run {r}" for i,(r,t) in enumerate(zip(runs["run_id"],runs["ts"]))]
                    pick=st.selectbox("Run timestamp",labels,index=0,help="Defaults to the most recent run. Pick an earlier run to see how results changed.")
                    src_run=runs["run_id"].iloc[labels.index(pick)]; run_lbl=pick
                else:
                    st.caption("No run history for this month yet (results pre-date run history). Showing current results.")
            else:
                st.caption("Showing the latest results. Select a single month to view previous runs.")
            def S(tbl,per):
                if src_run: return f"(SELECT * FROM {SCHEMA}.be_runhistory_summary WHERE run_id='{src_run}' AND period='{per}') t"
                return f"(SELECT * FROM {SCHEMA}.{tbl} WHERE check_month IN ({mf})) t"
            def F(tbl,per):
                if src_run: return f"(SELECT * FROM {SCHEMA}.be_runhistory WHERE run_id='{src_run}' AND period='{per}') t"
                return f"(SELECT * FROM {SCHEMA}.{tbl} WHERE check_month IN ({mf})) t"
            def FSEL(ivn="invoice_volume",evn="equinor_volume",den="diff_eur",dpn="diff_pct"):
                # flagged rows in the same shape as the summaries; COALESCE fills older rows from the legacy value/unit columns
                vu="('kWh/h','kWh','MWh')"
                return (f"endur_deal_numbers,flag_detail AS flag,gas_day,type,direction,location,"
                        f"COALESCE(invoice_eur,CASE WHEN unit='EUR' THEN invoice_value END) AS invoice_eur,"
                        f"COALESCE(equinor_eur,CASE WHEN unit='EUR' THEN equinor_value END) AS equinor_eur,"
                        f"COALESCE(invoice_volume,CASE WHEN unit IN {vu} THEN invoice_value END) AS {ivn},"
                        f"COALESCE(equinor_volume,CASE WHEN unit IN {vu} THEN equinor_value END) AS {evn},"
                        f"COALESCE(diff_eur,CASE WHEN unit='EUR' THEN diff END) AS {den},diff_pct AS {dpn}")
            sections=[]  # collected for the Excel export: (title, caption, summary_df, flagged_title, flagged_df)
            def show(title,caption,ssql,fsql,ft):
                st.markdown(f"## {title}"); st.caption(caption)
                ds=run_sql(ssql)
                if len(ds)>0:
                    nc=[c for c in ds.columns if any(k in c.lower() for k in ["eur","kwh","volume","diff","pct","gap"])]
                    ds=to_num(ds,nc)
                    if "verdict" in ds.columns: ds=_verdict_to_status(ds)  # v1.18.3: M-3 / Self Bill show the gap like Capacity M
                    sc=[c for c in ds.columns if "status" in c.lower() or "verdict" in c.lower()]
                    ds,comps=_aggregate_rows(ds)  # v1.16.0: PDF-style totals (capacity tables; other tables have no capacity_type and stay as they are)
                    raw_s=ds.copy(); ds,nc,sc=for_display(ds,nc,sc)
                    st.caption("Tick **Select** on a row to see its gas days, Endur deals, invoice lines and Ask AI below the table.")
                    sel_=select_table(ds,lambda d,nc=nc,sc=sc:styled(d,nc,sc),f"sel_{title}_{abs(hash((mf,src_run,len(ds))))%10**8}")  # new key per month/run
                    if "capacity_type" in raw_s.columns:
                        st.caption("One row per billing month, direction, location and line type, as on the PDF invoice "
                                   "(e.g. Firm LongTerm + Firm Season = Firm Total).")
                    if sel_:
                        parts=comps[sel_[0]]
                        if len(parts)>1:
                            st.info("This total is made of "+str(len(parts))+" lines: "+", ".join(
                                f"{p.get('capacity_type') or p.get('item')} ({(p.get('invoice_eur') or 0):,.2f} EUR, {p.get('status') or p.get('verdict')})" for p in parts)
                                +". Details per line below.")
                        for pi_,r_ in enumerate(parts):
                            if len(parts)>1: st.markdown(f"#### {r_.get('capacity_type') or r_.get('item')}")
                            _detail(r_, title if pi_==0 else f"{title}_{pi_}")
                else: st.info("No data.")
                # flagged issues are no longer shown on screen (gas-day panel replaces them); kept for the Excel 'Flagged' sheet
                df=run_sql(fsql)
                if len(df)>0:
                    nc=[c for c in df.columns if any(k in c.lower() for k in ["eur","kwh","volume","diff","pct","gap"])]
                    df=to_num(df,nc); df,nc,_=for_display(df,nc)
                sections.append((title,caption,ds,ft,df))
            def _detail(r_, title):
                """Drill-down for one underlying summary line (unchanged from v1.12 - v1.15)."""
                if True:
                    if True:
                        di_=_daily_item(r_); cap_=(not di_) or di_.startswith("Capacity")
                        if di_ and not cap_:  # v1.12.0: EiC / ZTP Trading / Alloc: gas days + invoice lines + Ask AI
                            with st.expander("Gas-day breakdown \u25be"): render_daily_panel(r_,mf,f"dp_{title}")
                            dl_,dm_=_daily_invoice_lines(r_,mf)
                            with st.expander("Invoice lines \u25be"): render_daily_invoice_panel(r_,dl_,dm_)
                            _ask_ai_ui(title,mf,src_run,r_,None,dl_,None,di_)
                        if cap_:  # v1.8.1: capacity detail in collapsible sections
                            if di_:
                                with st.expander("Gas-day breakdown \u25be"): render_daily_panel(r_,mf,f"dp_{title}")
                            with st.expander("Endur deals \u25be"): raw_e_=render_endur_panel(r_,f"ep_{title}_s")
                            if str(r_.get("line_type") or "").startswith("LTC"):
                                il_,im_=_invoice_lines(r_,mf)
                                with st.expander("Invoice lines \u25be"): render_invoice_panel(r_,il_,im_)
                                rec_=_reconcile(r_,il_,raw_e_,show=False)  # v1.9.1: no on-screen section; the split + per-deal price effect go to Ask AI
                                _ask_ai_ui(title,mf,src_run,r_,raw_e_,il_,rec_,di_)
            show("Capacity M","Capacity bookings where the billing month equals the invoice month.",
                f"SELECT endur_deal_numbers,billing_month,direction,type AS line_type,location,product_name AS capacity_type,invoice_eur,equinor_eur,invoice_volume AS invoice_kwh_h,equinor_volume AS endur_kwh_h,diff_eur AS gap_eur,status FROM {S('be_m','M')} ORDER BY direction,location",
                f"SELECT {FSEL('invoice_kwh_h','endur_kwh_h','gap_eur','gap_pct')} FROM {F('be_m_flagged','M')} ORDER BY gas_day",
                "Capacity M - Flagged Issues")
            st.divider()
            show("Capacity M-1","Capacity bookings where the billing month is one month prior to the invoice month.",
                f"SELECT endur_deal_numbers,billing_month,direction,type AS line_type,location,product_name AS capacity_type,invoice_eur,equinor_eur,invoice_volume AS invoice_kwh_h,equinor_volume AS endur_kwh_h,diff_eur AS gap_eur,status FROM {S('be_m_minus_1','M-1')} ORDER BY direction,location",
                f"SELECT {FSEL('invoice_kwh_h','endur_kwh_h','gap_eur','gap_pct')} FROM {F('be_m_minus_1_flagged','M-1')} ORDER BY gas_day",
                "Capacity M-1 - Flagged Issues")
            st.divider()
            show("Variable Fees M-3","Variable fees and energy in cash for the invoice month (gas month is M-3).",
                f"SELECT endur_deal_numbers,billing_month,type AS line_type,product_name AS item,invoice_eur,equinor_eur,diff_eur AS eur_diff,diff_pct AS qty_diff_pct,status AS verdict FROM {S('be_m_minus_3','M-3')} ORDER BY type,product_name",
                f"SELECT {FSEL(den='eur_diff')} FROM {F('be_m_minus_3_flagged','M-3')} ORDER BY gas_day",
                "Variable Fees M-3 - Flagged Issues")
            st.divider()
            show("Self Bill","Sales H Grid (allocation settlement)",
                f"SELECT endur_deal_numbers,billing_month,type AS line_type,product_name AS item,invoice_eur,equinor_eur,diff_eur AS eur_diff,status AS verdict FROM {S('be_alloc','Alloc')}",
                f"SELECT {FSEL(den='eur_diff')} FROM {F('be_alloc_flagged','Alloc')} ORDER BY gas_day",
                "Sales H Grid - Flagged Issues")
            st.divider()
            ml=", ".join(sl)
            xkey="xlsx_"+"|".join(sms)+(f"|{src_run}" if src_run else "")
            if st.button("Prepare download"):
                with st.spinner("Building Excel file..."):
                    dd_=run_sql(f"SELECT item,check_month,billing_month,gas_day,flag,invoice_volume,invoice_eur,invoice_price,equinor_volume,equinor_eur,equinor_price,diff_volume,diff_eur,diff_pct,unit,detail FROM {SCHEMA}.be_daily WHERE check_month IN ({mf}) ORDER BY check_month,section,item,gas_day")
                    if len(dd_):
                        dd_=to_num(dd_,_DAILY_NUM); dd_["check_month"]=dd_["check_month"].apply(_fmt_month); dd_["billing_month"]=dd_["billing_month"].apply(_fmt_month)
                        dd_=dd_.rename(columns={c:pretty_col(c) for c in dd_.columns})
                    else: dd_=None
                    st.session_state[xkey]=build_results_excel(sections,ml,dd_)
            if st.session_state.get(xkey):
                st.download_button("Download Excel",st.session_state[xkey],
                    file_name=f"Belgium_Invoice_Check_{ml.replace(' ','_').replace(',','')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

with t4:
    st.header("Run History")
    st.caption("Every flagged issue from every Run Check. Filter by month and run, type a comment, then click Save comments.")
    rh=run_sql(f"SELECT run_id, MAX(run_timestamp) AS ts, MAX(check_month) AS check_month, MAX(invoice_file) AS invoice_file FROM {SCHEMA}.be_runhistory_summary GROUP BY run_id ORDER BY ts DESC")
    if len(rh)==0:
        st.info("No runs recorded yet. Each Run Check from now on is saved here.")
    else:
        c1,c2=st.columns(2)
        mopts=sorted(rh["check_month"].dropna().unique(),reverse=True)
        msel=c1.multiselect("Invoice month",mopts,format_func=_fmt_month,placeholder="All months")
        rsub=rh[rh["check_month"].isin(msel)] if msel else rh
        rlab={f"{_ts_label(t)}  \u00b7  {_fmt_month(m)}  \u00b7  run {r}":r for r,t,m in zip(rsub["run_id"],rsub["ts"],rsub["check_month"])}
        rsel=c2.multiselect("Run timestamp",list(rlab.keys()),placeholder="All runs")
        conds=[]
        if msel: conds.append("check_month IN ("+",".join(sql_lit(m) for m in msel)+")")
        if rsel: conds.append("run_id IN ("+",".join(sql_lit(rlab[l]) for l in rsel)+")")
        where=("WHERE "+" AND ".join(conds)) if conds else ""
        _vu="('kWh/h','kWh','MWh')"  # older rows: fill the new columns from the legacy value/unit columns
        hist=run_sql(f"SELECT flag_id,run_timestamp,run_id,check_month,period,type,gas_day,location,direction,"
                     f"COALESCE(invoice_volume,CASE WHEN unit IN {_vu} THEN invoice_value END) AS invoice_volume,"
                     f"COALESCE(equinor_volume,CASE WHEN unit IN {_vu} THEN equinor_value END) AS equinor_volume,"
                     f"COALESCE(invoice_eur,CASE WHEN unit='EUR' THEN invoice_value END) AS invoice_eur,"
                     f"COALESCE(equinor_eur,CASE WHEN unit='EUR' THEN equinor_value END) AS equinor_eur,"
                     f"COALESCE(diff_eur,CASE WHEN unit='EUR' THEN diff END) AS diff_eur,diff_pct,"
                     f"flag_detail,comment,comment_by,comment_at,endur_deal_numbers FROM {SCHEMA}.be_runhistory {where} ORDER BY run_timestamp DESC, period, type, gas_day")
        nruns=len(rsel) if rsel else len(rsub)
        m1,m2,m3=st.columns(3)
        m1.metric("Runs",nruns); m2.metric("Flagged issues",len(hist))
        m3.metric("Commented",int(hist["comment"].fillna("").str.strip().ne("").sum()) if len(hist)>0 else 0)
        if len(hist)==0:
            st.success("No flagged issues for the selected run(s).")
        else:
            view=hist.copy()
            for c in ("invoice_volume","equinor_volume","invoice_eur","equinor_eur","diff_eur","diff_pct"):  # read-only display: commas + 2 dp
                view[c]=view[c].apply(_n2)
            view["run_timestamp"]=view["run_timestamp"].apply(_ts_label)
            view["check_month"]=view["check_month"].apply(_fmt_month)
            view=view.drop(columns=["run_id"])  # run id is already in the Run timestamp filter label
            view["endur_deal_numbers"]=view["endur_deal_numbers"].fillna("")
            view["flag_detail"]=view["flag_detail"].fillna("").str.replace("\u26a0\ufe0f","",regex=False).str.strip()
            view["comment"]=view["comment"].fillna("")
            view=view.set_index("flag_id")
            orig=view["comment"].copy()
            editable={"comment"}
            ekey="rh_editor_"+str(abs(hash((tuple(msel),tuple(rsel)))))
            ed=st.data_editor(view,key=ekey,use_container_width=True,hide_index=True,
                disabled=[c for c in view.columns if c not in editable],
                column_config={
                    "endur_deal_numbers":st.column_config.TextColumn("Endur Deal Numbers"),
                    "run_timestamp":st.column_config.TextColumn("Run timestamp"),
                    "check_month":st.column_config.TextColumn("Invoice month"),
                    "period":st.column_config.TextColumn("Period"),
                    "type":st.column_config.TextColumn("Check"),
                    "gas_day":st.column_config.TextColumn("Gas day / month"),
                    "invoice_volume":st.column_config.TextColumn("Invoice Volume"),
                    "equinor_volume":st.column_config.TextColumn("Equinor Volume"),
                    "invoice_eur":st.column_config.TextColumn("Invoice EUR"),
                    "equinor_eur":st.column_config.TextColumn("Equinor EUR"),
                    "diff_eur":st.column_config.TextColumn("Diff EUR"),
                    "diff_pct":st.column_config.TextColumn("Diff %"),
                    "flag_detail":st.column_config.TextColumn("Flag",width="large"),
                    "comment":st.column_config.TextColumn("Comment",width="large",help="Type a comment, then click Save comments"),
                    "comment_by":st.column_config.TextColumn("Comment by"),
                    "comment_at":st.column_config.TextColumn("Comment at"),
                })
            if st.button("Save comments",type="primary"):
                newc=ed["comment"].fillna("").astype(str)
                changed=newc[newc.str.strip()!=orig.reindex(newc.index).fillna("").astype(str).str.strip()]
                if len(changed)==0:
                    st.info("No comment changes to save.")
                else:
                    vals=",".join(f"({sql_lit(fid)},{sql_lit(c.strip() or None)})" for fid,c in changed.items())
                    exec_sql(f"MERGE INTO {SCHEMA}.be_runhistory t USING (SELECT flag_id, CAST(comment AS STRING) AS comment FROM VALUES {vals} AS s(flag_id,comment)) s "
                             f"ON t.flag_id=s.flag_id WHEN MATCHED THEN UPDATE SET t.comment=s.comment, t.comment_by={sql_lit(_user_email())}, t.comment_at=current_timestamp()")
                    st.session_state["rh_saved_msg"]=f"Saved {len(changed)} comment(s)."
                    st.rerun()
            if st.session_state.get("rh_saved_msg"):
                st.success(st.session_state.pop("rh_saved_msg"))

with t5:
    st.header("Overview")
    st.caption("Variable-cost items on each Fluxys bill, invoice vs Equinor, month by month. Each month shows its latest Run Check; a month appears once it has been run.")
    ovd=run_sql(f"SELECT * FROM {SCHEMA}.be_overview ORDER BY check_month")
    if len(ovd)==0:
        st.info("No overview yet. Run a check for a month to start building it.")
    else:
        OVN=["invoice_volume","invoice_eur","invoice_price","equinor_volume","equinor_eur","equinor_price","diff_eur","diff_pct"]
        ovd=to_num(ovd,OVN)
        ITEMS=["Alloc Settlement Buy","Energy in Cash - Entry IP","Energy in Cash - Exit Dom","Energy in Cash - Exit IP","ZTP Trading","Total variable cost"]
        present=set(ovd["item"])
        items=[i for i in ITEMS if i in present]+sorted(present-set(ITEMS))
        MET={"Invoice volume":"invoice_volume","Invoice \u20ac":"invoice_eur","Invoice \u20ac/unit":"invoice_price",
             "Equinor volume":"equinor_volume","Equinor \u20ac":"equinor_eur","Equinor \u20ac/unit":"equinor_price",
             "Diff \u20ac":"diff_eur","Diff %":"diff_pct"}
        sel_items=st.multiselect("Variable cost items",items,default=items,placeholder="All items")
        sel_items=sel_items or items  # nothing picked = show all
        mets=st.multiselect("Show",list(MET),default=["Invoice volume","Invoice \u20ac","Equinor volume","Equinor \u20ac","Diff \u20ac"])
        months=sorted(ovd["check_month"].unique())
        wide=pd.DataFrame(index=[_fmt_month(m) for m in months])
        price_cols,pct_cols=[],[]
        for it in sel_items:
            sub=ovd[ovd["item"]==it].drop_duplicates("check_month").set_index("check_month")
            unit=str(sub["volume_unit"].dropna().iloc[0]) if sub["volume_unit"].notna().any() else ""
            for m in mets:
                col=MET[m]
                if it=="Total variable cost" and ("volume" in col or "price" in col): continue
                lab=f"{it} \u00b7 {m}"+(f" ({unit})" if "volume" in col else (f" (\u20ac/{unit})" if "price" in col else ""))
                wide[lab]=sub[col].reindex(months).values
                if "price" in col: price_cols.append(lab)
                if col=="diff_pct": pct_cols.append(lab)
        # v1.14.3: month-on-month orange highlight removed from the Overview table
        numc=list(wide.columns)
        fmt={c:("{:,.6f}" if c in price_cols else "{:,.2f}") for c in numc}
        # v1.14.5: month labels as a real "Invoice month" column (header shown) instead of an unnamed index
        wide=wide.rename_axis("Invoice month").reset_index()
        st.dataframe(wide.style.format(fmt,na_rep=""),use_container_width=True,hide_index=True)
        st.caption("Volume units: kWh (Alloc, Energy in Cash), MWh (ZTP Trading, Entry + Exit).")
        st.subheader("Item trend")
        it=st.selectbox("Item",items,index=items.index("Total variable cost") if "Total variable cost" in items else 0)
        sub=ovd[ovd["item"]==it].drop_duplicates("check_month").sort_values("check_month").copy()
        ch=sub.set_index(pd.to_datetime(sub["check_month"]))[["invoice_eur","equinor_eur"]].rename(columns={"invoice_eur":"Invoice \u20ac","equinor_eur":"Equinor \u20ac"})
        st.line_chart(ch)
        sub["invoice_eur_change_pct"]=(sub["invoice_eur"]-sub["invoice_eur"].shift(1))/sub["invoice_eur"].shift(1).abs()*100
        tv=sub[["status","check_month","invoice_volume","invoice_eur","invoice_eur_change_pct","equinor_volume","equinor_eur","diff_eur","diff_pct"]].copy()
        tv["check_month"]=tv["check_month"].apply(_fmt_month)
        tv=tv.rename(columns={"check_month":"invoice_month"})
        tn=["invoice_volume","invoice_eur","invoice_eur_change_pct","equinor_volume","equinor_eur","diff_eur","diff_pct"]
        tv,tn2,ts2=for_display(tv,tn,["status"])
        st.dataframe(styled(tv,tn2),use_container_width=True,hide_index=True)  # v1.14.4: no status colours on Overview Item trend
