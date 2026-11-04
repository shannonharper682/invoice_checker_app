"""Belgium Invoice Checker - Demo App v1.0
Streamlit app replicating Invoice Checker Belgium v2 notebook logic.
Tabs: Upload Invoices | Run Check | Results.
All data in ms_vulcan_gpgtopm_plab.belgium_app_demo_v1.
"""
import os, io, time, uuid, calendar, re
from datetime import date, datetime, timedelta
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

SCHEMA = "ms_vulcan_gpgtopm_plab.belgium_app_demo_v1"

# --- Version: bump BASE_VERSION on each release. Dev app (name ends with -dev) shows the "-dev" suffix. ---
BASE_VERSION = "1.7.0"
APP_NAME = os.environ.get("DATABRICKS_APP_NAME", "")
IS_DEV = APP_NAME.endswith("-dev")
APP_VERSION = BASE_VERSION + ("-dev" if IS_DEV else "")
VOLUME = "/Volumes/ms_vulcan_gpgtopm_plab/belgium_app_demo_v1/invoices"
DISPATCH_TABLE = "ms_atlas.dispatchnatgas_standard.hourly_v2latest"
PRICE_TABLE = "ms_atlas.ms07splitprice_standard.eex_v1r0"
ENDURAST_TABLE = "ms_atlas.endurast_raw.hourly_v1latest"
COUNTRY = "BE"
DOMESTIC_LOCATIONS = ("TESSENDERLO", "ANTWERP JUPITER")
HUB_INTERNAL_LOCATIONS = ("ZBHUBEE", "ZBHUBFL")
QTY_MULTIPLIER = 0.0008
TRADING_FEE_RATE = 0.00195
PRICE_DATA_PACKAGE = "Eex_EuropeanGasSpotIndex"
PRICE_KEYS = "ZTP,Day"
PRICE_COLUMN = "Index"
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
                svc_h = (sh_e - sh_s + 1) if (sh_s > 0 and sh_e > 0) else 24
                if svc_h <= 0: svc_h += 24
                qe = pfi.find("QTY") if pfi is not None else None
                ue = pfi.find("UP") if pfi is not None else None
                qv = float(qe.get("QTY",0)) if qe is not None else None
                qu = qe.get("QTYUnit","") if qe is not None else None
                uv = float(ue.get("UP",0)) if ue is not None else None
                uu = ue.get("UPUnit","") if ue is not None else None
                vol = qv * svc_h if qv is not None else 0
                rows.append(dict(product_name=pname,capacity_type=cap_type,invoice_line_group=lg,
                    location=ln,detail=dt_,billing_start=billing_start,gas_day=gas_day,
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
        AND pv2.settlement_type_id=1 AND pv2.price>0 AND LOWER(d2.reference) NOT LIKE '%conversion%'),
    raw AS (SELECT d.deal_number,d.reference,d.service_type AS ect,
        CASE WHEN d.instrument_type_name='COMM-CAP-ENTRY' THEN 'Entry' ELSE 'Exit' END AS direction,
        l.location_name,l.ng_location_id,CAST(DATE_TRUNC('month',pv.start_date) AS DATE) AS billing_month,
        pv.calculated_profile_volume_kwh AS vol,pv.price,
        pv.calculated_profile_volume_kwh/((DATEDIFF(pv.end_date,pv.start_date)+1)*24) AS cap_kwh_h,
        pv.calculated_profile_volume_kwh*pv.price AS endur_eur,pv.start_date AS pv_start,
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
        AND cm.country_code='BE' AND pv.start_date>='{ss}' AND pv.end_date<='{m_end}')
    SELECT * FROM raw WHERE rn=1"""
    de = run_sql(esql)
    if len(de)==0: log("No Endur deals."); return [],[]
    de = to_num(de,["vol","price","cap_kwh_h","endur_eur"])
    de["billing_month"]=pd.to_datetime(de["billing_month"]).dt.date
    de["pv_start"]=pd.to_datetime(de["pv_start"]).dt.date
    dlt = de[de["bt"]!="Daily"].copy()
    sm = de["bt"]=="Daily"
    if len(sd)>0: sm &= ((de["pv_start"]>=sws)&(de["pv_start"]<=swe))|(de["pv_start"].isin(cds) if cds else False)
    dse = de[sm].copy()
    log(f"Endur: {len(dlt)} long-term + {len(dse)} seasonal")
    cb = df_lt[~is_auc].groupby(["billing_month","direction","location","service_rate_type","capacity_type"],as_index=False).agg(
        tariff_eur=("billed_amount","sum"),tqkh=("qty","sum"),tvk=("volume_kwh","sum"),
        nd=("gas_day","nunique"),iup=("up","first"),iuu=("up_unit","first"),
        detail=("detail",lambda x:" | ".join(sorted(set(str(v) for v in x if v)))),
        contracts=("contract_ref",lambda x:",".join(str(v) for v in x if v)))
    cb["ikwh"] = cb["tqkh"]/cb["nd"]
    comp_rows,flags = [],[]
    for _,r in cb.iterrows():
        bm,d,loc=r["billing_month"],r["direction"],r["location"]
        rt,ct=r["service_rate_type"],r["capacity_type"]
        el=LOCATION_MAP.get(loc); dn=None
        edf = (dlt if rt=="LongTerm" else dse)
        edf = edf[edf["ect"]==ct]
        dim=calendar.monthrange(bm.year,bm.month)[1]
        ec=ee=ep=None
        if el:
            # Endur locations such as 'VIP THE-ZTP BE' hold Entry AND Exit deals -> match on direction too
            er=edf[(edf["ng_location_id"]==el)&(edf["billing_month"]==bm)&(edf["direction"]==d)]
            dn=_dn(er)
            if rt=="LongTerm" and len(er)>0:
                cs=abs(er["cap_kwh_h"].sum())  # NET capacity (matches v2 notebook)
                ca=er["cap_kwh_h"].abs().sum()
                ap=er.apply(lambda x:abs(x["price"])*(24*365/1000 if abs(x["price"])>=0.01 else 24*365),axis=1)
                wp=(er["cap_kwh_h"].abs()*ap).sum()/ca if ca else None
                if cs:
                    ec=float(cs); ep=float(wp) if wp is not None else None
                    if ep is not None: ee=ec*ep*dim/365
            elif rt!="LongTerm" and len(er)>0:
                dd=er["pv_start"].nunique()
                if dd:
                    ec=float(er["cap_kwh_h"].abs().sum())/dd
                    ee=float(er["endur_eur"].abs().sum())
                    vs=float(er["vol"].abs().sum()); ep=ee/vs if vs else None
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
            price_gap_pct=round(pgpp,3) if pgpp is not None else None,contracts=r["contracts"],endur_deal_numbers=dn))
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
        if lt=="Tariff":
            if r["endur_kwh_h"] is None or pd.isna(r.get("endur_kwh_h")):
                flags.append(dict(type=r["service_rate_type"],gas_day=bms,direction=d,location=loc,
                    invoice_value=r["invoice_kwh_h"],endur_value=None,gap=None,gap_pct=None,
                    unit="kWh/h",note=f"NO ENDUR MATCH [{ct}]",**_lx(r)))
            else:
                if r["endur_kwh_h"]!=r["invoice_kwh_h"]:
                    g=r["endur_kwh_h"]-r["invoice_kwh_h"]
                    gp=round(g/r["invoice_kwh_h"]*100,3) if r["invoice_kwh_h"] else None
                    if gp is not None and abs(gp)>1:
                        flags.append(dict(type=r["service_rate_type"],gas_day=bms,direction=d,location=loc,
                            invoice_value=r["invoice_kwh_h"],endur_value=r["endur_kwh_h"],
                            gap=g,gap_pct=gp,unit="kWh/h",note=f"capacity rate mismatch [{ct}]",**_lx(r)))
                if r.get("price_gap_pct") is not None and not pd.isna(r.get("price_gap_pct")) and abs(r["price_gap_pct"])>1:
                    flags.append(dict(type=r["service_rate_type"],gas_day=bms,direction=d,location=loc,
                        invoice_value=r.get("invoice_price"),endur_value=r.get("endur_price"),gap=r.get("price_gap"),gap_pct=r["price_gap_pct"],
                        unit="EUR/unit",note=f"price mismatch [{ct}]",**_lx(r)))
        elif lt=="Auction Premium":
            if r.get("gap_eur") is not None and not pd.isna(r.get("gap_eur")) and r["invoice_eur"]:
                gp=round(r["gap_eur"]/r["invoice_eur"]*100,3)
                if abs(gp)>1:
                    flags.append(dict(type="AuctionPremium",gas_day=bms,direction=d,location=loc,
                        invoice_value=r["invoice_eur"],endur_value=r["equinor_eur"],gap=r["gap_eur"],gap_pct=gp,
                        unit="EUR",note=f"auction premium EUR mismatch [{ct}]",**_lx(r,vol=False)))
    def _eagg(fr_,et,bm):  # Endur capacity kWh/h + EUR for a group of deal rows (same basis as matched rows)
        dim_=calendar.monthrange(bm.year,bm.month)[1]; cap=eur_=None
        if et=="LongTerm":
            cap=abs(float(fr_["cap_kwh_h"].sum()))
            ca_=fr_["cap_kwh_h"].abs().sum()
            ap_=fr_.apply(lambda x:abs(x["price"])*(24*365/1000 if abs(x["price"])>=0.01 else 24*365),axis=1)
            wp_=(fr_["cap_kwh_h"].abs()*ap_).sum()/ca_ if ca_ else None
            if wp_ is not None: eur_=cap*wp_*dim_/365
        else:
            nd_=fr_["pv_start"].nunique(); cap=float(fr_["cap_kwh_h"].abs().sum())/nd_ if nd_ else None
            eur_=float(fr_["endur_eur"].abs().sum())
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
        cp["pm"]=(cp["iup"]-cp["ep"]).abs()<0.000001; cp["ea"]=cp["eqty"]*cp["ep"]; cp["ad"]=cp["ea"]-cp["ia"]
        cp["flag"]=""; he=~((cp["eqty"].fillna(0).abs()<0.0001)&(cp["iq"].isna()))
        cp.loc[he&(cp["qdp"].abs()>1),"flag"]="\u26a0\ufe0f QTY"
        cp.loc[he&(~cp["pm"]),"flag"]=cp.loc[he&(~cp["pm"]),"flag"]+" \u26a0\ufe0f PRICE"
        iqs=iv["iq"].sum(); eqs=cp["eqty"].sum()
        it=iv["ia"].sum(); et=cp["ea"].sum(); ed=et-it
        v="VERIFIED" if (it and abs(ed/it*100)<0.1 and cp["pm"].all()) else "REVIEW REQUIRED"
        sr.append(dict(category=cat,line_type="Energy in Cash",invoice_qty=float(iqs),equinor_qty=float(eqs),invoice_eur=it,equinor_eur=et,eur_diff=ed,qty_diff_pct=(eqs-iqs)/iqs*100 if iqs else None,days_flagged=len(cp[cp["flag"]!=""]),price_match=bool(cp["pm"].all()),verdict=v))
        sr[-1]["daily"]=[dict(gas_day=str(r["gas_day"]),invoice_volume=r["iq"],invoice_eur=r["ia"],invoice_price=r["iup"],
                              equinor_volume=r["eqty"],equinor_eur=r["ea"],equinor_price=r["ep"],flag=r["flag"]) for _,r in cp.iterrows()]
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
        ar.append(_u(r["billing_month"],f"LTC {r['line_type']}",f"{r['capacity_type']} {r.get('service_rate_type','')}",r["location"],r["direction"],ie=r["invoice_eur"],iv=r.get("invoice_kwh_h"),ee=r.get("equinor_eur"),ev=r.get("endur_kwh_h"),de=r.get("gap_eur"),dp=r.get("price_gap_pct"),st_=_gs(r.get("equinor_eur"),r.get("gap_eur")),dn=r.get("endur_deal_numbers")))
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
                flag=(str(r.get("flag") or "").replace("\u26a0\ufe0f","").strip() or None),run_id=rid,run_timestamp=ts))
    _EIC={"Entry at Interconnection Point":"Energy in Cash - Entry IP","Exit at Domestic Point":"Energy in Cash - Exit Dom","Exit at Interconnection Point":"Energy in Cash - Exit IP"}
    for e in eis: _dl("M-3",ack,_EIC.get(e.get("category"),"Energy in Cash - "+str(e.get("category"))),"kWh",e.get("daily"))
    if vfs: _dl("M-3",ack,"ZTP Trading","MWh",vfs.get("daily"))
    for a in als:
        if a["line"]=="Purchases": _dl("M-3",sck,"Alloc Settlement Buy","kWh",a.get("daily"))
        elif "Sales" in a.get("line",""): _dl("Alloc",sck,"Alloc Settlement Sale","kWh",a.get("daily"))
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

def _sc(val):
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
    if not fl:
        st.success("No flagged issues \u2014 every check matched within tolerance.")
        return
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
           "d.deal_start_date,d.deal_end_date,d.trade_date,d.para_position,"
           "l.location_name,l.ng_location_id,pv.settlement_type_id,pv.start_date,pv.end_date,pv.price,"
           "pv.calculated_profile_volume_kwh,pv.calculated_profile_volume,pv.calculated_payment,"
           "pv.unit_id,pv.price_unit_id,pv.volume_type_name "
           "FROM ms_atlas.endur_standard.deal_v2latest d "
           "JOIN ms_atlas.endur_standard.profilevolume_v2latest pv ON d.deal_number=pv.deal_number "
           "JOIN ms_atlas.endur_standard.location_v2latest l ON pv.location_id=l.location_id "
           f"WHERE {where}{per} ORDER BY d.deal_number,pv.start_date,pv.settlement_type_id,pv.price LIMIT 5000")
        with st.spinner("Querying Endur..."):
            raw=run_sql(q)
        if len(raw)==0:
            st.warning("No Endur rows found for this selection."); return
        raw=to_num(raw,["para_position","price","calculated_profile_volume_kwh","calculated_profile_volume","calculated_payment"])
        hc=["deal_number","reference","instrument_type_name","service_type","buy_sell_name","tran_status","deal_start_date","deal_end_date","trade_date","para_position"]
        st.markdown("**Deal header** (`deal_v2latest`)")
        st.dataframe(raw[hc].drop_duplicates(),use_container_width=True,hide_index=True)
        pc=["deal_number","location_name","ng_location_id","settlement_type_id","start_date","end_date","price","calculated_profile_volume_kwh","calculated_profile_volume","calculated_payment","unit_id","price_unit_id","volume_type_name"]
        st.markdown(f"**Profile rows** (`profilevolume_v2latest`, {len(raw)} rows)")
        st.dataframe(raw[pc],use_container_width=True,hide_index=True)
        st.caption("Raw, unprocessed and live from Endur (as it is now, not as it was at the time of the run). "
                   "All rows are shown, including price = 0 rows and every settlement type; the check itself only uses "
                   "settlement_type_id = 1 with price \u2260 0 (plus zero-price PRI_SEC sells where applicable).")

# === GAS-DAY PANEL (Results tab: select a daily item -> every gas day, invoice vs Equinor) ===
_EIC_ITEMS={"Entry at Interconnection Point":"Energy in Cash - Entry IP","Exit at Domestic Point":"Energy in Cash - Exit Dom","Exit at Interconnection Point":"Energy in Cash - Exit IP"}
_DAILY_NUM=["invoice_volume","invoice_eur","invoice_price","equinor_volume","equinor_eur","equinor_price","diff_volume","diff_eur","diff_pct"]

def _daily_item(row):
    """be_daily item for a summary row, or None if the row has no gas-day granularity (capacity, fixed fees)."""
    t=str(row.get("line_type") or row.get("type") or ""); it=str(row.get("item") or "")
    if t=="Energy in Cash": return _EIC_ITEMS.get(it,"Energy in Cash - "+it)
    if t=="Variable Trading Fee": return "ZTP Trading"
    if t=="Allocation Settlement": return "Alloc Settlement Sale" if "Sale" in it else "Alloc Settlement Buy"
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
        nflag=int((d["flag"]!="").sum())
        only=st.toggle(f"Only flagged days ({nflag})",value=False,key=key+"_only")
        v=d[["flag","gas_day"]+_DAILY_NUM].copy()
        if only: v=v[v["flag"]!=""]
        if len(v):
            s_=lambda c: v[c].sum(min_count=1)
            iv,ie,ev,ee=s_("invoice_volume"),s_("invoice_eur"),s_("equinor_volume"),s_("equinor_eur")
            dv=(ev-iv) if pd.notna(ev) and pd.notna(iv) else None
            tot=dict(flag="",gas_day="Total",invoice_volume=iv,invoice_eur=ie,invoice_price=(ie/iv if pd.notna(ie) and iv else None),
                     equinor_volume=ev,equinor_eur=ee,equinor_price=(ee/ev if pd.notna(ee) and ev else None),
                     diff_volume=dv,diff_eur=(ee-ie if pd.notna(ee) and pd.notna(ie) else None),diff_pct=(dv/abs(iv)*100 if dv is not None and iv else None))
            v=pd.concat([v,pd.DataFrame([tot])],ignore_index=True)
        disp,ncd,_=for_display(v,_DAILY_NUM)
        fmt={c:("{:,.6f}" if "Price" in c else "{:,.2f}") for c in ncd}
        def _rs(r):
            if str(r["Flag"] or "")!="": return ["background-color:#ffcdd2"]*len(r)
            if r["Gas Day"]=="Total": return ["font-weight:bold;background-color:#eeeeee"]*len(r)
            return [""]*len(r)
        st.dataframe(disp.style.format(fmt,na_rep="").apply(_rs,axis=1),use_container_width=True,hide_index=True)
        st.caption(f"Volumes in {unit}, prices in EUR per {unit}. Red = flagged day. The Total row adds up to the monthly figure in the summary"
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
t1,t2,t3,t5,t4 = st.tabs(["Upload Invoices","Run Check","Results","Overview","Run History"])

def _user_email():
    h=st.context.headers
    return h.get("x-forwarded-email") or h.get("x-forwarded-preferred-username") or "app_user"

with t1:
    st.header("Upload Invoice XML Files")
    st.caption("Upload Fluxys invoice XMLs. Auto-sorted into bill / self-bill.")
    files=st.file_uploader("Choose XML files",type=["xml"],accept_multiple_files=True)
    if files:
        for f in files:
            raw=f.read(); hdr=parse_xml_header(raw)
            if hdr is None: st.error(f"{f.name}: not a valid Fluxys XML."); continue
            it=hdr["invoice_type"]; sub="bill" if it=="Bill" else "self_bill"
            dest=f"{VOLUME}/{sub}/{f.name}"; volume_write(dest,raw)
            idate=date.fromisoformat(hdr["invoice_date"]); na=float(hdr["net_amount"]); im=idate.replace(day=1)
            ex=run_sql(f"SELECT file_name FROM {SCHEMA}.invoice_register WHERE invoice_number='{hdr['invoice_number']}'")
            if len(ex)>0:
                exec_sql(f"DELETE FROM {SCHEMA}.invoice_register WHERE invoice_number='{hdr['invoice_number']}'")
                st.warning(f"Replaced existing entry for invoice {hdr['invoice_number']}.")
            fn=f.name.replace("'","''")
            exec_sql(f"INSERT INTO {SCHEMA}.invoice_register (file_name,volume_path,invoice_type,invoice_number,invoice_date,invoice_month,net_amount_eur,file_size_bytes,uploaded_by,uploaded_at) VALUES ('{fn}','{dest}','{it}','{hdr['invoice_number']}','{idate}','{im}',{na},{len(raw)},'app_user',CURRENT_TIMESTAMP())")
            st.success(f"{f.name} -> {sub}/ ({it}, {hdr['invoice_number']}, EUR {na:,.2f})")
    st.subheader("Invoice Register")
    reg=run_sql(f"SELECT invoice_type,invoice_number,invoice_date,invoice_month,net_amount_eur,file_name,last_check_status FROM {SCHEMA}.invoice_register ORDER BY invoice_type,invoice_date")
    if len(reg)>0:
        reg=to_num(reg,["net_amount_eur"]); reg,_,_=for_display(reg)
        st.dataframe(reg,use_container_width=True,hide_index=True)
    else: st.info("No invoices uploaded yet.")

with t2:
    st.header("Run Invoice Check")
    bills=run_sql(f"SELECT DISTINCT invoice_month,file_name,volume_path FROM {SCHEMA}.invoice_register WHERE invoice_type='Bill' ORDER BY invoice_month")
    if len(bills)==0: st.info("No Bill invoices. Upload first.")
    else:
        ms=sorted(bills["invoice_month"].unique()); sm=st.selectbox("Invoice month",ms,index=len(ms)-1)
        br=bills[bills["invoice_month"]==sm].iloc[0]; bp=br["volume_path"]; bf=br["file_name"]
        st.write(f"**Bill:** {bf}")
        # Periods detected from the bill itself (same as v2 notebook)
        P=detect_periods(parse_invoice_all(volume_read(bp)), pd.to_datetime(sm).date())
        st.caption(f"Invoice month (M): {P['inv_m']} | Arrears VF/EiC: {P['ack']} | Alloc settlement: {P['sck'] or 'none on this bill'}")
        sbs=run_sql(f"SELECT file_name,volume_path FROM {SCHEMA}.invoice_register WHERE invoice_type='Self-Bill' ORDER BY invoice_date")
        sp=sf=None
        if P["ss"] is not None and len(sbs)>0:
            for _,sbr in sbs.iterrows():
                sbt=volume_read(sbr["volume_path"]); per=get_alloc_period(sbt,"Allocation Settl")
                if per and per.replace(day=1)==P["ss"].replace(day=1):
                    sp=sbr["volume_path"]; sf=sbr["file_name"]; break
        if sp: st.write(f"**Self-Bill:** {sf}")
        elif P["ss"] is not None: st.warning("No matching self-bill found (sales settlement will be zero).")
        if st.button("Run Check",type="primary"):
            la=st.empty(); logs=[]
            def log(m): logs.append(f"{datetime.utcnow().strftime('%H:%M:%S')} - {m}"); la.text_area("Progress","\n".join(logs),height=200)
            with st.spinner("Running..."):
                rid=str(uuid.uuid4())[:8]; started=datetime.utcnow()
                log(f"Reading bill: {bf}"); bb=volume_read(bp); df_all=parse_invoice_all(bb)
                log(f"Parsed {len(df_all)} entries.")
                P=detect_periods(df_all, pd.to_datetime(sm).date())
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
                    ds=to_num(ds,nc); sc=[c for c in ds.columns if "status" in c.lower() or "verdict" in c.lower()]
                    raw_s=ds.copy(); ds,nc,sc=for_display(ds,nc,sc)
                    ev=st.dataframe(styled(ds,nc,sc),use_container_width=True,hide_index=True,
                                    on_select="rerun",selection_mode="single-row",key=f"sel_{title}_s")
                    st.caption("Select a row to see the detail behind it: raw Endur deals for capacity rows, or every gas day for Energy in Cash, ZTP Trading and Allocation Settlement.")
                    if ev and ev.selection.rows:
                        r_=raw_s.iloc[ev.selection.rows[0]].to_dict()
                        if _daily_item(r_): render_daily_panel(r_,mf,f"dp_{title}")
                        else: render_endur_panel(r_,f"ep_{title}_s")
                else: st.info("No data.")
                # flagged issues are no longer shown on screen (gas-day panel replaces them); kept for the Excel 'Flagged' sheet
                df=run_sql(fsql)
                if len(df)>0:
                    nc=[c for c in df.columns if any(k in c.lower() for k in ["eur","kwh","volume","diff","pct","gap"])]
                    df=to_num(df,nc); df,nc,_=for_display(df,nc)
                sections.append((title,caption,ds,ft,df))
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
                    dd_=run_sql(f"SELECT item,check_month,billing_month,gas_day,flag,invoice_volume,invoice_eur,invoice_price,equinor_volume,equinor_eur,equinor_price,diff_volume,diff_eur,diff_pct,unit FROM {SCHEMA}.be_daily WHERE check_month IN ({mf}) ORDER BY check_month,section,item,gas_day")
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
        c1,c2=st.columns([3,1])
        mets=c1.multiselect("Show",list(MET),default=["Invoice volume","Invoice \u20ac","Equinor volume","Equinor \u20ac","Diff \u20ac"])
        thr=c2.number_input("Highlight month-on-month change above (%)",min_value=0.0,value=20.0,step=5.0)
        months=sorted(ovd["check_month"].unique())
        wide=pd.DataFrame(index=[_fmt_month(m) for m in months])
        mom_cols,price_cols,pct_cols=[],[],[]
        for it in sel_items:
            sub=ovd[ovd["item"]==it].drop_duplicates("check_month").set_index("check_month")
            unit=str(sub["volume_unit"].dropna().iloc[0]) if sub["volume_unit"].notna().any() else ""
            for m in mets:
                col=MET[m]
                if it=="Total variable cost" and ("volume" in col or "price" in col): continue
                lab=f"{it} \u00b7 {m}"+(f" ({unit})" if "volume" in col else (f" (\u20ac/{unit})" if "price" in col else ""))
                wide[lab]=sub[col].reindex(months).values
                if col in ("invoice_volume","invoice_eur","invoice_price","equinor_volume","equinor_eur","equinor_price"): mom_cols.append(lab)
                if "price" in col: price_cols.append(lab)
                if col=="diff_pct": pct_cols.append(lab)
        # month-on-month change vs the previous month shown in the table
        hi=pd.DataFrame("",index=wide.index,columns=wide.columns)
        for c in mom_cols:
            prev=wide[c].shift(1); chg=(wide[c]-prev)/prev.abs()
            hi.loc[(chg.abs()>thr/100).fillna(False).values,c]="background-color:#ffe0b2"
        numc=list(wide.columns)
        fmt={c:("{:,.6f}" if c in price_cols else "{:,.2f}") for c in numc}
        st.dataframe(wide.style.format(fmt,na_rep="").apply(lambda _:hi,axis=None),use_container_width=True)
        st.caption(f"Orange = changed by more than {thr:.0f}% versus the previous month in the table. Volume units: kWh (Alloc, Energy in Cash), MWh (ZTP Trading, Entry + Exit).")
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
        st.dataframe(styled(tv,tn2,ts2),use_container_width=True,hide_index=True)
