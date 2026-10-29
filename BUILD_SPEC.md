# Belgium Invoice Checker — Demo App (build spec)

App name: `belgium-invoice-demo`  |  Source folder: `/Users/shlh@equinor.com/belgium-invoice-app-demo/`
Pattern: same as KC0110 cost-split app (Streamlit, user-token auth with `sql` scope, SQL warehouse via databricks-sql-connector, WorkspaceClient for volume file I/O).

## Hard rules (isolation)
- Write ONLY to `ms_vulcan_gpgtopm_plab.belgium_app_demo_v1` (tables + volume).
- Do NOT read from or write to `be_invoicev2`, `be_invoice`, `combined_belgium_invoices`.
- Do NOT modify the notebook `invoice checking/invoice-checker-belgium/Invoice Checker Belgium v2` or the dashboard `Belgium Invoice PDF Format` (01f1b8c589ae191ea713bc86a91dd8c7). They are reference only.
- Write a brand-new `app.py` (no Spark available in Apps): port the notebook's Spark pulls to SQL warehouse queries and keep comparison logic in pandas.

## Storage (already created)
Schema: `ms_vulcan_gpgtopm_plab.belgium_app_demo_v1`

Volume `invoices` at `/Volumes/ms_vulcan_gpgtopm_plab/belgium_app_demo_v1/invoices/`:
- `bill/` — 6 Fluxys Bill XMLs (Jan–Jun 2026)
- `self_bill/` — 6 Self-Bill XMLs (Feb–Jun, Sep 2026)
- `reference/mapping_new.xlsx` — shipper mapping (replaces Workspace MAPPING_PATH)

Tables:
| Table | Contents |
|---|---|
| be_m, be_m_flagged | billing_month == invoice month (LTC Tariff, LTC Auction Premium, Fixed Fee) |
| be_m_minus_1, be_m_minus_1_flagged | billing_month == M-1 (LTC back-billing) |
| be_m_minus_3, be_m_minus_3_flagged | billing_month < M-1 (Variable Trading Fee, Energy in Cash, Allocation Settlement purchases, Fixed Fee) |
| be_alloc, be_alloc_flagged | Self-bill allocation settlement (sales) |
| be_other | Safety net (unclassified) |
| invoice_register | One row per XML in the volume (file_name, volume_path, invoice_type Bill/Self-Bill, invoice_number, invoice_date, invoice_month, net_amount_eur, file_size_bytes, uploaded_by, uploaded_at, last_checked_at, last_check_status) — seeded with 12 rows |
| run_log | One row per Run Check (run_id, invoice_file, selfbill_file, invoice_month, started_at, finished_at, run_by, status, message, rows_written) |

Result tables are seeded with a copy of be_invoicev2 (invoice months 2026-01, -02, -03, -06) so the Results tab has data on day one. Schemas are identical to be_invoicev2:
- Summary: check_month, invoice_file, invoice_month, billing_month, type, product_name, location, direction, invoice_eur, invoice_volume, equinor_eur, equinor_volume, diff_eur, diff_pct, status, run_timestamp
- Flagged: check_month, invoice_file, billing_month, type, gas_day, location, direction, invoice_value, equinor_value, diff, diff_pct, unit, flag_detail, run_timestamp
- `check_month` = invoice month as 'YYYY-MM-01' string. Re-runs are idempotent: delete rows for check_month, then append.

## Tab 1 — Upload Invoices
- File uploader (XML only, multiple). Read `InvoiceDetailHeader` (InvoiceType, InvoiceNumber, InvoiceDate, InvoiceNetAmount) to auto-route: Bill -> `bill/`, Self-Bill -> `self_bill/`. Reject non-Fluxys XML.
- Warn on duplicate invoice_number (offer overwrite).
- Insert/merge row into invoice_register (uploaded_by = current user).
- Below: table of invoice_register (filter by type), showing which months have/haven't been checked.

## Tab 2 — Run Check
- Select invoice month (from invoice_register Bill rows). App auto-picks the Bill XML and the matching Self-Bill XML (same logic as notebook: self-bill whose Allocation Settlement period matches); allow override.
- Run pipeline with step-by-step progress (st.status), same order as notebook:
  1. Parse invoice XML (unified parser `parse_invoice_all`)
  2. LTC — Endur pull (ROUTE_MAP, seasonal window, COMM-CAP-EXIT/ENTRY, price != 0, PRI_SEC carve-out at Virtualys) & Compare/Flag
  3. Allocation Settlement — parse self-bill + dispatch (`ms_atlas.dispatchnatgas_standard.hourly_v2latest`) & Compare/Flag
  4. Variable Fee — ZTPH volumes (mapping_new.xlsx from volume) & Compare
  5. Energy in Cash — Equinor pull (price `ms_atlas.ms07splitprice_standard.eex_v1r0`, Eex_EuropeanGasSpotIndex, ZTP,Day, Index) & Results
  6. Unified Save by billing period -> demo schema tables; write run_log; update invoice_register.last_checked_at/status
- Show overall summary (counts by status) on completion and link to Results tab.
- Source of truth for every rule, constant and threshold: notebook `Invoice Checker Belgium v2` cells 2–14. Port faithfully; do not change tolerances.

## Tab 3 — Results (replicates 'Belgium Invoice PDF Format' layout, top to bottom)
Global filter at top: **Invoice Month** (multi-select, from distinct check_month, displayed 'MMMM yyyy'; default = latest). All sections filter by check_month.

| # | Header + caption (copy exactly) | Summary table | Flagged table |
|---|---|---|---|
| 1 | **Capacity M** — "Capacity bookings where the billing month equals the invoice month." | be_m | be_m_flagged (title "Capacity M — Flagged Issues") |
| 2 | **Capacity M-1** — "Capacity bookings where the billing month is one month prior to the invoice month." | be_m_minus_1 | be_m_minus_1_flagged ("Capacity M-1 — Flagged Issues") |
| 3 | **Variable Fees M-3** — "Variable fees and energy in cash for the invoice month (gas month is M-3)." | be_m_minus_3 | be_m_minus_3_flagged ("Variable Fees M-3 — Flagged Issues") |
| 4 | **Self Bill** — "Sales H Grid (allocation settlement)" | be_alloc (line_type 'Sales H Grid') | be_alloc_flagged ("Sales H Grid — Flagged Issues") |

Columns, in dashboard order (map to V2 columns):
- Capacity summaries: invoice_month, billing_month, direction, line_type(=type), location, capacity_type(from product_name), invoice_eur, equinor_eur, invoice_kwh_h(=invoice_volume), endur_kwh_h(=equinor_volume), gap_eur(=diff_eur), plus status
- Capacity flagged: gas_day, direction, location, booking_type(=type), invoice_value, endur_value(=equinor_value), gap(=diff), gap_pct(=diff_pct), note(=flag_detail)
- Variable Fees summary: invoice_month, billing_month, line_type(=type), invoice_eur, equinor_eur, eur_diff, qty_diff_pct(=diff_pct), days_flagged (count from flagged table), verdict(=status)
- Variable Fees flagged: invoice_month, billing_month, gas_day, line_type, inv_qty, eq_qty, qty_diff, qty_diff_pct, unit, flag
- Sales H Grid summary: invoice_month, billing_month, line_type, invoice_eur, equinor_eur, eur_diff, verdict
- Sales H Grid flagged: gas_day, invoice_value, equinor_value, diff, diff_pct, flag

Presentation: EUR with thousands separators 2dp; status colour (OK green / FLAG red); empty flagged table shows "No flagged issues". Button "Download PDF" renders the 4 sections in the same order (reportlab), A4 landscape, title 'Belgium Invoice Check — <Invoice Month>'.

## Permissions needed
- User auth (sql scope) with SELECT on Atlas sources (Endur, dispatch, price) and ALL on belgium_app_demo_v1 + READ/WRITE VOLUME on invoices.
- SQL warehouse id via app.yaml resource.
