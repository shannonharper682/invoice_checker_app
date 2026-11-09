# Belgium Invoice Checker - knowledge base

This file is sent to the AI with every "Ask AI" request in the Results tab. It describes how the app lines up
the Fluxys Belgium bill against Equinor's Endur bookings, the special cases we know about, and past findings.
Edit it freely: add new findings to "Known cases" so the AI learns from them.

## 1. Sources

* **Fluxys Bill XML** (monthly, one per invoice month). Every `CustomerProduct` -> `InvoiceLine` (has
  `BillingStartDate`, `BillingType`, `InvoiceLineGroup`, `InvoiceLine` = location, `InvoiceLineDetail`) ->
  `BilledQuantity` (one per contract/day, has `EconomicStartDate/EndDate`, `BilledQuantityAmount` = EUR,
  `PriceFormulaInformation`, `AdditionalInformation` with ContractReference, ServiceReference,
  ServiceStart/EndGasDay, ServiceStart/EndGasHour, ServiceRateType, CapacityType, Direction).
* **Fluxys Self-Bill XML**: only used for Allocation Settlement sales (Sales H Grid).
* **Endur** (`ms_atlas.endur_standard`): `deal_v2latest` -> `profilevolume_v2latest` (deal_number) ->
  `location_v2latest` (location_id). Capacity deals are `instrument_type_name` COMM-CAP-ENTRY / COMM-CAP-EXIT,
  `service_type` Firm or Interruptible, `tran_status` Validated, Belgian locations only.

## 2. Bill price formula (capacity)

* LongTerm (yearly rate): `QTY * BILLp * UP / UNITp` - QTY kWh/h, UP EUR/kWh/h/year, BILLp = days billed
  in the month, UNITp = 365. Fluxys bills per DAY, not per hour (a 23 h or 25 h clock-change day counts as 1 day).
* Season (daily/within-day rate): `QTY * BILLp * UP / UNITp * SCmm * NYM` - one line per gas day, BILLp = 1,
  SCmm = seasonal multiplier (e.g. 0.5 in summer), NYM = short-term multiplier (e.g. 1.45).
  Within-day lines (`InvoiceLineDetail` contains "(Within-Day)") cover only the booked hours
  (ServiceStart/EndGasHour).
* Auction premium: separate lines, `InvoiceLineDetail` "Auction Premium on ...", UP unit EUR/kWh/h/Runtime,
  location shown as "Auction Premium" (no real location). The app links them to a location through the
  AUC-<id> contract numbers that also appear on that location's tariff lines.
* Negative QTY lines = capacity Fluxys moved off a contract (e.g. OCUC capacity converted to an auction
  contract AUC-...). A matching positive line usually appears on the receiving contract.
* `BillingType` is normally "Billing". "BackBilling" = a correction for an earlier month; it will not match
  the normal monthly amount and should be compared against what changed in that earlier month.
* **How to recognise back-billing (do not rely on `BillingType` alone):** capacity tariffs are normally billed
  LongTerm for the invoice month (M) and Season (daily) for M and M-1. A capacity line whose billing month is
  older than that (LongTerm for M-1 or earlier, Season for M-2 or earlier, e.g. February lines on the May bill)
  is back-billing, even when the XML says `BillingType` = "Billing". Example May 2026 bill: Feb 2026 Season
  Entry, Zeebrugge -2,787.03 EUR (BackBilling, SRV-IMPLICIT-404737, "Firm - Implicit Capacity Allocation") and
  ZPT +2,787.05 EUR (marked "Billing", SRV-410830): the same capacity moved from Zeebrugge to ZPT, net ~0.
  The checker compares these against the full month of Endur bookings, so large gaps / No Endur Match are
  expected and are NOT invoice errors; judge them by the positive and negative lines netting off.

## 3. Endur price and volume conventions

* Use `settlement_type_id = 1`. There are two rows per month per deal: one with price 0 and one with the real
  price - only the priced row counts. `settlement_type_id = 2` rows are reversals/contra entries; ignore.
* `calculated_profile_volume_kwh` is negative for buys and positive for sells. Volume = capacity kWh/h x hours
  in the period, and Endur IS clock-change aware (March month 743 h, October 745 h).
* `pv.price` units: **EUR/MWh** on OCUC/bilateral deals (price >= 0.01, e.g. 0.1515) and **EUR/kWh** on
  PRISMA deals (price < 0.01, e.g. 0.000153756). EUR = volume x price (/1000 when EUR/MWh).
  Conversion: EUR/kWh/h/year = EUR/MWh x 8.76 (365 x 24 / 1000).
* Endur often stores OCUC/bilateral prices as the bill tariff ROUNDED to 3 or 4 decimals in EUR/MWh
  (e.g. 0.1515 for 0.151484, 0.149 for 0.149201). On large volumes this gives EUR gaps proportional to volume:
  up to MWh x 0.00005 with 4 decimals, up to MWh x 0.0005 with 3 decimals. Buys and sells at the same rounded
  price partly offset each other. The app's "Price effect by Endur deal" table shows this per deal.
* Within-day PRISMA deals with a single-id reference `PRI-<id>-ENTRY/EXIT` (ZPT, Zeebrugge) have the price
  pro-rated by booked hours / 24 in Endur; the app re-bases them to a full day. Two-id references
  `PRI-<id>-<id>-...` (Virtualys, VIP BENE) already hold the full tariff - no re-basing.
* Clock-change days (DST): gas day before the last Sunday of March = 23 h, before the last Sunday of October = 25 h.
  Fluxys bills capacity per DAY (a 23 h / 25 h day is a full day: QTY x days x UP / 365), Endur values it per HOUR.
  The checker therefore puts Endur EUR on the Fluxys per-day basis: x (days x 24) / actual hours (v1.13.3).
  Capacity kWh/h always uses the actual hours. Within-day bookings only gain/lose the hour if they cover
  02:00-03:00 local. Verified: ZPT / Zeebrugge Season 28 Mar 2026 Endur = exactly 23/24 of the bill;
  VIP THE-ZTP LongTerm Oct 2026 Endur = bill x 745/744 (+36.32 EUR, 0.134 %).
* Deal duration classifies Endur deals: >= 360 days Yearly, >= 85 Quarterly, >= 27 Monthly, else Daily.
  Non-Daily deals are compared to LongTerm bill lines, Daily deals to Season bill lines.

## 4. How the app matches a capacity row

* Invoice location -> Endur `ng_location_id`: ZPT = BE.FLX.ZBEEEntry, Zeebrugge = BE.FLX.ZBHUBEEEntry-Exit,
  VIP THE-ZTP = BE.VIP.THE.ZTP, VIP BENE = BE.FLX.VIPBENE, Virtualys = BE.FLX.BL. Direction comes from the
  Endur instrument type (ENTRY/EXIT), never from the location name (VIP THE-ZTP holds both).
* Grouping: billing month x direction x location x rate type (LongTerm/Season) x capacity type (Firm/Interruptible).
* LongTerm: Endur capacity = NET kWh/h (|sum of signed kWh/h|), Equinor EUR = |sum of signed volume x price|.
  Invoice capacity = sum of QTY (net of negative lines).
* Season: compared per gas day (Gas-day breakdown); Endur capacity = average kWh/h per booked day.
* Periods: Capacity M = billing month equals invoice month; Capacity M-1 = previous month (late/back-billed
  capacity, typically Season days at the end of the previous month); Variable Fees M-3 = Energy in Cash,
  ZTP Trading fee, Allocation Settlement purchases (gas month about 3 months earlier).
* Auction premium rows: if a location has auction premium lines, the tariff row's Equinor EUR is set to the
  invoice capacity x invoice tariff (so the tariff row shows 0 gap) and the Auction Premium row gets
  Equinor EUR = Endur EUR - that tariff amount. So any Endur price difference shows up on the premium row.
* Flags: capacity gap > 1 %, price gap > 1 %, "NO ENDUR MATCH" (bill line with no Endur deal),
  "ENDUR ONLY" (Endur deal not on the bill), "UNMAPPED ENDUR LOCATION".

## 5. Location specifics

* **VIP BENE (Exit)**: Fluxys bills the GROSS OCUC holding to the original holder (Equinor), regardless of
  secondary trades. Do not net PRI_SEC sells at VIP BENE. Example: PRI_SEC_97383543_EXIT (Nov 2025-Mar 2026,
  644,571 kWh/h) is NOT on the bill; Fluxys still bills 1,025,800 kWh/h. OCUC deal 4342359 "Exit VIP-BENE
  (from OCUC)". Apr-Jun 2026: 512,900 kWh/h converted to auction contract AUC-99482150 - on the bill as
  -512,900 on SRV-354716/1/2 and +512,900 on AUC-99482150; in Endur as PRISMA buy 5709558 and
  "unbundled conversion" sell 5709673, which cancel.
* **Virtualys (Exit)**: Fluxys bills the NET position after sell-backs. Gas-year sell-backs are PRI_SEC sell
  deals (e.g. PRI_SEC_96269500_EXIT, deal 5499327, GY25 = 2,292,322 kWh/h) and MUST be included. They always
  have a zero-price row and may or may not have a priced row (June 2026: priced at 0.0001492 EUR/kWh). Only
  when a sell-back has no priced row does the app value it at the group's average priced rate (approximation).
  Main deals: 3279430 "Finpipe cap Virtualys (CS0015)" buy, 3412979 "Settlement" sell, 3503912
  "Assignment sell Engie GY21-25" sell.
  Bill shows several negative lines (SRV-146193/1/2, SRV-272790/1/1, TRD-...) off the main contract.
* **VIP THE-ZTP**: Endur location holds both Entry and Exit deals; always match on direction. Has auction
  premium contracts (AUC-68778348, AUC-93517046).
* **ZPT / Zeebrugge (Entry)**: mostly LongTerm plus many daily/within-day Season bookings.
* **Interruptible**: rare. "Overnominated Interruptible/Backhaul" (e.g. Virtualys SRV-OVERNOM-408992, 12 h)
  is an over-nomination CHARGE, not a booking - no Endur deal is expected, so "NO ENDUR MATCH" is normal.
  Other interruptible bookings have not been seen often; treat matches with care.
* "Firm - Implicit Capacity Allocation" capacity type has appeared once; not specifically handled.

## 5b. Daily items (Variable Fees M-3 and Self Bill sections)

These are billed about 3 months in arrears (e.g. the August bill carries May gas days) and compared per gas day.

* **Energy in Cash** (bill lines: `InvoiceLine` = "Energy in Cash", `InvoiceLineGroup` = Entry at Interconnection Point /
  Exit at Interconnection Point / Exit at Domestic Point; formula `QTY*UP`, QTY kWh, UP EUR/kWh "Daily Price"; one line
  per gas day and connection point, e.g. Entry IP = ZPT + IZT, Exit IP = VIP BENE + VIP THE-ZTP + Virtualys,
  Exit Dom = end users like AIR LIQUIDE ANTWERPEN, PP/T-POWER TESSENDERLO).
  Equinor side: dispatch `preliminary_allocation` (ms_atlas.dispatchnatgas_standard.hourly_v2latest, country BE,
  contract_type Flow) per gas day x 0.0008 (energy-in-cash share). Entry IP = sum of positive flows, Exit IP = sum of
  negative flows (hub-internal ZBHUBEE/ZBHUBFL and domestic points excluded), Exit Dom = TESSENDERLO + ANTWERP JUPITER.
  Price = EEX ZTP daily spot index EGSI (ms07splitprice eex_v1latest, EEX_NaturalGas_EGSI / 'ZTP,EGSI_D' / Price,
  published the day before the gas day) / 1000 = EUR/kWh. Same values as the old Eex_EuropeanGasSpotIndex 'ZTP,Day'
  series, which stopped at publication 2026-07-09 (the app uses EGSI from v1.14.2).
  Equinor EUR = Equinor kWh x that price. If the EEX table has no price yet for a gas day (it can lag behind the
  bill, e.g. October 2026 bill: no index after 10 July 2026), the day is NOT a price mismatch: Equinor EUR is valued
  at the invoice price and only the quantity is checked (detail column says so).
  Flags: quantity gap > 1 % (QTY) or price mismatch (PRICE, only when an EEX price exists). Smaller gaps are not flagged.
  Exit Dom connection points on the bill vs dispatch location_id (verified Oct 2026 bill, July 2026, every day
  within 0.5 kWh): "AIR LIQUIDE ANTWERPEN SITE BASF 2" (code 007134) = dispatch ANTWERP JUPITER;
  "PP/T-POWER TESSENDERLO" (code 005955) = dispatch TESSENDERLO. They are ALREADY mapped - do not suggest adding
  them again (it would double count). Dispatch has no separate BASF / Air Liquide location.
  Typical causes: preliminary vs final allocation (dispatch is preliminary), a connection point missing from the
  dispatch mapping, a new end-user/domestic point, a price publication-date shift.
* **ZTP Trading variable fee** (`InvoiceLineGroup` ZTP Trading, `InvoiceLine` Variable Fee; QTY MWh x 0.00195 EUR/MWh,
  separate Entry and Exit lines per day). Equinor side = exit MWh from the shipper mapping + Endurast AST, x 2
  (entry = exit). Flags: exit quantity gap > 1 %.
* **Allocation Settlement** purchases (bill) and sales (self-bill, "SB - Allocation Settlements - Sales") vs dispatch,
  priced at the ZTP index; settlement month is earlier than the bill month. Flags: price mismatch, entry without
  dispatch or vice versa, qty > 10 kWh, EUR > 1.
* Flag levels (v1.14.0): RED issue = numbers do not match; ORANGE "TECHNICAL: ..." = data missing or incomplete,
  what could be checked matches (e.g. no EEX ZTP price for a gas day -> quantity-only check; EiC verdict
  "TECHNICAL - no EEX price on N day(s), quantities verified"); YELLOW "NOTICE: ..." = unusual line but numbers
  match (Interruptible capacity, BackBilling lines). Treat orange/yellow as information, not invoice errors.
* Flag tolerance: issues are flagged above 1 % (capacity, price, quantity). This is deliberate - flagging every
  difference raised many ~0 % flags. A row can therefore show a small EUR gap with no flag.

## 6. Known cases (past findings)

* August 2026 bill (May 2026 gas days), Energy in Cash: all three categories differ only on 7 May 2026
  (Entry IP invoice 307,088 kWh vs Equinor 294,293 kWh = -564.22 EUR; Exit IP -51.69 EUR; Exit Dom -7.34 EUR);
  every other day matches within 0.05 EUR. Same price on both sides (0.044097 EUR/kWh) -> a quantity difference
  on that one day, likely the allocation used.

* June 2026 VIP BENE Exit: gap +11.80 EUR. Volumes equal (1,025,800 kWh/h, 738,576 MWh). Endur OCUC price
  0.1515 EUR/MWh (4 decimals) vs bill 1.327 EUR/kWh/h/year = 0.151484 EUR/MWh. Price rounding in Endur, not
  a wrong tariff.
* June 2026 Virtualys Exit: gap -793.02 EUR, volumes equal (3,206,359 kWh/h). Explained exactly by price
  rounding: deals 3279430, 3412979 and 3503912 are priced 0.149 EUR/MWh in Endur (3 decimals) vs bill
  1.307 EUR/kWh/h/year = 0.1492009 EUR/MWh -> -1,394.34 + 405.04 + 194.78 EUR; PRI_SEC 5499327 at 0.1492
  -> +1.50 EUR. Total -793.01. Not a wrong tariff.
* Older finding: OCUC price 0.146 EUR/MWh (~1.28 EUR/kWh/h/year) vs bill 1.327 = ~3.6 % tariff indexation gap
  (Endur tariff not updated for the new tariff year).
* April 2026 bill, M-1 (March 2026) ZPT Season -281.10 EUR and Zeebrugge -164.55 EUR with matching capacity:
  explained by the 23 h gas day 28 Mar 2026 (Endur 23 h vs Fluxys full day); removed by the per-day basis (v1.13.3).
* October 2026 bill: ZPT LongTerm Entry +23,450.89 EUR (2.24 %); after the DST per-day basis still about +22,011 EUR
  (2.10 %) with equal capacity -> a price/tariff difference, not DST. VIP BENE Exit capacity 1,525,800 (Endur) vs
  1,025,800 (bill) -> an extra 500,000 kWh/h in Endur = deal 5722845 PRI_SEC_100725782_EXIT, a secondary buy from
  Engie (transfer of use, not billed by Fluxys). Since v1.18.0 only deals with Fluxys as counterparty are compared,
  so it is left out (shown under 'Not included' in the Endur drill-down).
* Within-day price pro-rating (see section 3) previously doubled Virtualys Apr 2026 Season EUR; fixed.

## 7. Typical causes to consider

* Price rounding in Endur (volume-proportional, volumes equal; Endur price = bill tariff rounded to 3-4 decimals).
* Tariff not updated in Endur after a Fluxys tariff change (about 1-4 %, every location on that tariff).
* Missing or extra deal in Endur (capacity gap), deal not Validated, deal booked on the wrong location or
  direction, wrong Firm/Interruptible service type.
* Secondary trades netted where Fluxys bills gross (VIP BENE), or not netted where Fluxys bills net (Virtualys).
* Back-billing / corrections (`BillingType` BackBilling, or capacity lines older than M for LongTerm / M-1
  for Season) not matching the normal month - the checker does not reconcile these against Endur yet.
* Clock-change months (March/October): Fluxys bills per day, Endur per hour.
* Within-day bookings: hours and pro-rated prices.
* Auction premium rows mixing tariff and premium.
