#!/usr/bin/env python3
"""
DeepView · Acciones — pipeline de datos (Yahoo Finance)
=======================================================
Versión optimizada v3.8 (Multi-index, cierres seguros, protección de volumen y redondeo a ventana programada local)
"""

import argparse, json, sys, time, math
import datetime as dt
import requests
from io import StringIO
import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo

OUT_POINTS = 380      # nº de sesiones que se mandan al gráfico (para MA200 completa)
MIN_HISTORY = 252     # mínimo de sesiones para entrar en el ranking
CHUNK = 80            # tamaño de lote en la descarga
BENCHMARK = {"symbol": "ACWI", "label": "ACWI (MSCI All-Country)"}

GICS_ES = {
    "Information Technology": "Tecnología", "Health Care": "Salud",
    "Financials": "Financiero", "Consumer Discretionary": "Consumo discr.",
    "Consumer Staples": "Consumo básico", "Communication Services": "Comunicaciones",
    "Industrials": "Industrial", "Energy": "Energía", "Materials": "Materiales",
    "Utilities": "Utilities", "Real Estate": "Inmobiliario",
}

# --- IBEX 35 ---
IBEX = {
    "SAN.MC": ("Banco Santander", "Financiero"), "BBVA.MC": ("BBVA", "Financiero"),
    "ITX.MC": ("Inditex", "Consumo discr."), "IBE.MC": ("Iberdrola", "Utilities"),
    "TEF.MC": ("Telefónica", "Comunicaciones"), "REP.MC": ("Repsol", "Energía"),
    "AENA.MC": ("Aena", "Industrial"), "FER.MC": ("Ferrovial", "Industrial"),
    "AMS.MC": ("Amadeus IT", "Tecnología"), "CLNX.MC": ("Cellnex", "Comunicaciones"),
    "CABK.MC": ("CaixaBank", "Financiero"), "SAB.MC": ("Banco Sabadell", "Financiero"),
    "ELE.MC": ("Endesa", "Utilities"), "NTGY.MC": ("Naturgy", "Utilities"),
    "ACS.MC": ("ACS", "Industrial"), "GRF.MC": ("Grifols", "Salud"),
    "MAP.MC": ("Mapfre", "Financiero"), "ANA.MC": ("Acciona", "Industrial"),
    "ANE.MC": ("Acciona Energía", "Utilities"), "RED.MC": ("Redeia", "Utilities"),
    "ENG.MC": ("Enagás", "Utilities"), "COL.MC": ("Inm. Colonial", "Inmobiliario"),
    "MRL.MC": ("Merlin Properties", "Inmobiliario"), "ACX.MC": ("Acerinox", "Materiales"),
    "MTS.MC": ("ArcelorMittal", "Materiales"), "IAG.MC": ("IAG", "Industrial"),
    "MEL.MC": ("Meliá Hotels", "Consumo discr."), "SLR.MC": ("Solaria", "Utilities"),
    "BKT.MC": ("Bankinter", "Financiero"), "LOG.MC": ("Logista", "Industrial"),
    "IDR.MC": ("Indra", "Tecnología"), "UNI.MC": ("Unicaja", "Financiero"),
    "ROVI.MC": ("Laboratorios Rovi", "Salud"), "PUIG.MC": ("Puig Brands", "Consumo básico"),
    "SCYR.MC": ("Sacyr", "Industrial"),
}

# --- Europa ---
EUROPE = {
    "ASML.AS": ("ASML Holding", "Tecnología"), "ADYEN.AS": ("Adyen", "Tecnología"),
    "SAP.DE": ("SAP", "Tecnología"), "SIE.DE": ("Siemens", "Industrial"),
    "ALV.DE": ("Allianz", "Financiero"), "DTE.DE": ("Deutsche Telekom", "Comunicaciones"),
    "MBG.DE": ("Mercedes-Benz", "Consumo discr."), "BMW.DE": ("BMW", "Consumo discr."),
    "VOW3.DE": ("Volkswagen", "Consumo discr."), "BAS.DE": ("BASF", "Materiales"),
    "MUV2.DE": ("Munich Re", "Financiero"), "IFX.DE": ("Infineon", "Tecnología"),
    "ADS.DE": ("Adidas", "Consumo discr."), "MC.PA": ("LVMH", "Consumo discr."),
    "RMS.PA": ("Hermès", "Consumo discr."), "OR.PA": ("L'Oréal", "Consumo básico"),
    "TTE.PA": ("TotalEnergies", "Energía"), "SAN.PA": ("Sanofi", "Salud"),
    "AIR.PA": ("Airbus", "Industrial"), "AI.PA": ("Air Liquide", "Materiales"),
    "SU.PA": ("Schneider Electric", "Industrial"), "EL.PA": ("EssilorLuxottica", "Salud"),
    "CS.PA": ("AXA", "Financiero"), "BNP.PA": ("BNP Paribas", "Financiero"),
    "DG.PA": ("Vinci", "Industrial"), "NESN.SW": ("Nestlé", "Consumo básico"),
    "NOVN.SW": ("Novartis", "Salud"), "ROG.SW": ("Roche", "Salud"),
    "ZURN.SW": ("Zurich Insurance", "Financiero"), "UBSG.SW": ("UBS Group", "Financiero"),
    "ENEL.MI": ("Enel", "Utilities"), "ENI.MI": ("Eni", "Energía"),
    "ISP.MI": ("Intesa Sanpaolo", "Financiero"), "UCG.MI": ("UniCredit", "Financiero"),
    "RACE.MI": ("Ferrari", "Consumo discr."), "STLAM.MI": ("Stellantis", "Consumo discr."),
    "AZN.L": ("AstraZeneca", "Salud"), "SHEL.L": ("Shell", "Energía"),
    "ULVR.L": ("Unilever", "Consumo básico"), "HSBA.L": ("HSBC", "Financiero"),
    "BP.L": ("BP", "Energía"), "RIO.L": ("Rio Tinto", "Materiales"),
    "NOVO-B.CO": ("Novo Nordisk", "Salud"), "NOKIA.HE": ("Nokia", "Tecnología"),
    "INVE-B.ST": ("Investor AB", "Financiero"), "VOLV-B.ST": ("Volvo", "Industrial"),
}

# --- Respaldo US ---
FALLBACK_US = {
    "NVDA": ("NVIDIA", "Tecnología", "NDX"), "AAPL": ("Apple", "Tecnología", "NDX"),
    "MSFT": ("Microsoft", "Tecnología", "NDX"), "AMZN": ("Amazon", "Consumo discr.", "NDX"),
    "META": ("Meta Platforms", "Comunicaciones", "NDX"), "GOOGL": ("Alphabet", "Comunicaciones", "NDX"),
    "AVGO": ("Broadcom", "Tecnología", "NDX"), "TSLA": ("Tesla", "Consumo discr.", "NDX"),
    "COST": ("Costco", "Consumo básico", "NDX"), "NFLX": ("Netflix", "Comunicaciones", "NDX"),
    "AMD": ("AMD", "Tecnología", "NDX"), "PEP": ("PepsiCo", "Consumo básico", "NDX"),
    "ADBE": ("Adobe", "Tecnología", "NDX"), "CSCO": ("Cisco", "Tecnología", "NDX"),
    "PLTR": ("Palantir", "Tecnología", "NDX"), "PANW": ("Palo Alto Networks", "Tecnología", "NDX"),
    "CRWD": ("CrowdStrike", "Tecnología", "NDX"), "MU": ("Micron", "Tecnología", "NDX"),
    "INTC": ("Intel", "Tecnología", "NDX"), "QCOM": ("Qualcomm", "Tecnología", "NDX"),
    "TXN": ("Texas Instruments", "Tecnología", "NDX"), "AMAT": ("Applied Materials", "Tecnología", "NDX"),
    "LRCX": ("Lam Research", "Tecnología", "NDX"), "ISRG": ("Intuitive Surgical", "Salud", "NDX"),
    "BKNG": ("Booking", "Consumo discr.", "NDX"), "ANET": ("Arista Networks", "Tecnología", "SP500"),
    "LLY": ("Eli Lilly", "Salud", "SP500"), "JPM": ("JPMorgan Chase", "Financiero", "SP500"),
    "V": ("Visa", "Financiero", "SP500"), "MA": ("Mastercard", "Financiero", "SP500"),
    "XOM": ("Exxon Mobil", "Energía", "SP500"), "CVX": ("Chevron", "Energía", "SP500"),
    "JNJ": ("Johnson & Johnson", "Salud", "SP500"), "PG": ("Procter & Gamble", "Consumo básico", "SP500"),
    "HD": ("Home Depot", "Consumo discr.", "SP500"), "MRK": ("Merck", "Salud", "SP500"),
    "ABBV": ("AbbVie", "Salud", "SP500"), "WMT": ("Walmart", "Consumo básico", "SP500"),
    "KO": ("Coca-Cola", "Consumo básico", "SP500"), "BAC": ("Bank of America", "Financiero", "SP500"),
    "GE": ("GE Aerospace", "Industrial", "SP500"), "CAT": ("Caterpillar", "Industrial", "SP500"),
    "UNH": ("UnitedHealth", "Salud", "SP500"), "WFC": ("Wells Fargo", "Financiero", "SP500"),
    "GS": ("Goldman Sachs", "Financiero", "SP500"), "MS": ("Morgan Stanley", "Financiero", "SP500"),
    "BLK": ("BlackRock", "Financiero", "SP500"), "NOW": ("ServiceNow", "Tecnología", "SP500"),
    "ORCL": ("Oracle", "Tecnología", "SP500"), "CRM": ("Salesforce", "Tecnología", "SP500"),
    "ACN": ("Accenture", "Tecnología", "SP500"), "MCD": ("McDonald's", "Consumo discr.", "SP500"),
    "NKE": ("Nike", "Consumo discr.", "SP500"), "DIS": ("Walt Disney", "Comunicaciones", "SP500"),
    "VZ": ("Verizon", "Comunicaciones", "SP500"), "T": ("AT&T", "Comunicaciones", "SP500"),
    "PFE": ("Pfizer", "Salud", "SP500"), "TMO": ("Thermo Fisher", "Salud", "SP500"),
    "LIN": ("Linde", "Materiales", "SP500"), "BA": ("Boeing", "Industrial", "SP500"),
    "HON": ("Honeywell", "Industrial", "SP500"), "RTX": ("RTX", "Industrial", "SP500"),
    "UNP": ("Union Pacific", "Industrial", "SP500"),
}

def _wiki_tables(url):
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
    }
    res = requests.get(url, headers=headers, timeout=30)
    return pd.read_html(StringIO(res.text))

def wiki_us():
    out = {}
    try:
        sp = _wiki_tables("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        for _, r in sp.iterrows():
            sym = str(r["Symbol"]).replace(".", "-").strip()
            sec = str(r.get("GICS Sector", "")).strip()
            out[sym] = (str(r["Security"]).strip(), GICS_ES.get(sec, sec or "—"), "SP500")
    except Exception as e:
        print(f"  ! S&P 500 desde Wikipedia falló: {e}")
        return None
    try:
        for t in _wiki_tables("https://en.wikipedia.org/wiki/Nasdaq-100"):
            symcol = next((c for c in t.columns if str(c) in ("Ticker", "Symbol")), None)
            namecol = next((c for c in t.columns if "Compan" in str(c)), None)
            seccol = next((c for c in t.columns if "Sector" in str(c) or "GICS" in str(c)), None)
            if symcol is None or namecol is None:
                continue
            for _, r in t.iterrows():
                sym = str(r[symcol]).replace(".", "-").strip()
                if not sym or sym == "nan":
                    continue
                nm = str(r[namecol]).strip()
                sec = str(r[seccol]).strip() if seccol else out.get(sym, ("", "—"))[1]
                out[sym] = (nm, GICS_ES.get(sec, sec or "—"), "NDX")
            break
    except Exception as e:
        print(f"  ! Nasdaq-100 desde Wikipedia falló (sigo con S&P 500): {e}")
    return out

def build_universe(use_wiki):
    us = wiki_us() if use_wiki else None
    if not us:
        if use_wiki:
            print("  [AVISO] No pude leer Wikipedia; uso la lista US curada (~65).")
        us = FALLBACK_US
    uni, seen = [], set()

    def add(sym, t, n, sec, idx):
        if sym in seen:
            return
        seen.add(sym)
        uni.append({"sym": sym, "t": t, "n": n, "sec": sec, "idx": idx})

    for sym, (nm, sec, idx) in us.items():
        add(sym, sym, nm, sec, idx)
    for sym, (nm, sec) in IBEX.items():
        add(sym, sym.replace(".MC", ""), nm, sec, "IBEX")
    for sym, (nm, sec) in EUROPE.items():
        add(sym, sym.split(".")[0], nm, sec, "EU")
    return uni

def download(symbols):
    import yfinance as yf
    allsyms = symbols + [BENCHMARK["symbol"]]
    closes, vols = {}, {}
    
    for i in range(0, len(allsyms), CHUNK):
        chunk = allsyms[i:i + CHUNK]
        print(f"  · {i + 1}–{min(i + CHUNK, len(allsyms))} / {len(allsyms)}…")
        df = None
        for attempt in range(3):
            try:
                df = yf.download(chunk, period="2y", interval="1d", 
                                 group_by="ticker", threads=True, progress=False)
                break
            except Exception as e:
                print(f"    reintento {attempt + 1}: {e}")
                time.sleep(2)
                
        if df is None or df.empty:
            continue
            
        for s in chunk:
            try:
                if len(chunk) == 1:
                    sub = df
                else:
                    sub = df[s] if s in df.columns.get_level_values(0) else None
                    
                if sub is None or sub.empty:
                    continue
                
                c_col = "Adj Close" if "Adj Close" in sub.columns else "Close"
                c = sub[c_col].dropna()
                v = sub["Volume"].dropna() if "Volume" in sub.columns else pd.Series(dtype=float)
                
                if c.shape[0] >= 60:
                    closes[s], vols[s] = c, v
            except Exception:
                continue
        time.sleep(0.5)
    return closes, vols

def synth_prices(universe):
    rng = np.random.default_rng(42)
    idx = pd.bdate_range(end=dt.date.today(), periods=504)
    days = len(idx)
    closes, vols = {}, {}
    for s in [u["sym"] for u in universe] + [BENCHMARK["symbol"]]:
        drift = rng.uniform(-0.0007, 0.0015)
        rets = drift + rng.normal(0, 0.013, days)
        closes[s] = pd.Series(100 * np.exp(np.cumsum(rets)), index=idx)
        vols[s] = pd.Series(rng.uniform(1e6, 6e6, days), index=idx)
    return closes, vols

def ret(s, k):
    return float(s.iloc[-1] / s.iloc[-1 - k] - 1.0) if len(s) > k else np.nan

def compute(closes, vols, universe):
    bsym = BENCHMARK["symbol"]
    if bsym not in closes:
        print("  ! Sin datos del benchmark; uso el primer valor como referencia.")
        bsym = next(iter(closes))
    bench = closes[bsym].dropna()
    common = bench.index

    px = pd.DataFrame({u["sym"]: closes[u["sym"]] for u in universe if u["sym"] in closes})
    px = px.reindex(common).ffill(limit=5).bfill(limit=5)
    
    valid = [c for c in px.columns if px[c].dropna().shape[0] >= MIN_HISTORY]
    print(f"  · {len(valid)} valores con histórico suficiente (≥{MIN_HISTORY} sesiones).")

    stat = {}
    for c in valid:
        s = px[c].dropna()
        r1, r3, r6, r12 = ret(s, 21), ret(s, 63), ret(s, 126), ret(s, 252)
        blended = np.nansum([0.2 * r1, 0.4 * r3, 0.2 * r6, 0.2 * r12])
        stat[c] = {"r1": r1, "r3": r3, "r6": r6, "r12": r12, "blended": blended}
    st = pd.DataFrame(stat).T

    def rank99(col):
        return (st[col].rank(pct=True) * 98 + 1).round().fillna(1).astype(int)

    rs, rs1, rs3, rs6, rs12 = (rank99("blended"), rank99("r1"), rank99("r3"),
                               rank99("r6"), rank99("r12"))

    bench_out = [round(float(x), 4) for x in bench.iloc[-OUT_POINTS:].tolist()]
    rows = []
    for c in valid:
        s = px[c].dropna()
        u = {u["sym"]: u for u in universe}[c]
        last = float(s.iloc[-1])
        prev = float(s.iloc[-2]) if len(s) > 1 else last
        chg = (last / prev - 1) * 100
        ma50 = float(s.rolling(50).mean().iloc[-1]) if len(s) >= 50 else np.nan
        ma200 = float(s.rolling(200).mean().iloc[-1]) if len(s) >= 200 else np.nan
        ma200p = float(s.rolling(200).mean().iloc[-21]) if len(s) >= 221 else np.nan
        win = s.iloc[-252:]
        hi, lo = float(win.max()), float(win.min())
        pfh, pfl = (last / hi - 1) * 100, (last / lo - 1) * 100
        
        v = vols.get(c, pd.Series(dtype=float)).reindex(common, fill_value=0).fillna(1.0)
        v_mean = v.iloc[-50:].mean()
        
        if v.shape[0] >= 50 and v_mean > 0:
            last_vol = v.iloc[-1]
            rv = float(last_vol / v_mean) if not (math.isnan(last_vol) or math.isnan(v_mean)) else 1.0
        else:
            rv = 1.0
            
        R = int(rs[c])

        c1 = (not math.isnan(ma50)) and last > ma50
        c2 = (not math.isnan(ma50)) and (not math.isnan(ma200)) and ma50 > ma200
        c3 = (not math.isnan(ma200)) and (not math.isnan(ma200p)) and ma200 > ma200p
        c4 = (not math.isnan(pfh)) and pfh >= -25
        c5 = (not math.isnan(pfl)) and pfl >= 30
        c6 = R >= 70
        crit = [
            {"ok": bool(c1), "tx": "Precio sobre la MA50",
             "vx": f"{last:.2f} / {ma50:.2f}" if not math.isnan(ma50) else "—"},
            {"ok": bool(c2), "tx": "MA50 sobre MA200",
             "vx": f"{ma50:.2f} / {ma200:.2f}" if not math.isnan(ma200) else "—"},
            {"ok": bool(c3), "tx": "MA200 inclinada al alza", "vx": "sí" if c3 else "no"},
            {"ok": bool(c4), "tx": "A < 25% del máximo de 52s", "vx": f"{pfh:.1f}%"},
            {"ok": bool(c5), "tx": "A > 30% del mínimo de 52s", "vx": f"+{pfl:.0f}%"},
            {"ok": bool(c6), "tx": "RS Rating ≥ 70", "vx": str(R)},
        ]
        cnt = sum(1 for x in crit if x["ok"])
        pser = s.ffill().bfill()
        rows.append({
            "sym": c, "t": u["t"], "n": u["n"], "idx": u["idx"], "sec": u["sec"],
            "rs": R, "rs1m": int(rs1[c]), "rs3m": int(rs3[c]),
            "rs6m": int(rs6[c]), "rs12m": int(rs12[c]),
            "px": round(last, 2), "chg": round(chg, 2),
            "rv": round(rv, 2) if not (math.isnan(rv) or math.isinf(rv)) else 1.0,
            "pctFromHigh": round(pfh, 2), "pctFromLow": round(pfl, 2),
            "trendCount": cnt, "trendOK": cnt == 6, "crit": crit,
            "prices": [round(float(x), 4) for x in pser.iloc[-OUT_POINTS:].dropna().tolist()],
        })
    rows.sort(key=lambda r: r["rs"], reverse=True)
    return rows, bench_out

def write(rows, bench_out, path_js, path_json):
    ventanas_horarias = [
        "09:00", "10:30", "12:00", "14:00", "15:30", 
        "17:00", "18:30", "20:00", "21:30", "23:00"
    ]
    
    tz_madrid = ZoneInfo("Europe/Madrid")
    ahora = dt.datetime.now(tz_madrid)
    minutos_ahora = ahora.hour * 60 + ahora.minute
    
    ventana_elegida = ventanas_horarias[0]
    min_diff = float('inf')
    for v in ventanas_horarias:
        h, m = map(int, v.split(":"))
        minutos_v = h * 60 + m
        diff = abs(minutos_ahora - minutos_v)
        if diff < min_diff:
            min_diff = diff
            ventana_elegida = v

    # Guardamos la marca temporal exacta con espacio (ej: "2026-06-23 21:30")
    timestamp_oficial = f"{ahora.strftime('%Y-%m-%d')} {ventana_elegida}"

    feed = {
        "generatedAt": timestamp_oficial,
        "benchmark": BENCHMARK["label"],
        "benchmarkPrices": bench_out,
        "universeSize": len(rows),
        "rows": rows,
    }
    js = "window.DEEPVIEW_FEED = " + json.dumps(feed, ensure_ascii=False, separators=(",", ":")) + ";\n"
    with open(path_js, "w", encoding="utf-8") as f:
        f.write(js)
    with open(path_json, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, separators=(",", ":"))
    print(f"  ✓ {path_js} ({len(js) / 1e6:.2f} MB) y {path_json} escritos · {len(rows)} valores.")

def main():
    ap = argparse.ArgumentParser(description="Pipeline de datos DeepView (Yahoo Finance)")
    ap.add_argument("--no-us-wiki", action="store_true", help="usa lista US curada en vez de Wikipedia")
    ap.add_argument("--selftest", action="store_true", help="sin red: precios sintéticos para validar")
    ap.add_argument("--out", default="feed.js", help="ruta de salida del .js (def. feed.js)")
    a = ap.parse_args()

    print("DeepView · pipeline de datos\n" + "-" * 32)
    uni = build_universe(use_wiki=not a.no_us_wiki)
    n = lambda k: sum(1 for u in uni if u["idx"] == k)
    print(f"Universo: {len(uni)} símbolos  (IBEX {n('IBEX')} · S&P {n('SP500')} · "
          f"Nasdaq {n('NDX')} · Europa {n('EU')})")

    if a.selftest:
        print("Modo selftest: precios sintéticos (sin Yahoo).")
        closes, vols = synth_prices(uni)
    else:
        print("Descargando de Yahoo Finance…")
        closes, vols = download([u["sym"] for u in uni])

    if not closes:
        print("Sin datos. Aborto.")
        sys.exit(1)

    rows, bench = compute(closes, vols, uni)
    write(rows, bench, a.out, a.out.replace(".js", ".json"))

    led = sum(1 for r in rows if r["rs"] >= 80)
    tok = sum(1 for r in rows if r["trendOK"])
    print(f"Resumen: {len(rows)} valores · {led} líderes (RS≥80) · {tok} con plantilla OK.")
    print("Top 5 por RS:", ", ".join(f"{r['t']}({r['rs']})" for r in rows[:5]))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrumpido.")
