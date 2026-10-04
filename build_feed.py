#!/usr/bin/env python3
"""
DeepView · Acciones — pipeline de datos (Yahoo Finance)
=======================================================
Descarga OHLCV ajustado del universo (IBEX 35, S&P 500, Nasdaq-100, Europa),
calcula el RS Rating cross-sectional (percentil vs todo el universo) y la
plantilla de tendencia, y genera feed.js (+ feed.json) que consume
deepview_acciones.html[cite: 9].
"""

import argparse, json, sys, time, math
import datetime as dt
import urllib.request
from io import StringIO
import numpy as np
import pandas as pd

OUT_POINTS = 380      # nº de sesiones que se mandan al gráfico (para MA200 completa)[cite: 9]
MIN_HISTORY = 252     # mínimo de sesiones para entrar en el ranking[cite: 9]
CHUNK = 80            # tamaño de lote en la descarga[cite: 9]
BENCHMARK = {"symbol": "ACWI", "label": "ACWI (MSCI All-Country)"}[cite: 9]

# --- CONFIGURACIÓN CENTRALIZADA DE ESTRATEGIA ---
STRATEGY_CONFIG = {
    "min_rs_rating": 70,           # RS mínimo para plantillas de tendencia / VCP[cite: 9]
    "flow_lookback": 20,           # Ventana para días de acumulación[cite: 9]
    "min_flow_move": 0.002,        # Movimiento mínimo porcentual para acumulación[cite: 9]
    "dry_lookback": 10,            # Ventana de sesiones para análisis de volumen seco[cite: 9]
    "dry_vol_threshold": 0.55,     # Umbral de volumen relativo para día seco (55%)[cite: 9]
    "dry_move_threshold": 0.012,   # Rango de precio máximo permitido en día seco (1.2%)[cite: 9]
    "climax_vol_mult": 1.80,       # Multiplicador de volumen para clímax vendedor[cite: 9]
}

GICS_ES = {
    "Information Technology": "Tecnología", "Health Care": "Salud",
    "Financials": "Financiero", "Consumer Discretionary": "Consumo discr.",
    "Consumer Staples": "Consumo básico", "Communication Services": "Comunicaciones",
    "Industrials": "Industrial", "Energy": "Energía", "Materials": "Materiales",
    "Utilities": "Utilities", "Real Estate": "Inmobiliario",
}[cite: 9]

# --- IBEX 35 (símbolos Yahoo con sufijo .MC) ---
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
}[cite: 9]

# --- Europa (blue chips líquidos) ---
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
}[cite: 9]

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
}[cite: 9]


def _wiki_tables(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
    })[cite: 9]
    with urllib.request.urlopen(req, timeout=30) as resp:
        html = resp.read().decode("utf-8", "replace")[cite: 9]
    return pd.read_html(StringIO(html))[cite: 9]


def wiki_us():
    out = {}[cite: 9]
    try:
        sp = _wiki_tables("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0][cite: 9]
        for _, r in sp.iterrows():
            sym = str(r["Symbol"]).replace(".", "-").strip()[cite: 9]
            sec = str(r.get("GICS Sector", "")).strip()[cite: 9]
            out[sym] = (str(r["Security"]).strip(), GICS_ES.get(sec, sec or "—"), "SP500")[cite: 9]
    except Exception as e:
        print(f"  ! S&P 500 desde Wikipedia falló: {e}")[cite: 9]
        return None[cite: 9]
    try:
        for t in _wiki_tables("https://en.wikipedia.org/wiki/Nasdaq-100"):[cite: 9]
            symcol = next((c for c in t.columns if str(c) in ("Ticker", "Symbol")), None)[cite: 9]
            namecol = next((c for c in t.columns if "Compan" in str(c)), None)[cite: 9]
            seccol = next((c for c in t.columns if "Sector" in str(c) or "GICS" in str(c)), None)[cite: 9]
            if symcol is None or namecol is None:
                continue[cite: 9]
            for _, r in t.iterrows():
                sym = str(r[symcol]).replace(".", "-").strip()[cite: 9]
                if not sym or sym == "nan":
                    continue[cite: 9]
                nm = str(r[namecol]).strip()[cite: 9]
                sec = str(r[seccol]).strip() if seccol else out.get(sym, ("", "—"))[1][cite: 9]
                out[sym] = (nm, GICS_ES.get(sec, sec or "—"), "NDX")[cite: 9]
            break[cite: 9]
    except Exception as e:
        print(f"  ! Nasdaq-100 desde Wikipedia falló: {e}")[cite: 9]
    return out[cite: 9]


def build_universe(use_wiki):
    us = wiki_us() if use_wiki else None[cite: 9]
    if not us:
        if use_wiki:
            print("  [AVISO] No pude leer Wikipedia; uso la lista US curada (~65).")[cite: 9]
        us = FALLBACK_US[cite: 9]
    uni, seen = [], set()[cite: 9]

    def add(sym, t, n, sec, idx):
        if sym in seen:
            return[cite: 9]
        seen.add(sym)[cite: 9]
        uni.append({"sym": sym, "t": t, "n": n, "sec": sec, "idx": idx})[cite: 9]

    for sym, (nm, sec, idx) in us.items():
        add(sym, sym, nm, sec, idx)[cite: 9]
    for sym, (nm, sec) in IBEX.items():
        add(sym, sym.replace(".MC", ""), nm, sec, "IBEX")[cite: 9]
    for sym, (nm, sec) in EUROPE.items():
        add(sym, sym.split(".")[0], nm, sec, "EU")[cite: 9]
    return uni[cite: 9]


def download(symbols):
    import yfinance as yf[cite: 9]
    allsyms = symbols + [BENCHMARK["symbol"]][cite: 9]
    closes, vols = {}, {}[cite: 9]
    for i in range(0, len(allsyms), CHUNK):
        chunk = allsyms[i:i + CHUNK][cite: 9]
        print(f"  · {i + 1}–{min(i + CHUNK, len(allsyms))} / {len(allsyms)}…")[cite: 9]
        df = None[cite: 9]
        for attempt in range(3):
            try:
                df = yf.download(chunk, period="2y", interval="1d", auto_adjust=True,
                                 group_by="ticker", threads=True, progress=False)[cite: 9]
                break[cite: 9]
            except Exception as e:
                print(f"    reintento {attempt + 1}: {e}")[cite: 9]
                time.sleep(2)[cite: 9]
        if df is None or df.empty:
            print("    lote vacío, salto.")[cite: 9]
            continue[cite: 9]
        for s in chunk:
            try:
                sub = df if len(chunk) == 1 else (df[s] if s in df.columns.get_level_values(0) else None)[cite: 9]
                if sub is None or sub.empty:
                    continue[cite: 9]
                c = sub["Close"].dropna()[cite: 9]
                v = sub["Volume"].dropna()[cite: 9]
                if c.shape[0] >= 60:
                    closes[s], vols[s] = c, v[cite: 9]
            except Exception:
                continue[cite: 9]
        time.sleep(1)[cite: 9]
    return closes, vols[cite: 9]


def synth_prices(universe):
    rng = np.random.default_rng(42)[cite: 9]
    idx = pd.bdate_range(end=dt.date.today(), periods=504)[cite: 9]
    days = len(idx)[cite: 9]
    closes, vols = {}, {}[cite: 9]
    for s in [u["sym"] for u in universe] + [BENCHMARK["symbol"]]:[cite: 9]
        drift = rng.uniform(-0.0007, 0.0015)[cite: 9]
        rets = drift + rng.normal(0, 0.013, days)[cite: 9]
        closes[s] = pd.Series(100 * np.exp(np.cumsum(rets)), index=idx)[cite: 9]
        vols[s] = pd.Series(rng.uniform(1e6, 6e6, days), index=idx)[cite: 9]
    return closes, vols[cite: 9]


def ret(s, k):
    return float(s.iloc[-1] / s.iloc[-1 - k] - 1.0) if len(s) > k else np.nan[cite: 9]


def _aligned_market_data(close, volume):
    if close is None or volume is None:
        return pd.DataFrame(columns=["close", "volume"])[cite: 9]
    frame = pd.concat(
        [close.rename("close"), volume.rename("volume")],
        axis=1,
        join="inner",
    ).dropna()[cite: 9]
    return frame[frame["volume"] > 0][cite: 9]


def calc_accumulation_days(close, volume):
    cfg = STRATEGY_CONFIG
    frame = _aligned_market_data(close, volume)[cite: 9]
    if len(frame) < cfg["flow_lookback"] + 1:
        return 0

    recent = frame.iloc[-(cfg["flow_lookback"] + 1):][cite: 9]
    returns = recent["close"].pct_change()[cite: 9]
    volume_up = recent["volume"] > recent["volume"].shift(1)[cite: 9]
    accumulation = int(((returns >= cfg["min_flow_move"]) & volume_up).sum())
    return accumulation


def calc_trend_exhaustion(close, volume):
    """
    SISTEMA UNIFICADO GENERAL DE ALERTA DE AGOTAMIENTO Y DISTRIBUCIÓN
    -----------------------------------------------------------------
    Evalúa 4 patrones institucionales sin esperar a caídas graves (-15%)
    ni saltar por pequeñas pausas de consolidación (-1%).
    """
    if close is None or volume is None or len(close) < 60:
        return 0

    c = close.dropna()
    v = volume.reindex(c.index).dropna()
    if len(c) < 60:
        return 0

    vol_ma50 = v.rolling(50).mean()
    ema21 = c.ewm(span=21, adjust=False).mean()
    hi52 = c.rolling(252, min_periods=60).max()

    rets = c.pct_change()
    rel_vol = v / vol_ma50

    # 1. CLÚSTER DE DISTRIBUCIÓN: Días de caída >= 0.8% con Volumen > 1.25x MA50 en 15 sesiones
    dist_days_15 = int(((rets <= -0.008) & (rel_vol > 1.25)).iloc[-15:].sum())

    # 2. CHURNING / ESTANCAMIENTO: Volumen > 1.5x pero precio plano (-0.5% a +0.3%) cerca de máximos (>= 92% de 52w)
    near_highs = (c / hi52) >= 0.92
    stalling_days = int(((rets >= -0.005) & (rets <= 0.003) & (rel_vol > 1.5) & near_highs).iloc[-10:].sum())

    # 3. AGOTAMIENTO CLIMÁTICO: Precio extendido > 18% sobre EMA21 con volumen alto (> 1.8x)
    ext_ema21 = (c / ema21) - 1.0
    climax_exhaustion = 1 if (ext_ema21.iloc[-1] > 0.18 and rel_vol.iloc[-1] > 1.8) else 0

    # 4. PÉRDIDA DE CARÁCTER: Cierre por debajo de la EMA21 con volumen institucional (> 1.3x)
    character_loss = 1 if (c.iloc[-1] < ema21.iloc[-1] and rel_vol.iloc[-1] > 1.3) else 0

    # --- PONDERACIÓN DEL SCORE DE SALIDA ---
    score = 0
    if dist_days_15 >= 3:
        score += 2
    elif dist_days_15 == 2:
        score += 1

    if stalling_days >= 2:
        score += 1

    if climax_exhaustion:
        score += 1

    if character_loss:
        score += 1

    # ANULACIÓN POR ABSORCIÓN: Si la última sesión es un fuerte rebote alcista (> +2.0%), la distribución se neutraliza
    if len(rets) > 0 and rets.iloc[-1] > 0.020:
        score = max(0, score - 2)

    return min(4, score)


def calc_dry_metrics(close, volume, rs_rank, above_ma50):
    """
    VOLUMEN SECO ESTÁNDAR VCP UNIFICADO
    Volumen < 55% de la media de 50 sesiones y movimiento de precio < 1.2%.
    """
    cfg = STRATEGY_CONFIG
    frame = _aligned_market_data(close, volume)[cite: 9]
    if len(frame) < 60:
        return 0, 0.0[cite: 9]

    c = frame["close"][cite: 9]
    v = frame["volume"][cite: 9]
    vol_ma50 = v.rolling(50).mean()[cite: 9]
    rel_volume = v / vol_ma50[cite: 9]
    abs_returns = c.pct_change().abs()[cite: 9]

    lookback = cfg["dry_lookback"][cite: 9]
    recent_rel = rel_volume.iloc[-lookback:][cite: 9]
    recent_move = abs_returns.iloc[-lookback:][cite: 9]
    
    dry_days = int(((recent_rel < cfg["dry_vol_threshold"]) & (recent_move < cfg["dry_move_threshold"])).sum())

    vol_ratio = float(v.iloc[-10:].mean() / v.iloc[-50:].mean())[cite: 9]
    ret10 = c.pct_change().iloc[-10:][cite: 9]
    ret50 = c.pct_change().iloc[-50:][cite: 9]
    volatility_ratio = float(ret10.std() / ret50.std()) if ret50.std() > 0 else 1.0[cite: 9]

    range10 = float(c.iloc[-10:].max() / c.iloc[-10:].min() - 1.0)[cite: 9]
    range50 = float(c.iloc[-50:].max() / c.iloc[-50:].min() - 1.0)[cite: 9]
    range_ratio = range10 / range50 if range50 > 0 else 1.0[cite: 9]

    volume_score = np.clip((1.0 - vol_ratio) / 0.60, 0.0, 1.0) * 100.0[cite: 9]
    volatility_score = np.clip((1.0 - volatility_ratio) / 0.60, 0.0, 1.0) * 100.0[cite: 9]
    range_score = np.clip((1.0 - range_ratio) / 0.75, 0.0, 1.0) * 100.0[cite: 9]
    score = 0.50 * volume_score + 0.30 * volatility_score + 0.20 * range_score[cite: 9]

    if rs_rank < 50:
        score *= 0.75[cite: 9]
    if not above_ma50:
        score *= 0.80[cite: 9]

    return dry_days, round(float(np.clip(score, 0.0, 100.0)), 1)[cite: 9]


def calc_selling_climax(close, volume):
    cfg = STRATEGY_CONFIG
    frame = _aligned_market_data(close, volume)[cite: 9]
    if len(frame) < 60:
        return False, None[cite: 9]

    c = frame["close"][cite: 9]
    v = frame["volume"][cite: 9]
    returns = c.pct_change()[cite: 9]
    vol_ma50 = v.rolling(50).mean()[cite: 9]
    rel_volume = v / vol_ma50[cite: 9]

    recent = pd.DataFrame({"ret": returns, "rv": rel_volume}).iloc[-10:][cite: 9]
    hits = recent[(recent["ret"] <= -0.04) & (recent["rv"] >= cfg["climax_vol_mult"])][cite: 9]
    if hits.empty:
        return False, None[cite: 9]
    return True, hits.index[-1].date().isoformat()[cite: 9]


def calc_vcp_score(close, volume):
    """
    VCP basado en 3 contracciones sucesivas reales (legs) en los últimos 75 días.
    Mide si la volatilidad, los rangos de precio y el volumen se reducen progresivamente[cite: 9].
    """
    frame = _aligned_market_data(close, volume)[cite: 9]
    if len(frame) < 75:
        return 0.0[cite: 9]

    c = frame["close"][cite: 9]
    v = frame["volume"][cite: 9]
    returns = c.pct_change()[cite: 9]

    leg1_ret = returns.iloc[-75:-50][cite: 9]
    leg2_ret = returns.iloc[-50:-25][cite: 9]
    leg3_ret = returns.iloc[-25:][cite: 9]

    leg1_vol = v.iloc[-75:-50].mean()[cite: 9]
    leg2_vol = v.iloc[-50:-25].mean()[cite: 9]
    leg3_vol = v.iloc[-25:].mean()[cite: 9]

    leg1_range = float(c.iloc[-75:-50].max() / c.iloc[-75:-50].min() - 1.0)[cite: 9]
    leg2_range = float(c.iloc[-50:-25].max() / c.iloc[-50:-25].min() - 1.0)[cite: 9]
    leg3_range = float(c.iloc[-25:].max() / c.iloc[-25:].min() - 1.0)[cite: 9]

    std1 = float(leg1_ret.std())[cite: 9]
    std2 = float(leg2_ret.std())[cite: 9]
    std3 = float(leg3_ret.std())[cite: 9]

    vol_contracting_steps = 0
    if std3 < std2: vol_contracting_steps += 1[cite: 9]
    if std2 < std1: vol_contracting_steps += 1[cite: 9]
    volatility_score = (vol_contracting_steps / 2.0) * 100.0[cite: 9]

    range_contracting_steps = 0
    if leg3_range < leg2_range: range_contracting_steps += 1[cite: 9]
    if leg2_range < leg1_range: range_contracting_steps += 1[cite: 9]
    range_score = (range_contracting_steps / 2.0) * 100.0[cite: 9]

    vol_contracting_steps_v = 0
    if leg3_vol < leg2_vol: vol_contracting_steps_v += 1[cite: 9]
    if leg2_vol < leg1_vol: vol_contracting_steps_v += 1[cite: 9]
    volume_score = (vol_contracting_steps_v / 2.0) * 100.0[cite: 9]

    score = 0.40 * volatility_score + 0.35 * range_score + 0.25 * volume_score[cite: 9]
    return round(float(np.clip(score, 0.0, 100.0)), 1)[cite: 9]


def classify_signal(last, ma50, ma200, rs_rank, dry_score, vcp_score,
                    early_dist, accumulation20, selling_climax):
    cfg = STRATEGY_CONFIG
    below_ma50 = not math.isnan(ma50) and last < ma50[cite: 9]
    below_ma200 = not math.isnan(ma200) and last < ma200[cite: 9]
    above_ma50 = not math.isnan(ma50) and last > ma50[cite: 9]

    if early_dist >= 3:
        return "DISTRIBUTION"[cite: 9]
    if above_ma50 and rs_rank >= cfg["min_rs_rating"] and dry_score >= 55 and vcp_score >= 50 and early_dist <= 1:
        return "VCP"[cite: 9]
    if selling_climax and dry_score >= 35 and early_dist <= 2 and not below_ma200:
        return "PULLBACK"[cite: 9]
    if accumulation20 >= 5:
        return "ACCUMULATION"[cite: 9]
    return "NEUTRAL"[cite: 9]


def compute(closes, vols, universe):
    bsym = BENCHMARK["symbol"][cite: 9]
    if bsym not in closes:
        print("  ! Sin datos del benchmark; uso el primer valor como referencia.")[cite: 9]
        bsym = next(iter(closes))[cite: 9]
    bench = closes[bsym].dropna()[cite: 9]
    common = bench.index[cite: 9]

    px = pd.DataFrame({u["sym"]: closes[u["sym"]] for u in universe if u["sym"] in closes})[cite: 9]
    px = px.reindex(common).ffill(limit=3)[cite: 9]
    valid = [c for c in px.columns if px[c].dropna().shape[0] >= MIN_HISTORY][cite: 9]
    print(f"  · {len(valid)} valores con histórico suficiente (≥{MIN_HISTORY} sesiones).")[cite: 9]

    # --- pass 1: estadísticos de momentum ---
    stat = {}[cite: 9]
    for c in valid:
        try:
            s = px[c].dropna()[cite: 9]
            r1, r3, r6, r12 = ret(s, 21), ret(s, 63), ret(s, 126), ret(s, 252)[cite: 9]
            blended = np.nansum([0.2 * r1, 0.4 * r3, 0.2 * r6, 0.2 * r12])[cite: 9]
            stat[c] = {"r1": r1, "r3": r3, "r6": r6, "r12": r12, "blended": blended}[cite: 9]
        except Exception:
            continue[cite: 9]
    st = pd.DataFrame(stat).T[cite: 9]

    def rank99(col):
        if col not in st.columns or st.empty:
            return pd.Series(1, index=st.index)[cite: 9]
        return (st[col].rank(pct=True) * 98 + 1).round().fillna(1).astype(int)[cite: 9]

    rs, rs1, rs3, rs6, rs12 = (rank99("blended"), rank99("r1"), rank99("r3"),
                               rank99("r6"), rank99("r12"))[cite: 9]

    umap = {u["sym"]: u for u in universe}[cite: 9]
    bench_out = [round(float(x), 4) for x in bench.iloc[-OUT_POINTS:].tolist()][cite: 9]
    rows = [][cite: 9]
    
    for c in valid:
        try:
            s = px[c].dropna()[cite: 9]
            u = umap[c][cite: 9]
            last = float(s.iloc[-1])[cite: 9]
            prev = float(s.iloc[-2]) if len(s) > 1 else last[cite: 9]
            chg = (last / prev - 1) * 100[cite: 9]
            ma50 = float(s.rolling(50).mean().iloc[-1]) if len(s) >= 50 else np.nan[cite: 9]
            ma200 = float(s.rolling(200).mean().iloc[-1]) if len(s) >= 200 else np.nan[cite: 9]
            ma200p = float(s.rolling(200).mean().iloc[-21]) if len(s) >= 221 else np.nan[cite: 9]
            win = s.iloc[-252:][cite: 9]
            hi, lo = float(win.max()), float(win.min())[cite: 9]
            pfh, pfl = (last / hi - 1) * 100, (last / lo - 1) * 100[cite: 9]
            
            v = vols.get(c)[cite: 9]
            rv = (float(v.iloc[-1] / v.iloc[-50:].mean())
                  if (v is not None and v.shape[0] >= 50 and v.iloc[-50:].mean() > 0) else np.nan)[cite: 9]
            
            R = int(rs.get(c, 1))[cite: 9]
            above_ma50 = (not math.isnan(ma50)) and last > ma50[cite: 9]

            early_dist = 0[cite: 9]
            accumulation20 = 0[cite: 9]
            dry_days = 0[cite: 9]
            dry_score = 0.0[cite: 9]
            vcp_score = 0.0[cite: 9]
            selling_climax = False[cite: 9]
            selling_climax_date = None[cite: 9]

            if v is not None:
                early_dist = calc_trend_exhaustion(s, v)
                accumulation20 = calc_accumulation_days(s, v)[cite: 9]
                dry_days, dry_score = calc_dry_metrics(s, v, rs_rank=R, above_ma50=above_ma50)[cite: 9]
                vcp_score = calc_vcp_score(s, v)[cite: 9]
                selling_climax, selling_climax_date = calc_selling_climax(s, v)[cite: 9]

            signal = classify_signal(
                last=last,
                ma50=ma50,
                ma200=ma200,
                rs_rank=R,
                dry_score=dry_score,
                vcp_score=vcp_score,
                early_dist=early_dist,
                accumulation20=accumulation20,
                selling_climax=selling_climax,
            )[cite: 9]

            flow_balance = accumulation20 - early_dist[cite: 9]
            institutional_score = (
                0.30 * dry_score
                + 0.25 * vcp_score
                + 0.20 * R
                + 2.5 * flow_balance
            )[cite: 9]
            institutional_score = round(float(np.clip(institutional_score, 0.0, 100.0)), 1)[cite: 9]

            c1 = (not math.isnan(ma50)) and last > ma50[cite: 9]
            c2 = (not math.isnan(ma50)) and (not math.isnan(ma200)) and ma50 > ma200[cite: 9]
            c3 = (not math.isnan(ma200)) and (not math.isnan(ma200p)) and ma200 > ma200p[cite: 9]
            c4 = (not math.isnan(pfh)) and pfh >= -25[cite: 9]
            c5 = (not math.isnan(pfl)) and pfl >= 30[cite: 9]
            c6 = R >= STRATEGY_CONFIG["min_rs_rating"][cite: 9]
            
            crit = [
                {"ok": bool(c1), "tx": "Precio sobre la MA50", "vx": f"{last:.2f} / {ma50:.2f}" if not math.isnan(ma50) else "—"},[cite: 9]
                {"ok": bool(c2), "tx": "MA50 sobre MA200", "vx": f"{ma50:.2f} / {ma200:.2f}" if not math.isnan(ma200) else "—"},[cite: 9]
                {"ok": bool(c3), "tx": "MA200 inclinada al alza", "vx": "sí" if c3 else "no"},[cite: 9]
                {"ok": bool(c4), "tx": "A < 25% del máximo de 52s", "vx": f"{pfh:.1f}%"},[cite: 9]
                {"ok": bool(c5), "tx": "A > 30% del mínimo de 52s", "vx": f"+{pfl:.0f}%"},[cite: 9]
                {"ok": bool(c6), "tx": f"RS Rating ≥ {STRATEGY_CONFIG['min_rs_rating']}", "vx": str(R)},[cite: 9]
            ]
            cnt = sum(1 for x in crit if x["ok"])[cite: 9]
            pser = px[c].ffill().bfill()[cite: 9]
            
            rows.append({
                "sym": c, "t": u["t"], "n": u["n"], "idx": u["idx"], "sec": u["sec"],[cite: 9]
                "rs": R, "rs1m": int(rs1.get(c, 1)), "rs3m": int(rs3.get(c, 1)),[cite: 9]
                "rs6m": int(rs6.get(c, 1)), "rs12m": int(rs12.get(c, 1)),[cite: 9]
                "px": round(last, 2), "chg": round(chg, 2),[cite: 9]
                "rv": round(rv, 2) if not math.isnan(rv) else 1.0,[cite: 9]
                "dryDays10": int(dry_days),[cite: 9]
                "heavyDays10": int(early_dist),        # Gotas rojas unificadas (0 a 4)[cite: 9]
                "distribution20": int(early_dist),[cite: 9]
                "accumulation20": int(accumulation20),[cite: 9]
                "flowBalance20": int(flow_balance),[cite: 9]
                "dryScore": dry_score,[cite: 9]
                "vcpScore": vcp_score,[cite: 9]
                "sellingClimax": bool(selling_climax),[cite: 9]
                "sellingClimaxDate": selling_climax_date,[cite: 9]
                "institutionalScore": institutional_score,[cite: 9]
                "signal": signal,[cite: 9]
                "pctFromHigh": round(pfh, 2), "pctFromLow": round(pfl, 2),[cite: 9]
                "trendCount": cnt, "trendOK": cnt == 6, "crit": crit,[cite: 9]
                "prices": [round(float(x), 4) for x in pser.iloc[-OUT_POINTS:].tolist()],[cite: 9]
            })
        except Exception as e:
            print(f"  ! Error procesando métricas para {c}: {e}")[cite: 9]
            continue[cite: 9]

    rows.sort(key=lambda r: (r["institutionalScore"], r["rs"]), reverse=True)[cite: 9]
    return rows, bench_out[cite: 9]


def write(rows, bench_out, path_js, path_json):
    feed = {
        "generatedAt": dt.date.today().isoformat(),[cite: 9]
        "benchmark": BENCHMARK["label"],[cite: 9]
        "benchmarkPrices": bench_out,[cite: 9]
        "universeSize": len(rows),[cite: 9]
        "rows": rows,[cite: 9]
    }
    js = "window.DEEPVIEW_FEED = " + json.dumps(feed, ensure_ascii=False, separators=(",", ":")) + ";\n"[cite: 9]
    with open(path_js, "w", encoding="utf-8") as f:
        f.write(js)[cite: 9]
    with open(path_json, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, separators=(",", ":"))[cite: 9]
    print(f"  ✓ {path_js} ({len(js) / 1e6:.2f} MB) y {path_json} escritos · {len(rows)} valores.")[cite: 9]


def main():
    ap = argparse.ArgumentParser(description="Pipeline de datos DeepView (Yahoo Finance)")[cite: 9]
    ap.add_argument("--no-us-wiki", action="store_true", help="usa lista US curada en vez de Wikipedia")[cite: 9]
    ap.add_argument("--selftest", action="store_true", help="sin red: precios sintéticos para validar")[cite: 9]
    ap.add_argument("--out", default="feed.js", help="ruta de salida del .js")[cite: 9]
    a = ap.parse_args()[cite: 9]

    print("DeepView · pipeline de datos\n" + "-" * 32)[cite: 9]
    uni = build_universe(use_wiki=not a.no_us_wiki)[cite: 9]
    n = lambda k: sum(1 for u in uni if u["idx"] == k)[cite: 9]
    print(f"Universo: {len(uni)} símbolos  (IBEX {n('IBEX')} · S&P {n('SP500')} · "
          f"Nasdaq {n('NDX')} · Europa {n('EU')})")[cite: 9]

    if a.selftest:
        print("Modo selftest: precios sintéticos (sin Yahoo).")[cite: 9]
        closes, vols = synth_prices(uni)[cite: 9]
    else:
        print("Descargando de Yahoo Finance…")[cite: 9]
        closes, vols = download([u["sym"] for u in uni])[cite: 9]

    if not closes:
        print("Sin datos. Aborto.")[cite: 9]
        sys.exit(1)[cite: 9]

    rows, bench = compute(closes, vols, uni)[cite: 9]
    write(rows, bench, a.out, a.out.replace(".js", ".json"))[cite: 9]

    led = sum(1 for r in rows if r["rs"] >= 80)[cite: 9]
    tok = sum(1 for r in rows if r["trendOK"])[cite: 9]
    print(f"Resumen: {len(rows)} valores · {led} líderes (RS≥80) · {tok} con plantilla OK.")[cite: 9]
    print("Top 5 institucional:", ", ".join(
        f"{r['t']}({r['institutionalScore']}, {r['signal']})" for r in rows[:5]
    ))[cite: 9]
    print("\nListo. Abre deepview_acciones.html.")[cite: 9]


if __name__ == "__main__":
    try:
        main()[cite: 9]
    except KeyboardInterrupt:
        print("\nInterrumpido.")[cite: 9]
