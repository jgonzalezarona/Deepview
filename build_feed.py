#!/usr/bin/env python3
"""
DeepView · Acciones — pipeline de datos (Yahoo Finance)
=======================================================
Descarga OHLCV ajustado del universo (IBEX 35, S&P 500, Nasdaq-100, Europa),
calcula el RS Rating cross-sectional (percentil vs todo el universo) y la
plantilla de tendencia, y genera feed.js (+ feed.json) que consume
deepview_acciones.html[cite: 5].
"""

import argparse, json, sys, time, math
import datetime as dt
import urllib.request
from io import StringIO
import numpy as np
import pandas as pd

OUT_POINTS = 380      # nº de sesiones que se mandan al gráfico (para MA200 completa)[cite: 5]
MIN_HISTORY = 252     # mínimo de sesiones para entrar en el ranking[cite: 5]
CHUNK = 80            # tamaño de lote en la descarga[cite: 5]
BENCHMARK = {"symbol": "ACWI", "label": "ACWI (MSCI All-Country)"}[cite: 5]

# --- CONFIGURACIÓN CENTRALIZADA DE ESTRATEGIA ---
STRATEGY_CONFIG = {
    "min_rs_rating": 70,           # RS mínimo para plantillas de tendencia / VCP
    "flow_lookback": 20,           # Ventana para días de acumulación
    "min_flow_move": 0.002,        # Movimiento mínimo porcentual para acumulación
    "dry_lookback": 10,            # Ventana de sesiones para análisis de volumen seco
    "dry_vol_threshold": 0.55,     # Umbral de volumen relativo para día seco
    "dry_move_threshold": 0.012,   # Rango de precio máximo permitido en día seco (1.2%)
    "climax_vol_mult": 1.80,       # Multiplicador de volumen para clímax vendedor
}

GICS_ES = {
    "Information Technology": "Tecnología", "Health Care": "Salud",
    "Financials": "Financiero", "Consumer Discretionary": "Consumo discr.",
    "Consumer Staples": "Consumo básico", "Communication Services": "Comunicaciones",
    "Industrials": "Industrial", "Energy": "Energía", "Materials": "Materiales",
    "Utilities": "Utilities", "Real Estate": "Inmobiliario",
}[cite: 5]

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
}[cite: 5]

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
}[cite: 5]

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
}[cite: 5]


def _wiki_tables(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
    })[cite: 5]
    with urllib.request.urlopen(req, timeout=30) as resp:
        html = resp.read().decode("utf-8", "replace")[cite: 5]
    return pd.read_html(StringIO(html))[cite: 5]


def wiki_us():
    out = {}[cite: 5]
    try:
        sp = _wiki_tables("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0][cite: 5]
        for _, r in sp.iterrows():
            sym = str(r["Symbol"]).replace(".", "-").strip()[cite: 5]
            sec = str(r.get("GICS Sector", "")).strip()[cite: 5]
            out[sym] = (str(r["Security"]).strip(), GICS_ES.get(sec, sec or "—"), "SP500")[cite: 5]
    except Exception as e:
        print(f"  ! S&P 500 desde Wikipedia falló: {e}")[cite: 5]
        return None[cite: 5]
    try:
        for t in _wiki_tables("https://en.wikipedia.org/wiki/Nasdaq-100"):[cite: 5]
            symcol = next((c for c in t.columns if str(c) in ("Ticker", "Symbol")), None)[cite: 5]
            namecol = next((c for c in t.columns if "Compan" in str(c)), None)[cite: 5]
            seccol = next((c for c in t.columns if "Sector" in str(c) or "GICS" in str(c)), None)[cite: 5]
            if symcol is None or namecol is None:
                continue[cite: 5]
            for _, r in t.iterrows():
                sym = str(r[symcol]).replace(".", "-").strip()[cite: 5]
                if not sym or sym == "nan":
                    continue[cite: 5]
                nm = str(r[namecol]).strip()[cite: 5]
                sec = str(r[seccol]).strip() if seccol else out.get(sym, ("", "—"))[1][cite: 5]
                out[sym] = (nm, GICS_ES.get(sec, sec or "—"), "NDX")[cite: 5]
            break[cite: 5]
    except Exception as e:
        print(f"  ! Nasdaq-100 desde Wikipedia falló: {e}")[cite: 5]
    return out[cite: 5]


def build_universe(use_wiki):
    us = wiki_us() if use_wiki else None[cite: 5]
    if not us:
        if use_wiki:
            print("  [AVISO] No pude leer Wikipedia; uso la lista US curada (~65).")[cite: 5]
        us = FALLBACK_US[cite: 5]
    uni, seen = [], set()[cite: 5]

    def add(sym, t, n, sec, idx):
        if sym in seen:
            return[cite: 5]
        seen.add(sym)[cite: 5]
        uni.append({"sym": sym, "t": t, "n": n, "sec": sec, "idx": idx})[cite: 5]

    for sym, (nm, sec, idx) in us.items():
        add(sym, sym, nm, sec, idx)[cite: 5]
    for sym, (nm, sec) in IBEX.items():
        add(sym, sym.replace(".MC", ""), nm, sec, "IBEX")[cite: 5]
    for sym, (nm, sec) in EUROPE.items():
        add(sym, sym.split(".")[0], nm, sec, "EU")[cite: 5]
    return uni[cite: 5]


def download(symbols):
    import yfinance as yf[cite: 5]
    allsyms = symbols + [BENCHMARK["symbol"]][cite: 5]
    closes, vols = {}, {}[cite: 5]
    for i in range(0, len(allsyms), CHUNK):
        chunk = allsyms[i:i + CHUNK][cite: 5]
        print(f"  · {i + 1}–{min(i + CHUNK, len(allsyms))} / {len(allsyms)}…")[cite: 5]
        df = None[cite: 5]
        for attempt in range(3):
            try:
                df = yf.download(chunk, period="2y", interval="1d", auto_adjust=True,
                                 group_by="ticker", threads=True, progress=False)[cite: 5]
                break[cite: 5]
            except Exception as e:
                print(f"    reintento {attempt + 1}: {e}")[cite: 5]
                time.sleep(2)[cite: 5]
        if df is None or df.empty:
            print("    lote vacío, salto.")[cite: 5]
            continue[cite: 5]
        for s in chunk:
            try:
                sub = df if len(chunk) == 1 else (df[s] if s in df.columns.get_level_values(0) else None)[cite: 5]
                if sub is None or sub.empty:
                    continue[cite: 5]
                c = sub["Close"].dropna()[cite: 5]
                v = sub["Volume"].dropna()[cite: 5]
                if c.shape[0] >= 60:
                    closes[s], vols[s] = c, v[cite: 5]
            except Exception:
                continue[cite: 5]
        time.sleep(1)[cite: 5]
    return closes, vols[cite: 5]


def synth_prices(universe):
    rng = np.random.default_rng(42)[cite: 5]
    idx = pd.bdate_range(end=dt.date.today(), periods=504)[cite: 5]
    days = len(idx)[cite: 5]
    closes, vols = {}, {}[cite: 5]
    for s in [u["sym"] for u in universe] + [BENCHMARK["symbol"]]:[cite: 5]
        drift = rng.uniform(-0.0007, 0.0015)[cite: 5]
        rets = drift + rng.normal(0, 0.013, days)[cite: 5]
        closes[s] = pd.Series(100 * np.exp(np.cumsum(rets)), index=idx)[cite: 5]
        vols[s] = pd.Series(rng.uniform(1e6, 6e6, days), index=idx)[cite: 5]
    return closes, vols[cite: 5]


def ret(s, k):
    return float(s.iloc[-1] / s.iloc[-1 - k] - 1.0) if len(s) > k else np.nan[cite: 5]


def _aligned_market_data(close, volume):
    if close is None or volume is None:
        return pd.DataFrame(columns=["close", "volume"])[cite: 5]
    frame = pd.concat(
        [close.rename("close"), volume.rename("volume")],
        axis=1,
        join="inner",
    ).dropna()[cite: 5]
    return frame[frame["volume"] > 0][cite: 5]


def calc_accumulation_days(close, volume):
    cfg = STRATEGY_CONFIG
    frame = _aligned_market_data(close, volume)[cite: 5]
    if len(frame) < cfg["flow_lookback"] + 1:
        return 0

    recent = frame.iloc[-(cfg["flow_lookback"] + 1):][cite: 5]
    returns = recent["close"].pct_change()[cite: 5]
    volume_up = recent["volume"] > recent["volume"].shift(1)[cite: 5]
    accumulation = int(((returns >= cfg["min_flow_move"]) & volume_up).sum())
    return accumulation


def calc_trend_exhaustion(close, volume):
    """
    SISTEMA UNIFICADO GENERAL DE ALERTA DE AGOTAMIENTO Y DISTRIBUCIÓN
    Evalúa 4 patrones institucionales sin esperar a caídas graves (-15%)
    ni saltar por pequeñas pausas de consolidación (-1%).
    """
    if close is None or volume is None or len(close) < 60:
        return 0

    c = close
    v = volume
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
    if rets.iloc[-1] > 0.020:
        score = max(0, score - 2)

    return min(4, score)


def calc_dry_metrics(close, volume, rs_rank, above_ma50):
    """
    VOLUMEN SECO ESTÁNDAR VCP UNIFICADO
    Volumen < 55% de la media de 50 sesiones y movimiento de precio < 1.2%.
    """
    cfg = STRATEGY_CONFIG
    frame = _aligned_market_data(close, volume)[cite: 5]
    if len(frame) < 60:
        return 0, 0.0[cite: 5]

    c = frame["close"][cite: 5]
    v = frame["volume"][cite: 5]
    vol_ma50 = v.rolling(50).mean()[cite: 5]
    rel_volume = v / vol_ma50[cite: 5]
    abs_returns = c.pct_change().abs()[cite: 5]

    lookback = cfg["dry_lookback"]
    recent_rel = rel_volume.iloc[-lookback:][cite: 5]
    recent_move = abs_returns.iloc[-lookback:][cite: 5]
    
    dry_days = int(((recent_rel < cfg["dry_vol_threshold"]) & (recent_move < cfg["dry_move_threshold"])).sum())

    vol_ratio = float(v.iloc[-10:].mean() / v.iloc[-50:].mean())[cite: 5]
    ret10 = c.pct_change().iloc[-10:][cite: 5]
    ret50 = c.pct_change().iloc[-50:][cite: 5]
    volatility_ratio = float(ret10.std() / ret50.std()) if ret50.std() > 0 else 1.0[cite: 5]

    range10 = float(c.iloc[-10:].max() / c.iloc[-10:].min() - 1.0)[cite: 5]
    range50 = float(c.iloc[-50:].max() / c.iloc[-50:].min() - 1.0)[cite: 5]
    range_ratio = range10 / range50 if range50 > 0 else 1.0[cite: 5]

    volume_score = np.clip((1.0 - vol_ratio) / 0.60, 0.0, 1.0) * 100.0[cite: 5]
    volatility_score = np.clip((1.0 - volatility_ratio) / 0.60, 0.0, 1.0) * 100.0[cite: 5]
    range_score = np.clip((1.0 - range_ratio) / 0.75, 0.0, 1.0) * 100.0[cite: 5]
    score = 0.50 * volume_score + 0.30 * volatility_score + 0.20 * range_score[cite: 5]

    if rs_rank < 50:
        score *= 0.75[cite: 5]
    if not above_ma50:
        score *= 0.80[cite: 5]

    return dry_days, round(float(np.clip(score, 0.0, 100.0)), 1)[cite: 5]


def calc_selling_climax(close, volume):
    cfg = STRATEGY_CONFIG
    frame = _aligned_market_data(close, volume)[cite: 5]
    if len(frame) < 60:
        return False, None[cite: 5]

    c = frame["close"][cite: 5]
    v = frame["volume"][cite: 5]
    returns = c.pct_change()[cite: 5]
    vol_ma50 = v.rolling(50).mean()[cite: 5]
    rel_volume = v / vol_ma50[cite: 5]

    recent = pd.DataFrame({"ret": returns, "rv": rel_volume}).iloc[-10:][cite: 5]
    hits = recent[(recent["ret"] <= -0.04) & (recent["rv"] >= cfg["climax_vol_mult"])][cite: 5]
    if hits.empty:
        return False, None[cite: 5]
    return True, hits.index[-1].date().isoformat()[cite: 5]


def calc_vcp_score(close, volume):
    """
    VCP basado en 3 contracciones sucesivas reales (legs) en los últimos 75 días.
    Mide si la volatilidad, los rangos de precio y el volumen se reducen progresivamente[cite: 5].
    """
    frame = _aligned_market_data(close, volume)[cite: 5]
    if len(frame) < 75:
        return 0.0[cite: 5]

    c = frame["close"][cite: 5]
    v = frame["volume"][cite: 5]
    returns = c.pct_change()[cite: 5]

    leg1_ret = returns.iloc[-75:-50][cite: 5]
    leg2_ret = returns.iloc[-50:-25][cite: 5]
    leg3_ret = returns.iloc[-25:][cite: 5]

    leg1_vol = v.iloc[-75:-50].mean()[cite: 5]
    leg2_vol = v.iloc[-50:-25].mean()[cite: 5]
    leg3_vol = v.iloc[-25:].mean()[cite: 5]

    leg1_range = float(c.iloc[-75:-50].max() / c.iloc[-75:-50].min() - 1.0)[cite: 5]
    leg2_range = float(c.iloc[-50:-25].max() / c.iloc[-50:-25].min() - 1.0)[cite: 5]
    leg3_range = float(c.iloc[-25:].max() / c.iloc[-25:].min() - 1.0)[cite: 5]

    std1 = float(leg1_ret.std())[cite: 5]
    std2 = float(leg2_ret.std())[cite: 5]
    std3 = float(leg3_ret.std())[cite: 5]

    vol_contracting_steps = 0
    if std3 < std2: vol_contracting_steps += 1[cite: 5]
    if std2 < std1: vol_contracting_steps += 1[cite: 5]
    volatility_score = (vol_contracting_steps / 2.0) * 100.0[cite: 5]

    range_contracting_steps = 0
    if leg3_range < leg2_range: range_contracting_steps += 1[cite: 5]
    if leg2_range < leg1_range: range_contracting_steps += 1[cite: 5]
    range_score = (range_contracting_steps / 2.0) * 100.0[cite: 5]

    vol_contracting_steps_v = 0
    if leg3_vol < leg2_vol: vol_contracting_steps_v += 1[cite: 5]
    if leg2_vol < leg1_vol: vol_contracting_steps_v += 1[cite: 5]
    volume_score = (vol_contracting_steps_v / 2.0) * 100.0[cite: 5]

    score = 0.40 * volatility_score + 0.35 * range_score + 0.25 * volume_score[cite: 5]
    return round(float(np.clip(score, 0.0, 100.0)), 1)[cite: 5]


def classify_signal(last, ma50, ma200, rs_rank, dry_score, vcp_score,
                    early_dist, accumulation20, selling_climax):
    cfg = STRATEGY_CONFIG
    below_ma50 = not math.isnan(ma50) and last < ma50[cite: 5]
    below_ma200 = not math.isnan(ma200) and last < ma200[cite: 5]
    above_ma50 = not math.isnan(ma50) and last > ma50[cite: 5]

    if early_dist >= 3:
        return "DISTRIBUTION"[cite: 5]
    if above_ma50 and rs_rank >= cfg["min_rs_rating"] and dry_score >= 55 and vcp_score >= 50 and early_dist <= 1:
        return "VCP"[cite: 5]
    if selling_climax and dry_score >= 35 and early_dist <= 2 and not below_ma200:
        return "PULLBACK"[cite: 5]
    if accumulation20 >= 5:
        return "ACCUMULATION"[cite: 5]
    return "NEUTRAL"[cite: 5]


def compute(closes, vols, universe):
    bsym = BENCHMARK["symbol"][cite: 5]
    if bsym not in closes:
        print("  ! Sin datos del benchmark; uso el primer valor como referencia.")[cite: 5]
        bsym = next(iter(closes))[cite: 5]
    bench = closes[bsym].dropna()[cite: 5]
    common = bench.index[cite: 5]

    px = pd.DataFrame({u["sym"]: closes[u["sym"]] for u in universe if u["sym"] in closes})[cite: 5]
    px = px.reindex(common).ffill(limit=3)[cite: 5]
    valid = [c for c in px.columns if px[c].dropna().shape[0] >= MIN_HISTORY][cite: 5]
    print(f"  · {len(valid)} valores con histórico suficiente (≥{MIN_HISTORY} sesiones).")[cite: 5]

    # --- pass 1: estadísticos de momentum ---
    stat = {}[cite: 5]
    for c in valid:
        try:
            s = px[c].dropna()[cite: 5]
            r1, r3, r6, r12 = ret(s, 21), ret(s, 63), ret(s, 126), ret(s, 252)[cite: 5]
            blended = np.nansum([0.2 * r1, 0.4 * r3, 0.2 * r6, 0.2 * r12])[cite: 5]
            stat[c] = {"r1": r1, "r3": r3, "r6": r6, "r12": r12, "blended": blended}[cite: 5]
        except Exception:
            continue[cite: 5]
    st = pd.DataFrame(stat).T[cite: 5]

    def rank99(col):
        if col not in st.columns or st.empty:
            return pd.Series(1, index=st.index)[cite: 5]
        return (st[col].rank(pct=True) * 98 + 1).round().fillna(1).astype(int)[cite: 5]

    rs, rs1, rs3, rs6, rs12 = (rank99("blended"), rank99("r1"), rank99("r3"),
                               rank99("r6"), rank99("r12"))[cite: 5]

    umap = {u["sym"]: u for u in universe}[cite: 5]
    bench_out = [round(float(x), 4) for x in bench.iloc[-OUT_POINTS:].tolist()][cite: 5]
    rows = [][cite: 5]
    
    for c in valid:
        try:
            s = px[c].dropna()[cite: 5]
            u = umap[c][cite: 5]
            last = float(s.iloc[-1])[cite: 5]
            prev = float(s.iloc[-2]) if len(s) > 1 else last[cite: 5]
            chg = (last / prev - 1) * 100[cite: 5]
            ma50 = float(s.rolling(50).mean().iloc[-1]) if len(s) >= 50 else np.nan[cite: 5]
            ma200 = float(s.rolling(200).mean().iloc[-1]) if len(s) >= 200 else np.nan[cite: 5]
            ma200p = float(s.rolling(200).mean().iloc[-21]) if len(s) >= 221 else np.nan[cite: 5]
            win = s.iloc[-252:][cite: 5]
            hi, lo = float(win.max()), float(win.min())[cite: 5]
            pfh, pfl = (last / hi - 1) * 100, (last / lo - 1) * 100[cite: 5]
            
            v = vols.get(c)[cite: 5]
            rv = (float(v.iloc[-1] / v.iloc[-50:].mean())
                  if (v is not None and v.shape[0] >= 50 and v.iloc[-50:].mean() > 0) else np.nan)[cite: 5]
            
            R = int(rs.get(c, 1))[cite: 5]
            above_ma50 = (not math.isnan(ma50)) and last > ma50[cite: 5]

            early_dist = 0[cite: 5]
            accumulation20 = 0[cite: 5]
            dry_days = 0[cite: 5]
            dry_score = 0.0[cite: 5]
            vcp_score = 0.0[cite: 5]
            selling_climax = False[cite: 5]
            selling_climax_date = None[cite: 5]

            if v is not None:
                early_dist = calc_trend_exhaustion(s, v)
                accumulation20 = calc_accumulation_days(s, v)[cite: 5]
                dry_days, dry_score = calc_dry_metrics(s, v, rs_rank=R, above_ma50=above_ma50)[cite: 5]
                vcp_score = calc_vcp_score(s, v)[cite: 5]
                selling_climax, selling_climax_date = calc_selling_climax(s, v)[cite: 5]

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
            )[cite: 5]

            flow_balance = accumulation20 - early_dist[cite: 5]
            institutional_score = (
                0.30 * dry_score
                + 0.25 * vcp_score
                + 0.20 * R
                + 2.5 * flow_balance
            )[cite: 5]
            institutional_score = round(float(np.clip(institutional_score, 0.0, 100.0)), 1)[cite: 5]

            c1 = (not math.isnan(ma50)) and last > ma50[cite: 5]
            c2 = (not math.isnan(ma50)) and (not math.isnan(ma200)) and ma50 > ma200[cite: 5]
            c3 = (not math.isnan(ma200)) and (not math.isnan(ma200p)) and ma200 > ma200p[cite: 5]
            c4 = (not math.isnan(pfh)) and pfh >= -25[cite: 5]
            c5 = (not math.isnan(pfl)) and pfl >= 30[cite: 5]
            c6 = R >= STRATEGY_CONFIG["min_rs_rating"][cite: 5]
            
            crit = [
                {"ok": bool(c1), "tx": "Precio sobre la MA50", "vx": f"{last:.2f} / {ma50:.2f}" if not math.isnan(ma50) else "—"},[cite: 5]
                {"ok": bool(c2), "tx": "MA50 sobre MA200", "vx": f"{ma50:.2f} / {ma200:.2f}" if not math.isnan(ma200) else "—"},[cite: 5]
                {"ok": bool(c3), "tx": "MA200 inclinada al alza", "vx": "sí" if c3 else "no"},[cite: 5]
                {"ok": bool(c4), "tx": "A < 25% del máximo de 52s", "vx": f"{pfh:.1f}%"},[cite: 5]
                {"ok": bool(c5), "tx": "A > 30% del mínimo de 52s", "vx": f"+{pfl:.0f}%"},[cite: 5]
                {"ok": bool(c6), "tx": f"RS Rating ≥ {STRATEGY_CONFIG['min_rs_rating']}", "vx": str(R)},[cite: 5]
            ]
            cnt = sum(1 for x in crit if x["ok"])[cite: 5]
            pser = px[c].ffill().bfill()[cite: 5]
            
            rows.append({
                "sym": c, "t": u["t"], "n": u["n"], "idx": u["idx"], "sec": u["sec"],[cite: 5]
                "rs": R, "rs1m": int(rs1.get(c, 1)), "rs3m": int(rs3.get(c, 1)),[cite: 5]
                "rs6m": int(rs6.get(c, 1)), "rs12m": int(rs12.get(c, 1)),[cite: 5]
                "px": round(last, 2), "chg": round(chg, 2),[cite: 5]
                "rv": round(rv, 2) if not math.isnan(rv) else 1.0,[cite: 5]
                "dryDays10": int(dry_days),[cite: 5]
                "heavyDays10": int(early_dist),        # Gotas rojas unificadas (0 a 4)[cite: 5]
                "distribution20": int(early_dist),[cite: 5]
                "accumulation20": int(accumulation20),[cite: 5]
                "flowBalance20": int(flow_balance),[cite: 5]
                "dryScore": dry_score,[cite: 5]
                "vcpScore": vcp_score,[cite: 5]
                "sellingClimax": bool(selling_climax),[cite: 5]
                "sellingClimaxDate": selling_climax_date,[cite: 5]
                "institutionalScore": institutional_score,[cite: 5]
                "signal": signal,[cite: 5]
                "pctFromHigh": round(pfh, 2), "pctFromLow": round(pfl, 2),[cite: 5]
                "trendCount": cnt, "trendOK": cnt == 6, "crit": crit,[cite: 5]
                "prices": [round(float(x), 4) for x in pser.iloc[-OUT_POINTS:].tolist()],[cite: 5]
            })
        except Exception as e:
            print(f"  ! Error procesando métricas para {c}: {e}")[cite: 5]
            continue[cite: 5]

    rows.sort(key=lambda r: (r["institutionalScore"], r["rs"]), reverse=True)[cite: 5]
    return rows, bench_out[cite: 5]


def write(rows, bench_out, path_js, path_json):
    feed = {
        "generatedAt": dt.date.today().isoformat(),[cite: 5]
        "benchmark": BENCHMARK["label"],[cite: 5]
        "benchmarkPrices": bench_out,[cite: 5]
        "universeSize": len(rows),[cite: 5]
        "rows": rows,[cite: 5]
    }
    js = "window.DEEPVIEW_FEED = " + json.dumps(feed, ensure_ascii=False, separators=(",", ":")) + ";\n"[cite: 5]
    with open(path_js, "w", encoding="utf-8") as f:
        f.write(js)[cite: 5]
    with open(path_json, "w", encoding="utf-8") as f:
        json.dump(feed, f, ensure_ascii=False, separators=(",", ":"))[cite: 5]
    print(f"  ✓ {path_js} ({len(js) / 1e6:.2f} MB) y {path_json} escritos · {len(rows)} valores.")[cite: 5]


def main():
    ap = argparse.ArgumentParser(description="Pipeline de datos DeepView (Yahoo Finance)")[cite: 5]
    ap.add_argument("--no-us-wiki", action="store_true", help="usa lista US curada en vez de Wikipedia")[cite: 5]
    ap.add_argument("--selftest", action="store_true", help="sin red: precios sintéticos para validar")[cite: 5]
    ap.add_argument("--out", default="feed.js", help="ruta de salida del .js")[cite: 5]
    a = ap.parse_args()[cite: 5]

    print("DeepView · pipeline de datos\n" + "-" * 32)[cite: 5]
    uni = build_universe(use_wiki=not a.no_us_wiki)[cite: 5]
    n = lambda k: sum(1 for u in uni if u["idx"] == k)[cite: 5]
    print(f"Universo: {len(uni)} símbolos  (IBEX {n('IBEX')} · S&P {n('SP500')} · "
          f"Nasdaq {n('NDX')} · Europa {n('EU')})")[cite: 5]

    if a.selftest:
        print("Modo selftest: precios sintéticos (sin Yahoo).")[cite: 5]
        closes, vols = synth_prices(uni)[cite: 5]
    else:
        print("Descargando de Yahoo Finance…")[cite: 5]
        closes, vols = download([u["sym"] for u in uni])[cite: 5]

    if not closes:
        print("Sin datos. Aborto.")[cite: 5]
        sys.exit(1)[cite: 5]

    rows, bench = compute(closes, vols, uni)[cite: 5]
    write(rows, bench, a.out, a.out.replace(".js", ".json"))[cite: 5]

    led = sum(1 for r in rows if r["rs"] >= 80)[cite: 5]
    tok = sum(1 for r in rows if r["trendOK"])[cite: 5]
    print(f"Resumen: {len(rows)} valores · {led} líderes (RS≥80) · {tok} con plantilla OK.")[cite: 5]
    print("Top 5 institucional:", ", ".join(
        f"{r['t']}({r['institutionalScore']}, {r['signal']})" for r in rows[:5]
    ))[cite: 5]
    print("\nListo. Abre deepview_acciones.html.")[cite: 5]


if __name__ == "__main__":
    try:
        main()[cite: 5]
    except KeyboardInterrupt:
        print("\nInterrumpido.")[cite: 5]
