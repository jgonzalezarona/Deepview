#!/usr/bin/env python3
"""
DeepView · Fundamentales (Yahoo Finance)
========================================
Lee feed.json (ya generado por build_feed.py), elige los valores "interesantes"
(líderes / candidatos técnicos) y descarga SOLO sus fundamentales -> fundamentals.js
(+ fundamentals.json), que el dashboard fusiona por símbolo.

Se separa del pipeline de precios a propósito: los precios se refrescan a diario
(rápido, en lotes) y los fundamentales cuando quieras (lento, una petición por
empresa). Cobertura buena en US grande; con huecos en IBEX/Europa.

Uso:
  python build_fundamentals.py            # solo candidatos del feed.json
  python build_fundamentals.py --all      # todos los del feed (lento)
  python build_fundamentals.py --selftest # sin red: fundamentales sintéticos
"""

import argparse, json, sys, time, math
import datetime as dt

FEED_JSON = "feed.json"
FUND_RS_MIN = 70   # umbral de RS para considerar un valor "interesante"


def is_candidate(r):
    if r.get("rs", 0) >= FUND_RS_MIN:
        return True
    if r.get("trendOK"):
        return True
    # rezagada girando: muy lejos de máximos pero girando al alza
    if (r.get("pctFromHigh", 0) <= -25 and r.get("pctFromLow", 0) >= 8
            and r.get("rs1m", 0) > r.get("rs6m", 0)):
        return True
    return False


def _info(sym):
    import yfinance as yf
    tk = yf.Ticker(sym)
    for attempt in range(2):
        try:
            return (tk.get_info() if hasattr(tk, "get_info") else tk.info) or {}
        except Exception:
            try:
                return tk.info or {}
            except Exception:
                time.sleep(1)
    return {}


def fields_from_info(info):
    def g(*keys):
        for k in keys:
            v = info.get(k)
            if v is not None and not (isinstance(v, float) and math.isnan(v)):
                return v
        return None
    pct = lambda x: round(x * 100, 1) if isinstance(x, (int, float)) else None
    num = lambda x, d=2: round(x, d) if isinstance(x, (int, float)) else None
    margin = g("profitMargins")
    teps = g("trailingEps")
    profit = (margin > 0) if isinstance(margin, (int, float)) else (
        (teps > 0) if isinstance(teps, (int, float)) else None)
    mcap = g("marketCap")
    return {
        "epsQ": pct(g("earningsQuarterlyGrowth")),   # crecimiento BPA último trimestre
        "epsA": pct(g("earningsGrowth")),            # crecimiento BPA anual
        "sales": pct(g("revenueGrowth")),            # crecimiento ventas
        "margin": pct(margin),                       # margen neto
        "roe": pct(g("returnOnEquity")),             # ROE
        "pe": num(g("trailingPE"), 1),               # PER
        "fpe": num(g("forwardPE"), 1),               # PER adelantado
        "ps": num(g("priceToSalesTrailing12Months"), 2),  # P/Ventas
        "mcap": (round(mcap / 1e9, 2) if isinstance(mcap, (int, float)) else None),  # miles de millones
        "profit": profit,
    }


def synth_fund(sym):
    import random
    rng = random.Random(sum(ord(ch) for ch in sym) * 7 + len(sym))
    return {
        "epsQ": round(rng.uniform(-30, 80), 1), "epsA": round(rng.uniform(-20, 60), 1),
        "sales": round(rng.uniform(-15, 45), 1), "margin": round(rng.uniform(-5, 40), 1),
        "roe": round(rng.uniform(-10, 45), 1), "pe": round(rng.uniform(8, 60), 1),
        "fpe": round(rng.uniform(7, 45), 1), "ps": round(rng.uniform(0.5, 25), 2),
        "mcap": round(rng.uniform(2, 3000), 2), "profit": rng.random() > 0.25,
    }


def main():
    ap = argparse.ArgumentParser(description="Fundamentales DeepView (Yahoo Finance)")
    ap.add_argument("--all", action="store_true", help="todos los valores del feed (lento)")
    ap.add_argument("--selftest", action="store_true", help="sin red: fundamentales sintéticos")
    ap.add_argument("--feed", default=FEED_JSON, help="ruta del feed.json (def. feed.json)")
    ap.add_argument("--out", default="fundamentals.js", help="salida (def. fundamentals.js)")
    a = ap.parse_args()

    print("DeepView · fundamentales\n" + "-" * 26)
    try:
        feed = json.load(open(a.feed, encoding="utf-8"))
    except Exception as e:
        print(f"[ERROR] No puedo leer {a.feed}: {e}")
        print("        Ejecuta antes actualizar_COMPLETO.bat (genera feed.json).")
        sys.exit(1)

    rows = feed.get("rows", [])
    if not rows or "sym" not in rows[0]:
        print("[ERROR] El feed.json no trae 'sym'.")
        print("        Regenera feed.js con la version nueva de build_feed.py (vuelve a lanzar COMPLETO).")
        sys.exit(1)

    cands = rows if a.all else [r for r in rows if is_candidate(r)]
    extra = "" if a.all else f" (interesantes: RS>={FUND_RS_MIN}, plantilla OK o rezagada girando)"
    print(f"Fundamentales para {len(cands)} de {len(rows)} valores{extra}.")
    if not cands:
        print("No hay candidatos; nada que descargar.")
        sys.exit(0)

    out = {}
    if a.selftest:
        print("Modo selftest: fundamentales sinteticos (sin Yahoo).")
        for r in cands:
            out[r["sym"]] = synth_fund(r["sym"])
    else:
        print("Descargando de Yahoo (una peticion por empresa, paciencia)…")
        for i, r in enumerate(cands, 1):
            if i % 25 == 0 or i == len(cands):
                print(f"  · {i}/{len(cands)}…")
            out[r["sym"]] = fields_from_info(_info(r["sym"]))
            time.sleep(0.3)

    payload = {"generatedAt": dt.date.today().isoformat(), "count": len(out), "rows": out}
    js = "window.DEEPVIEW_FUND = " + json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + ";\n"
    with open(a.out, "w", encoding="utf-8") as f:
        f.write(js)
    with open(a.out.replace(".js", ".json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    got = sum(1 for v in out.values() if any(x is not None for x in v.values()))
    print(f"✓ {a.out} escrito · {len(out)} valores ({got} con datos reales).")
    print("Recarga el dashboard (F5).")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrumpido.")
