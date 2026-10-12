#!/usr/bin/env python3
"""
Recolector de marcadores deportivos (en vivo e históricos) -> base de datos.

Fuente: endpoints públicos NO oficiales de ESPN (sin API key).
Almacenamiento: SQLite por defecto, o Postgres/MySQL con DATABASE_URL.

Uso:
  python collector.py live                      # una pasada: hoy +/- 1 día
  python collector.py loop --interval 300       # corre para siempre (cada 5 min)
  python collector.py backfill --from 2023-08-01 --to 2024-06-30
  python collector.py export --out datos.parquet
"""
import argparse
import datetime as dt
import json
import logging
import os
import time

import requests
from sqlalchemy import (Column, DateTime, Integer, MetaData, String, Table,
                        Text, create_engine, delete, select, text)

BASE = "https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard"

# (deporte, liga, nombre). Para añadir ligas, agrega una línea.
# Rugby en ESPN usa IDs numéricos de liga: verifica los IDs que quieras
# abriendo la URL del scoreboard en el navegador antes de activarlos.
LEAGUES = [
    ("soccer", "eng.1", "Premier League"),
    ("soccer", "esp.1", "LaLiga"),
    ("soccer", "ger.1", "Bundesliga"),
    ("soccer", "ita.1", "Serie A"),
    ("soccer", "fra.1", "Ligue 1"),
    ("soccer", "col.1", "Liga BetPlay"),
    ("soccer", "uefa.champions", "Champions League"),
    ("soccer", "uefa.europa", "Europa League"),
    ("soccer", "conmebol.libertadores", "Libertadores"),
    ("soccer", "fifa.world", "Mundial FIFA"),
    ("basketball", "nba", "NBA"),
    ("football", "nfl", "NFL"),
    ("football", "college-football", "NCAA Football"),
    ("baseball", "mlb", "MLB"),
    ("hockey", "nhl", "NHL"),
    ("tennis", "atp", "ATP"),
    ("tennis", "wta", "WTA"),
    # ("rugby", "<ID_LIGA>", "Nombre"),
]

DB_URL = os.environ.get("DATABASE_URL", "sqlite:///sports.db")
# Algunos hosts entregan "postgres://", SQLAlchemy exige "postgresql://"
if DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DB_URL, pool_pre_ping=True)
meta = MetaData()
events = Table(
    "events", meta,
    Column("sport", String(30), primary_key=True),
    Column("league", String(60), primary_key=True),
    Column("event_id", String(60), primary_key=True),
    Column("league_name", String(80)),
    Column("start_time", String(40)),
    Column("state", String(10)),        # pre | in | post
    Column("status", String(80)),
    Column("home", String(120)),
    Column("away", String(120)),
    Column("home_score", String(20)),
    Column("away_score", String(20)),
    Column("raw", Text),                # JSON original, por si luego necesitas más campos
    Column("fetched_at", DateTime),
)
meta.create_all(engine)

log = logging.getLogger("collector")
session = requests.Session()
session.headers["User-Agent"] = "sports-research-collector/1.0"


def get_json(url, params, retries=4):
    for i in range(retries):
        try:
            r = session.get(url, params=params, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (404, 400):
                return None
            log.warning("HTTP %s en %s", r.status_code, r.url)
        except requests.RequestException as e:
            log.warning("Error de red: %s", e)
        time.sleep(2 ** i)
    return None


def _name(c):
    if "team" in c:
        return c["team"].get("displayName")
    if "athlete" in c:
        return c["athlete"].get("displayName")
    return c.get("displayName")


def parse_competitions(ev):
    """Devuelve lista de competiciones; tenis las trae dentro de 'groupings'."""
    comps = list(ev.get("competitions", []))
    for g in ev.get("groupings", []):
        comps.extend(g.get("competitions", []))
    return comps


def parse_events(data, sport, league, league_name):
    rows = []
    now = dt.datetime.utcnow()
    for ev in (data or {}).get("events", []):
        for comp in parse_competitions(ev):
            cs = comp.get("competitors", [])
            if len(cs) < 2:
                continue
            home = next((c for c in cs if c.get("homeAway") == "home"), cs[0])
            away = next((c for c in cs if c.get("homeAway") == "away"), cs[1])
            st = comp.get("status", {}).get("type", {}) or ev.get("status", {}).get("type", {})
            rows.append(dict(
                sport=sport, league=league,
                event_id=str(comp.get("id") or ev.get("id")),
                league_name=league_name,
                start_time=comp.get("date") or comp.get("startDate") or ev.get("date"),
                state=st.get("state"), status=st.get("description"),
                home=_name(home), away=_name(away),
                home_score=home.get("score"), away_score=away.get("score"),
                raw=json.dumps(comp, ensure_ascii=False),
                fetched_at=now,
            ))
    return rows


def upsert(rows):
    if not rows:
        return
    with engine.begin() as conn:
        for r in rows:  # delete+insert: portable entre SQLite/Postgres/MySQL
            conn.execute(delete(events).where(
                events.c.sport == r["sport"], events.c.league == r["league"],
                events.c.event_id == r["event_id"]))
        conn.execute(events.insert(), rows)


def fetch_range(sport, league, name, d1, d2):
    dates = d1.strftime("%Y%m%d") if d1 == d2 else f"{d1:%Y%m%d}-{d2:%Y%m%d}"
    data = get_json(BASE.format(sport=sport, league=league),
                    {"dates": dates, "limit": 1000})
    rows = parse_events(data, sport, league, name)
    upsert(rows)
    return len(rows)


def run_live():
    today = dt.date.today()
    total = 0
    for sport, league, name in LEAGUES:
        n = fetch_range(sport, league, name, today - dt.timedelta(days=1),
                        today + dt.timedelta(days=1))
        total += n
        log.info("%-22s %3d eventos", name, n)
        time.sleep(0.5)  # cortesía con el servidor
    log.info("Total: %d eventos", total)


def run_backfill(start, end, chunk=14):
    for sport, league, name in LEAGUES:
        d = start
        while d <= end:
            e = min(d + dt.timedelta(days=chunk - 1), end)
            n = fetch_range(sport, league, name, d, e)
            log.info("%s %s..%s -> %d", name, d, e, n)
            d = e + dt.timedelta(days=1)
            time.sleep(0.5)


def run_export(out):
    import pandas as pd
    df = pd.read_sql(select(events), engine)
    df.drop(columns=["raw"]).to_parquet(out, index=False) if out.endswith(".parquet") \
        else df.drop(columns=["raw"]).to_csv(out, index=False)
    log.info("Exportadas %d filas a %s", len(df), out)


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("live")
    lp = sub.add_parser("loop"); lp.add_argument("--interval", type=int, default=300)
    bp = sub.add_parser("backfill")
    bp.add_argument("--from", dest="d1", required=True)
    bp.add_argument("--to", dest="d2", default=dt.date.today().isoformat())
    ep = sub.add_parser("export"); ep.add_argument("--out", default="datos.parquet")
    a = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    if a.cmd == "live":
        run_live()
    elif a.cmd == "loop":
        while True:
            try:
                run_live()
            except Exception:
                log.exception("Fallo en la pasada; reintento en el próximo ciclo")
            time.sleep(a.interval)
    elif a.cmd == "backfill":
        run_backfill(dt.date.fromisoformat(a.d1), dt.date.fromisoformat(a.d2))
    elif a.cmd == "export":
        run_export(a.out)


if __name__ == "__main__":
    main()
