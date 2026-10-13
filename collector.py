#!/usr/bin/env python3
"""
Recolector deportivo MULTI-FUENTE -> base de datos -> interfaz web.

Fuentes (sin clave salvo que se indique):
  espn          ESPN (2 hosts)                      todos los deportes
  mlb           MLB Stats API (oficial)             MLB
  nhl           NHL web API (oficial)               NHL
  nba           NBA CDN (oficial)                   NBA
  footballdata  football-data.org  [FOOTBALL_DATA_TOKEN, gratis]   Big-5, Champions, Mundial
  fixturedl     FixtureDownload                     Big-5, Champions, Europa, NFL, NBA, NHL, rugby
  openliga      OpenLigaDB                          Bundesliga
  nflverse      nflverse (GitHub)                   NFL
  openfootball  openfootball (GitHub, con retraso)  Big-5
  sackmann      Jeff Sackmann (GitHub, con retraso) ATP / WTA (solo resultados)

Cada partido se FUSIONA entre fuentes aunque los nombres difieran
("Wolves" = "Wolverhampton Wanderers FC"). Si una fuente cae, las demás
siguen alimentando la base de datos. El estado de cada fuente se guarda
y se muestra al pie de la interfaz.

Por cada equipo/jugador se conservan SOLO sus últimos 10 partidos jugados y
el próximo por jugar (más lo que esté en vivo).

Comandos:
  python collector.py sync            # pasada ligera (ayer..pasado mañana)
  python collector.py sync --deep     # pasada profunda (120 días atrás, 30 adelante)
  python collector.py site --out site # genera site/index.html + site/data.json
  python collector.py check           # diagnóstico: cada fuente x cada liga
  python collector.py export --out datos.csv
  python collector.py loop            # sync + site cada 10 min (uso local)
"""
import argparse
import csv
import datetime as dt
import hashlib
import io
import json
import logging
import os
import re
import time
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from difflib import SequenceMatcher

import requests
from sqlalchemy import (Column, DateTime, Integer, MetaData, String, Table,
                        Text, create_engine, delete, func, inspect, select,
                        text, tuple_)

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

# (deporte, liga, nombre). Para añadir ligas de ESPN, agrega una línea.
# Rugby en ESPN usa IDs numéricos de liga: verifica el ID abriendo la URL del
# scoreboard en el navegador antes de activarlo.
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
    ("rugby", "premiership-rugby", "Premiership Rugby"),
    ("rugby", "6-nations", "Six Nations"),
    ("rugby", "super-rugby-pacific", "Super Rugby Pacific"),
    ("rugby", "nations-championship", "Nations Championship"),
]
# Ligas que ESPN no cubre con este identificador (solo las sirven otras fuentes)
NO_ESPN = {"premiership-rugby", "6-nations", "super-rugby-pacific", "nations-championship"}

KEEP_PAST = 10     # últimos partidos jugados por equipo/jugador
KEEP_NEXT = 1      # próximos partidos por jugar por equipo/jugador
LIVE_BACK, LIVE_FWD = 2, 2      # ventana de la pasada ligera (días)
DEEP_BACK = int(os.environ.get("DEEP_BACK", 120))
DEEP_FWD = int(os.environ.get("DEEP_FWD", 30))
DEEP_CHUNK = 7     # días por consulta de ESPN en la pasada profunda
SLEEP = 0.3        # pausa de cortesía entre consultas de ESPN (s)
ESPN_DAY_BACK = int(os.environ.get("ESPN_DAY_BACK", 60))   # modo por días: días hacia atrás
DEEP_EVERY_HOURS = int(os.environ.get("DEEP_EVERY_HOURS", 26))  # cada cuánto se fuerza la pasada profunda
RUN_BUDGET = 28 * 60   # segundos máximos de recolección por pasada (el job de GitHub dura 40 min)
NOT_PLAYED = ("postponed", "cancel", "suspended", "abandon", "ppd", "aplaz")
TOL_DAYS = {"tennis": 16.0}          # tolerancia de fecha al cruzar fuentes
DEFAULT_TOL = 1.3
STRONG, WEAK = 0.85, 0.25            # umbrales de parecido de nombres

DB_URL = (os.environ.get("DATABASE_URL") or "").strip() or "sqlite:///sports.db"
# Cualquier variante de Postgres (postgres://, postgresql://, postgresql+psycopg://...)
# se fuerza al controlador psycopg2, que es el que instala requirements.txt.
DB_URL = re.sub(r"^postgres(ql)?(\+\w+)?://", "postgresql+psycopg2://", DB_URL)

engine = create_engine(DB_URL, pool_pre_ping=True)
meta = MetaData()
events = Table(
    "events", meta,
    Column("sport", String(30), primary_key=True),
    Column("league", String(60), primary_key=True),
    Column("event_id", String(80), primary_key=True),
    Column("league_name", String(80)),
    Column("start_time", String(40)),
    Column("state", String(10)),        # pre | in | post
    Column("status", String(80)),
    Column("home", String(120)),
    Column("away", String(120)),
    Column("home_score", String(20)),
    Column("away_score", String(20)),
    Column("winner", String(5)),        # home | away | NULL
    Column("detail", String(160)),      # tenis: parciales por set
    Column("sources", String(120)),     # fuentes que reportaron el partido
    Column("src_ids", Text),            # {"ids": {fuente: id}, "flip": [fuentes]}
    Column("name_prio", Integer),       # prioridad de la fuente de los nombres
    Column("raw", Text),                # JSON de la fuente principal
    Column("fetched_at", DateTime),
)
aliases_t = Table(
    "aliases", meta,
    Column("sport", String(30), primary_key=True),
    Column("alias", String(160), primary_key=True),
    Column("canonical", String(160)),
)
health_t = Table(
    "source_health", meta,
    Column("source", String(30), primary_key=True),
    Column("label", String(60)),
    Column("status", String(10)),       # ok | partial | fail | off
    Column("last_ok", DateTime),
    Column("last_run", DateTime),
    Column("detail", String(300)),
)

log = logging.getLogger("collector")
session = requests.Session()
session.headers["User-Agent"] = "Mozilla/5.0 (compatible; sports-collector/3.0)"


def utcnow():
    return dt.datetime.now(dt.timezone.utc)


def ensure_schema():
    """Crea las tablas y añade columnas nuevas si venías de una versión anterior."""
    meta.create_all(engine)
    cols = inspect(engine).get_columns("events")
    existing = {c["name"] for c in cols}
    with engine.begin() as conn:
        if engine.dialect.name == "postgresql":
            for c in cols:
                if c["name"] == "event_id" and (getattr(c["type"], "length", None) or 80) < 80:
                    conn.execute(text("ALTER TABLE events ALTER COLUMN event_id TYPE VARCHAR(80)"))
        for col in events.columns:
            if col.name not in existing:
                ddl = col.type.compile(dialect=engine.dialect)
                conn.execute(text(f"ALTER TABLE events ADD COLUMN {col.name} {ddl}"))
                log.info("Columna añadida: %s", col.name)


# ───────────────────────────── red ─────────────────────────────
class SourceError(Exception):
    """Una fuente no pudo entregar datos (HTTP != 200, formato inesperado...)."""


def http_get(url, params=None, headers=None, retries=3, as_text=False):
    """Devuelve (status_http, contenido|None). status None = sin conexión con la fuente."""
    status, conn_fail = None, 0
    for i in range(retries):
        try:
            r = session.get(url, params=params, headers=headers, timeout=(10, 25))
            status = r.status_code
            if status == 200:
                return status, (r.text if as_text else r.json())
            log.warning("HTTP %s en %s", status, r.url)
            if status in (400, 401, 403, 404):
                return status, None
            if status == 429:
                time.sleep(15 * (i + 1))
                continue
        except (requests.ConnectionError, requests.Timeout) as e:
            log.warning("Sin conexión con %s: %s", url, type(e).__name__)
            status, conn_fail = None, conn_fail + 1
            if conn_fail >= 2:      # la fuente está caída: no insistir
                return None, None
        except (requests.RequestException, ValueError) as e:
            log.warning("Error de red/JSON en %s: %s", url, e)
        if i < retries - 1:
            time.sleep(2 ** i)
    return status, None


def need(status, data, what):
    if status != 200 or data is None:
        raise SourceError(f"{what}: HTTP {status}")
    return data


# ───────────────────── nombres y comparación ───────────────────
STOP = {"fc", "cf", "afc", "ac", "sc", "cd", "ud", "rc", "as", "ss", "us", "club",
        "the", "de", "calcio", "fk", "sk", "if", "ca", "cs", "sv", "bk", "1", "cp"}


def strip_acc(s):
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))


def norm(s, sport="soccer"):
    s = strip_acc(s).lower().replace("&", " and ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    toks = s.split()
    if sport != "tennis":
        kept = [t for t in toks if t not in STOP]
        toks = kept or toks
    return " ".join(toks)


def sim(a, b):
    """Parecido 0..1 entre dos nombres normalizados (prefijos: 'man' ~ 'manchester')."""
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0
    ta, tb = a.split(), b.split()
    short, long_ = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    i, ok = 0, True
    for t in short:
        while i < len(long_) and not (long_[i] == t or (len(t) >= 3 and long_[i].startswith(t))):
            i += 1
        if i == len(long_):
            ok = False
            break
        i += 1
    if ok:
        return 0.95
    return SequenceMatcher(None, a, b).ratio()


def tennis_parts(n):
    t = n.split()
    return (t[-1] if t else ""), (t[0][0] if t else ""), len(t)


def tsim(a, b):
    """Parecido entre jugadores: mismo apellido + misma inicial."""
    if a == b:
        return 1.0
    (sa, ia, na), (sb, ib, nb) = tennis_parts(a), tennis_parts(b)
    if not sa or not sb:
        return 0.0
    if sa == sb or (len(sa) > 3 and (sa in b.split() or sb in a.split())):
        return 0.92 if (ia == ib or na == 1 or nb == 1) else 0.3
    return 0.0


ALIASES = {}      # (sport, nombre normalizado) -> nombre canónico (aprendido)
KNOWN = {}        # (sport, nombre normalizado) -> (nombre, prioridad)
NEW_ALIASES = []


def canon(sport, name, prio=9):
    """Nombre canónico: alias aprendidos y nombres ya conocidos de una fuente igual o mejor."""
    for _ in range(3):
        c = None
        n = norm(name, sport)
        if (sport, n) in ALIASES:
            c = ALIASES[(sport, n)]
        elif (sport, n) in KNOWN and KNOWN[(sport, n)][1] <= prio:
            c = KNOWN[(sport, n)][0]
        if not c or c == name:
            break
        name = c
    return name


def learn(sport, alias_name, canonical):
    na = norm(alias_name, sport)
    if not na or na == norm(canonical, sport) or ALIASES.get((sport, na)) == canonical:
        return
    ALIASES[(sport, na)] = canonical
    NEW_ALIASES.append(dict(sport=sport, alias=na, canonical=canonical))


def know(sport, name, prio):
    k = (sport, norm(name, sport))
    if k not in KNOWN or prio < KNOWN[k][1]:
        KNOWN[k] = (name, prio)


def parse_iso(s):
    if not s:
        return None
    try:
        d = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        try:
            d = dt.datetime.strptime(str(s)[:10], "%Y-%m-%d")
        except ValueError:
            return None
    return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)


def day_diff(a, b):
    da, db = parse_iso(a), parse_iso(b)
    if not da or not db:
        return None
    return abs((da - db).total_seconds()) / 86400.0


def pair_score(inc, ex, sport):
    f = (lambda a, b: tsim(a, b)) if sport == "tennis" else sim
    nh, na = norm(inc["home"], sport), norm(inc["away"], sport)
    eh, ea = norm(ex["home"], sport), norm(ex["away"], sport)
    d1 = (f(nh, eh), f(na, ea))
    d2 = (f(nh, ea), f(na, eh))
    parts, swapped = (d2, True) if sum(d2) > sum(d1) else (d1, False)
    if sport == "tennis":
        ok = min(parts) >= STRONG
    else:
        ok = (max(parts) >= STRONG and min(parts) >= WEAK) or sum(parts) >= 1.5
    return ok, sum(parts), swapped, parts


def flip_detail(d):
    if not d:
        return d
    return re.sub(r"(\d+)-(\d+)", lambda m: f"{m.group(2)}-{m.group(1)}", d)


def flip(r):
    r = dict(r)
    r["home"], r["away"] = r["away"], r["home"]
    r["home_score"], r["away_score"] = r.get("away_score"), r.get("home_score")
    r["winner"] = {"home": "away", "away": "home"}.get(r.get("winner"))
    r["detail"] = flip_detail(r.get("detail"))
    return r


# ───────────────────────── proveedores ─────────────────────────
def mkrow(**kw):
    base = dict(src_id=None, start_time=None, state="pre", status=None, home=None, away=None,
                home_score=None, away_score=None, winner=None, detail=None, raw=None)
    base.update(kw)
    return base


def in_window(iso, start, end, pad=0):
    d = (iso or "")[:10]
    return (start - dt.timedelta(pad)).isoformat() <= d <= (end + dt.timedelta(pad)).isoformat()


def win_from_scores(state, hs, as_):
    if state != "post":
        return None
    try:
        h, a = float(hs), float(as_)
    except (TypeError, ValueError):
        return None
    return "home" if h > a else "away" if a > h else None


def local_to_utc(date_s, time_s, tz):
    """'2026-10-12' + '20:00' en la zona tz -> ISO UTC."""
    try:
        h, m = (time_s or "12:00").split(":")[:2]
        d = dt.datetime.strptime(date_s, "%Y-%m-%d").replace(hour=int(h), minute=int(m))
        if ZoneInfo:
            d = d.replace(tzinfo=ZoneInfo(tz)).astimezone(dt.timezone.utc)
        else:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.strftime("%Y-%m-%dT%H:%MZ")
    except Exception:
        return f"{date_s}T12:00Z"


class Provider:
    name, label, prio = "", "", 9
    lagging = False      # fuente con retraso: sus "por jugar" ya vencidos se ignoran
    backup = False       # solo se consulta en pasada profunda o si las demás fallan
    leagues = None       # None = todas las de LEAGUES
    min_interval = 0.0
    _last = 0.0

    def enabled(self):
        return True

    def why_off(self):
        return ""

    def supports(self, league):
        return self.leagues is None or league in self.leagues

    def throttle(self):
        wait = self.min_interval - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)
        self._last = time.monotonic()

    def fetch(self, sport, league, name, start, end, deep):
        raise NotImplementedError


# ---- ESPN ------------------------------------------------------
class ESPN(Provider):
    name, label, prio = "espn", "ESPN", 1

    def supports(self, league):
        return league not in NO_ESPN
    HOSTS = ["https://site.api.espn.com", "https://site.web.api.espn.com"]

    def _query(self, sport, league, dates):
        status = None
        for host in self.HOSTS:
            url = f"{host}/apis/site/v2/sports/{sport}/{league}/scoreboard"
            status, data = http_get(url, {"dates": dates, "limit": 1000}, retries=2)
            if status is None:
                continue          # sin conexión con este host: probar el siguiente
            if not (data or {}).get("events"):
                s2, d2 = http_get(url, {"dates": dates}, retries=2)
                if (d2 or {}).get("events") or status != 200:
                    status, data = s2, d2
            if status == 200:
                return status, data
        return status, None

    @staticmethod
    def _name(c):
        if "team" in c:
            return c["team"].get("displayName")
        if "athlete" in c:
            return c["athlete"].get("displayName")
        return c.get("displayName")

    @staticmethod
    def _score(c):
        s = c.get("score")
        if isinstance(s, dict):
            s = s.get("displayValue", s.get("value"))
        return None if s is None or s == "" else str(s)

    @staticmethod
    def _sets(h, a):
        parts, hw, aw = [], 0, 0
        for x, y in zip(h.get("linescores") or [], a.get("linescores") or []):
            hv, av = x.get("value"), y.get("value")
            if hv is None or av is None:
                continue
            hv, av = int(hv), int(av)
            parts.append(f"{hv}-{av}")
            hw += hv > av
            aw += av > hv
        return " ".join(parts), hw, aw

    def parse(self, data, sport):
        rows = []
        for ev in (data or {}).get("events") or []:
            comps = list(ev.get("competitions", []))
            for g in ev.get("groupings", []):  # tenis
                comps.extend(g.get("competitions", []))
            for comp in comps:
                cs = comp.get("competitors", [])
                if len(cs) < 2:
                    continue
                home = next((c for c in cs if c.get("homeAway") == "home"), cs[0])
                away = next((c for c in cs if c.get("homeAway") == "away"), cs[1])
                hn, an = self._name(home), self._name(away)
                if not hn or not an or "TBD" in (hn, an):
                    continue
                st = (comp.get("status", {}).get("type")
                      or ev.get("status", {}).get("type") or {})
                state = st.get("state")
                hs, as_, detail = self._score(home), self._score(away), None
                if sport == "tennis":
                    detail, hset, aset = self._sets(home, away)
                    if state == "post" and hs is None:
                        hs, as_ = str(hset), str(aset)
                winner = ("home" if home.get("winner") is True
                          else "away" if away.get("winner") is True else None)
                rows.append(mkrow(
                    src_id=str(comp.get("id") or ev.get("id")),
                    start_time=comp.get("date") or comp.get("startDate") or ev.get("date"),
                    state=state, status=st.get("description"), home=hn, away=an,
                    home_score=hs, away_score=as_, winner=winner, detail=detail or None,
                    raw=comp))
        return rows

    day_mode = set()     # deportes en los que ESPN rechaza rangos de fechas (HTTP 400)

    def _fetch_days(self, sport, league, start, end, deep):
        """Una consulta por día (en paralelo). ESPN rechaza los rangos en estos deportes."""
        if deep:
            start = max(start, end - dt.timedelta(days=ESPN_DAY_BACK + 30))
        days = [start + dt.timedelta(n) for n in range((end - start).days + 1)]

        def one(day):
            time.sleep(0.1)
            return self._query(sport, league, day.strftime("%Y%m%d"))
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(one, days))
        rows, ok, last = [], 0, None
        for status, data in results:
            if status == 200:
                ok += 1
                rows += self.parse(data, sport)
            else:
                last = status
        if ok == 0:
            raise SourceError(f"ESPN {league}: HTTP {last}")
        return rows

    def fetch(self, sport, league, name, start, end, deep):
        if sport in self.day_mode:
            return self._fetch_days(sport, league, start, end, deep)
        chunk = DEEP_CHUNK if deep else (end - start).days + 1
        rows, ok, fail, last = [], 0, 0, None
        d = start
        while d <= end:
            e = min(d + dt.timedelta(days=chunk - 1), end)
            dates = d.strftime("%Y%m%d") if d == e else f"{d:%Y%m%d}-{e:%Y%m%d}"
            status, data = self._query(sport, league, dates)
            if status == 200:
                ok += 1
                rows += self.parse(data, sport)
            else:
                fail += 1
                last = status
                if ok == 0 and status == 400 and d != e:
                    # ¿rechaza solo los rangos? probar un día suelto
                    s1, d1 = self._query(sport, league, start.strftime("%Y%m%d"))
                    if s1 == 200:
                        log.warning("ESPN rechaza rangos de fechas en '%s': paso a consultas por día", sport)
                        self.day_mode.add(sport)
                        return self._fetch_days(sport, league, start, end, deep)
                if ok == 0 and fail >= 2:     # la fuente no responde: no gastar el resto
                    raise SourceError(f"ESPN {league}: HTTP {last}")
            d = e + dt.timedelta(days=1)
            time.sleep(SLEEP)
        if ok == 0 and fail:
            raise SourceError(f"ESPN {league}: HTTP {last}")
        return rows


# ---- football-data.org (token gratuito) -----------------------
class FootballData(Provider):
    name, label, prio = "footballdata", "football-data.org", 2
    backup = True
    min_interval = 6.5          # plan gratis: 10 llamadas por minuto
    leagues = {"eng.1": "PL", "esp.1": "PD", "ger.1": "BL1", "ita.1": "SA",
               "fra.1": "FL1", "uefa.champions": "CL", "fifa.world": "WC"}

    def enabled(self):
        return bool(os.environ.get("FOOTBALL_DATA_TOKEN"))

    def why_off(self):
        return "sin configurar (falta el secreto FOOTBALL_DATA_TOKEN)"

    def fetch(self, sport, league, name, start, end, deep):
        self.throttle()
        code = self.leagues[league]
        status, data = http_get(f"https://api.football-data.org/v4/competitions/{code}/matches",
                                headers={"X-Auth-Token": os.environ["FOOTBALL_DATA_TOKEN"]})
        data = need(status, data, f"football-data {code}")
        rows = []
        for m in data.get("matches", []):
            if not in_window(m.get("utcDate"), start, end):
                continue
            s = m.get("status")
            state = ("post" if s in ("FINISHED", "AWARDED") else
                     "in" if s in ("IN_PLAY", "PAUSED", "LIVE") else
                     "post" if s in ("POSTPONED", "SUSPENDED", "CANCELLED") else "pre")
            ft = (m.get("score") or {}).get("fullTime") or {}
            hs, as_ = ft.get("home"), ft.get("away")
            w = (m.get("score") or {}).get("winner")
            winner = "home" if w == "HOME_TEAM" else "away" if w == "AWAY_TEAM" else None
            hn = (m.get("homeTeam") or {}).get("name")
            an = (m.get("awayTeam") or {}).get("name")
            if not hn or not an:
                continue
            rows.append(mkrow(
                src_id=str(m["id"]), start_time=m.get("utcDate"), state=state,
                status=s.title() if s else None, home=hn, away=an,
                home_score=None if hs is None else str(hs),
                away_score=None if as_ is None else str(as_), winner=winner, raw=m))
        return rows


# ---- MLB Stats API (oficial) ----------------------------------
class MLB(Provider):
    name, label, prio = "mlb", "MLB Stats API", 2
    leagues = {"mlb"}

    def fetch(self, sport, league, name, start, end, deep):
        status, data = http_get("https://statsapi.mlb.com/api/v1/schedule",
                                {"sportId": 1, "startDate": start.isoformat(),
                                 "endDate": end.isoformat()})
        data = need(status, data, "MLB schedule")
        rows = []
        for day in data.get("dates", []):
            for g in day.get("games", []):
                stt = g.get("status") or {}
                detailed = (stt.get("detailedState") or "")
                abstract = stt.get("abstractGameState")
                t = g.get("teams") or {}
                hn = ((t.get("home") or {}).get("team") or {}).get("name")
                an = ((t.get("away") or {}).get("team") or {}).get("name")
                if not hn or not an:
                    continue
                if any(b in detailed.lower() for b in ("postponed", "cancel", "suspended")):
                    state = "post"
                else:
                    state = {"Final": "post", "Live": "in"}.get(abstract, "pre")
                hs, as_ = (t.get("home") or {}).get("score"), (t.get("away") or {}).get("score")
                winner = ("home" if (t.get("home") or {}).get("isWinner") is True else
                          "away" if (t.get("away") or {}).get("isWinner") is True else None)
                rows.append(mkrow(
                    src_id=str(g["gamePk"]), start_time=g.get("gameDate"), state=state,
                    status=detailed or abstract, home=hn, away=an,
                    home_score=None if hs is None else str(hs),
                    away_score=None if as_ is None else str(as_), winner=winner, raw=g))
        return rows


# ---- NHL web API (oficial) ------------------------------------
class NHL(Provider):
    name, label, prio = "nhl", "NHL API", 2
    leagues = {"nhl"}

    @staticmethod
    def _team(t):
        place = (t.get("placeName") or {}).get("default")
        common = (t.get("commonName") or {}).get("default")
        return f"{place} {common}".strip() if place and common else (common or t.get("abbrev"))

    def fetch(self, sport, league, name, start, end, deep):
        rows, seen, d, ok = [], set(), start, 0
        while d <= end:
            status, data = http_get(f"https://api-web.nhle.com/v1/schedule/{d.isoformat()}")
            if status != 200 and ok == 0 and d >= start + dt.timedelta(days=14):
                break  # sin respuesta en las 3 primeras semanas: la fuente está caída
            if status == 200 and data:
                ok += 1
                for day in data.get("gameWeek", []):
                    for g in day.get("games", []):
                        if g["id"] in seen or not in_window(g.get("startTimeUTC"), start, end):
                            continue
                        seen.add(g["id"])
                        gs = g.get("gameState")
                        if g.get("gameScheduleState") in ("PPD", "CNCL", "SUSP"):
                            state, status_txt = "post", "Postponed"
                        else:
                            state = ("post" if gs in ("FINAL", "OFF") else
                                     "in" if gs in ("LIVE", "CRIT") else "pre")
                            status_txt = {"post": "Final", "in": "In Progress"}.get(state, "Scheduled")
                        h, a = g.get("homeTeam") or {}, g.get("awayTeam") or {}
                        hn, an = self._team(h), self._team(a)
                        if not hn or not an:
                            continue
                        hs, as_ = h.get("score"), a.get("score")
                        rows.append(mkrow(
                            src_id=str(g["id"]), start_time=g.get("startTimeUTC"), state=state,
                            status=status_txt, home=hn, away=an,
                            home_score=None if hs is None else str(hs),
                            away_score=None if as_ is None else str(as_),
                            winner=win_from_scores(state, hs, as_), raw=g))
            d += dt.timedelta(days=7)
        if ok == 0:
            raise SourceError("NHL schedule: sin respuesta")
        return rows


# ---- NBA CDN (oficial) ----------------------------------------
class NBA(Provider):
    name, label, prio = "nba", "NBA CDN", 2
    backup = True        # devuelve HTTP 403 desde los servidores de GitHub
    leagues = {"nba"}
    URL = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"

    def fetch(self, sport, league, name, start, end, deep):
        status, data = http_get(self.URL, headers={"Referer": "https://www.nba.com/",
                                                   "Origin": "https://www.nba.com"})
        data = need(status, data, "NBA schedule")
        rows = []
        for gd in (data.get("leagueSchedule") or {}).get("gameDates", []):
            for g in gd.get("games", []):
                when = g.get("gameDateTimeUTC") or g.get("gameDateTimeEst")
                if not in_window(when, start, end):
                    continue
                h, a = g.get("homeTeam") or {}, g.get("awayTeam") or {}
                hn = f"{h.get('teamCity', '')} {h.get('teamName', '')}".strip()
                an = f"{a.get('teamCity', '')} {a.get('teamName', '')}".strip()
                if not hn or not an:
                    continue
                st = g.get("gameStatus")
                txt = g.get("gameStatusText") or ""
                if any(b in txt.lower() for b in ("ppd", "postponed", "cancel")):
                    state = "post"
                else:
                    state = {3: "post", 2: "in"}.get(st, "pre")
                hs = h.get("score") if state != "pre" else None
                as_ = a.get("score") if state != "pre" else None
                rows.append(mkrow(
                    src_id=str(g["gameId"]), start_time=when, state=state, status=txt.strip(),
                    home=hn, away=an, home_score=None if hs is None else str(hs),
                    away_score=None if as_ is None else str(as_),
                    winner=win_from_scores(state, hs, as_), raw=g))
        return rows


# ---- nflverse (GitHub) ----------------------------------------
NFL_TEAMS = {
    "ARI": "Arizona Cardinals", "ATL": "Atlanta Falcons", "BAL": "Baltimore Ravens",
    "BUF": "Buffalo Bills", "CAR": "Carolina Panthers", "CHI": "Chicago Bears",
    "CIN": "Cincinnati Bengals", "CLE": "Cleveland Browns", "DAL": "Dallas Cowboys",
    "DEN": "Denver Broncos", "DET": "Detroit Lions", "GB": "Green Bay Packers",
    "HOU": "Houston Texans", "IND": "Indianapolis Colts", "JAX": "Jacksonville Jaguars",
    "KC": "Kansas City Chiefs", "LAC": "Los Angeles Chargers", "LA": "Los Angeles Rams",
    "LAR": "Los Angeles Rams", "LV": "Las Vegas Raiders", "MIA": "Miami Dolphins",
    "MIN": "Minnesota Vikings", "NE": "New England Patriots", "NO": "New Orleans Saints",
    "NYG": "New York Giants", "NYJ": "New York Jets", "PHI": "Philadelphia Eagles",
    "PIT": "Pittsburgh Steelers", "SEA": "Seattle Seahawks", "SF": "San Francisco 49ers",
    "TB": "Tampa Bay Buccaneers", "TEN": "Tennessee Titans", "WAS": "Washington Commanders",
}


class NFLverse(Provider):
    name, label, prio = "nflverse", "nflverse", 3
    lagging = True
    backup = True
    leagues = {"nfl"}
    URL = "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"

    def fetch(self, sport, league, name, start, end, deep):
        status, body = http_get(self.URL, as_text=True)
        body = need(status, body, "nflverse games.csv")
        rows = []
        for r in csv.DictReader(io.StringIO(body)):
            day = r.get("gameday") or ""
            if not in_window(day, start, end):
                continue
            hn, an = NFL_TEAMS.get(r.get("home_team")), NFL_TEAMS.get(r.get("away_team"))
            if not hn or not an:
                continue
            hs, as_ = (r.get("home_score") or None), (r.get("away_score") or None)
            state = "post" if hs is not None and as_ is not None else "pre"
            rows.append(mkrow(
                src_id=r["game_id"], start_time=local_to_utc(day, r.get("gametime"), "America/New_York"),
                state=state, status="Final" if state == "post" else "Scheduled", home=hn, away=an,
                home_score=hs, away_score=as_, winner=win_from_scores(state, hs, as_),
                raw={k: r.get(k) for k in ("game_id", "season", "week", "gameday", "gametime",
                                           "home_team", "away_team", "home_score", "away_score")}))
        return rows


# ---- openfootball (GitHub; actualiza con retraso) -------------
class OpenFootball(Provider):
    name, label, prio = "openfootball", "openfootball", 4
    lagging = True
    backup = True
    leagues = {"eng.1": ("en.1", "Europe/London"), "esp.1": ("es.1", "Europe/Madrid"),
               "ger.1": ("de.1", "Europe/Berlin"), "ita.1": ("it.1", "Europe/Rome"),
               "fra.1": ("fr.1", "Europe/Paris")}

    @staticmethod
    def season_of(d):
        y = d.year if d.month >= 7 else d.year - 1
        return f"{y}-{(y + 1) % 100:02d}"

    def fetch(self, sport, league, name, start, end, deep):
        code, tz = self.leagues[league]
        rows, got = [], False
        for season in sorted({self.season_of(start), self.season_of(end)}):
            status, data = http_get(
                f"https://raw.githubusercontent.com/openfootball/football.json/master/{season}/{code}.json")
            if status == 404:
                continue
            data = need(status, data, f"openfootball {season}/{code}")
            got = True
            for m in data.get("matches", []):
                when = local_to_utc(m.get("date", ""), m.get("time"), tz)
                if not in_window(when, start, end):
                    continue
                sc = m.get("score")
                ft = sc.get("ft") if isinstance(sc, dict) else None
                if not (isinstance(ft, list) and len(ft) == 2):
                    ft = None
                state = "post" if ft else "pre"
                hs, as_ = (str(ft[0]), str(ft[1])) if ft else (None, None)
                rows.append(mkrow(
                    src_id=f"{season}:{code}:{m.get('date')}:{m.get('team1')}",
                    start_time=when, state=state, status="Final" if ft else "Scheduled",
                    home=m.get("team1"), away=m.get("team2"), home_score=hs, away_score=as_,
                    winner=win_from_scores(state, hs, as_), raw=m))
        if not got:
            raise SourceError(f"openfootball {code}: sin archivo para la temporada")
        return rows


# ---- Jeff Sackmann (GitHub; solo resultados, con retraso) -----
class Sackmann(Provider):
    name, label, prio = "sackmann", "Sackmann (tenis)", 4
    lagging = True
    backup = True
    leagues = {"atp": "tennis_atp/master/atp_matches_{y}.csv",
               "wta": "tennis_wta/master/wta_matches_{y}.csv"}

    def fetch(self, sport, league, name, start, end, deep):
        rows, got = [], False
        for y in sorted({start.year, end.year}):
            url = "https://raw.githubusercontent.com/JeffSackmann/" + self.leagues[league].format(y=y)
            status, body = http_get(url, as_text=True)
            if status == 404:
                continue
            body = need(status, body, f"Sackmann {league} {y}")
            got = True
            for r in csv.DictReader(io.StringIO(body)):
                td = r.get("tourney_date") or ""
                if len(td) != 8:
                    continue
                iso = f"{td[:4]}-{td[4:6]}-{td[6:]}"
                if not in_window(iso, start - dt.timedelta(20), end):
                    continue
                w, l, sc = r.get("winner_name"), r.get("loser_name"), r.get("score") or ""
                if not w or not l:
                    continue
                home, away = sorted([w, l])
                sets = re.findall(r"(\d+)-(\d+)", sc)
                hw = sum(1 for a, b in sets if int(a) > int(b))
                aw = sum(1 for a, b in sets if int(b) > int(a))
                w_is_home = (home == w)
                hs, as_ = (hw, aw) if w_is_home else (aw, hw)
                detail = sc if w_is_home else flip_detail(sc)
                rows.append(mkrow(
                    src_id=f"{r.get('tourney_id')}-{r.get('match_num')}", start_time=f"{iso}T12:00Z",
                    state="post", status="Final", home=home, away=away,
                    home_score=str(hs), away_score=str(as_), winner="home" if w_is_home else "away",
                    detail=detail or None, raw={k: r.get(k) for k in (
                        "tourney_id", "tourney_name", "round", "winner_name", "loser_name", "score")}))
        if not got:
            log.info("Sackmann %s: aún no hay archivo del año en curso (se publica con retraso)", league)
        return rows


# ---- FixtureDownload (sin clave; temporada completa en 1 archivo) --
class FixtureDownload(Provider):
    name, label, prio = "fixturedl", "FixtureDownload", 3
    # liga -> (slug, ¿el año del slug es el año de calendario?)
    leagues = {"eng.1": ("epl", False), "esp.1": ("la-liga", False), "ita.1": ("serie-a", False),
               "ger.1": ("bundesliga", False), "fra.1": ("ligue-1", False),
               "uefa.champions": ("champions-league", False), "uefa.europa": ("europa-league", False),
               "nfl": ("nfl", False), "nba": ("nba", False), "nhl": ("nhl", False),
               "premiership-rugby": ("premiership-rugby", False), "6-nations": ("6-nations", True),
               "super-rugby-pacific": ("super-rugby-pacific", True),
               "nations-championship": ("nations-championship", True)}
    PLACEHOLDER = re.compile(r"\b(tbc|tbd|to be|winner|loser|runner)\b|/", re.I)

    def fetch(self, sport, league, name, start, end, deep):
        slug, calendar = self.leagues[league]
        if sport == "rugby":   # pocos partidos al año: mirar más atrás para reunir 10
            start = start - dt.timedelta(days=420)
        season = lambda d: d.year if (calendar or d.month >= 7) else d.year - 1
        rows, got, last = [], False, None
        for y in sorted({season(start), season(end)}):
            status, data = http_get(f"https://fixturedownload.com/feed/json/{slug}-{y}")
            if status == 404:
                last = f"{slug}-{y}: HTTP 404"
                continue
            data = need(status, data, f"fixturedownload {slug}-{y}")
            if not isinstance(data, list):
                raise SourceError(f"fixturedownload {slug}-{y}: formato inesperado")
            got = True
            for m in data:
                when = (m.get("DateUtc") or "").replace(" ", "T")
                if not in_window(when, start, end):
                    continue
                hn, an = m.get("HomeTeam") or "", m.get("AwayTeam") or ""
                if len(hn) < 3 or len(an) < 3 or self.PLACEHOLDER.search(f"{hn} {an}"):
                    continue
                hs, as_ = m.get("HomeTeamScore"), m.get("AwayTeamScore")
                played = hs is not None and as_ is not None
                state = "post" if played else "pre"
                sid = f"{y}:" + hashlib.md5(f"{when}|{hn}|{an}".encode()).hexdigest()[:12]
                rows.append(mkrow(
                    src_id=sid, start_time=when, state=state,
                    status="Final" if played else "Scheduled", home=hn, away=an,
                    home_score=str(hs) if played else None, away_score=str(as_) if played else None,
                    winner=win_from_scores(state, hs, as_), raw=m))
        if not got:
            raise SourceError(last or f"fixturedownload {slug}: sin archivo")
        return rows


# ---- OpenLigaDB (sin clave; Bundesliga) -----------------------
class OpenLigaDB(Provider):
    name, label, prio = "openliga", "OpenLigaDB", 3
    leagues = {"ger.1": "bl1"}

    def fetch(self, sport, league, name, start, end, deep):
        code = self.leagues[league]
        season = lambda d: d.year if d.month >= 7 else d.year - 1
        rows, got, now = [], False, utcnow()
        for y in sorted({season(start), season(end)}):
            status, data = http_get(f"https://api.openligadb.de/getmatchdata/{code}/{y}")
            if status == 404:
                continue
            data = need(status, data, f"OpenLigaDB {code}/{y}")
            if not isinstance(data, list):
                raise SourceError(f"OpenLigaDB {code}/{y}: formato inesperado")
            got = True
            for m in data:
                when = m.get("matchDateTimeUTC")
                if not in_window(when, start, end):
                    continue
                hn = (m.get("team1") or {}).get("teamName")
                an = (m.get("team2") or {}).get("teamName")
                if not hn or not an:
                    continue
                results = m.get("matchResults") or []
                final = next((r for r in results if r.get("resultTypeID") == 2), None) \
                    or (results[-1] if results else None)
                hs, as_ = (final.get("pointsTeam1"), final.get("pointsTeam2")) if final else (None, None)
                st = parse_iso(when)
                if m.get("matchIsFinished"):
                    state, status_txt = "post", "Final"
                elif st and st <= now < st + dt.timedelta(hours=3):
                    goals = m.get("goals") or []
                    hs, as_ = (goals[-1].get("scoreTeam1"), goals[-1].get("scoreTeam2")) if goals else (0, 0)
                    state, status_txt = "in", "In Progress"
                else:
                    state, status_txt, hs, as_ = "pre", "Scheduled", None, None
                rows.append(mkrow(
                    src_id=str(m.get("matchID")), start_time=when, state=state, status=status_txt,
                    home=hn, away=an, home_score=None if hs is None else str(hs),
                    away_score=None if as_ is None else str(as_),
                    winner=win_from_scores(state, hs, as_), raw=m))
        if not got:
            raise SourceError(f"OpenLigaDB {code}: sin datos de la temporada")
        return rows


PROVIDERS = sorted([ESPN(), MLB(), NHL(), NBA(), FootballData(), FixtureDownload(), OpenLigaDB(),
                    NFLverse(), OpenFootball(), Sackmann()], key=lambda p: p.prio)

# ─────────────────── fusión de fuentes por liga ────────────────
RANK = {"pre": 0, "in": 1, "post": 2}
RESULT_FIELDS = ("state", "status", "home_score", "away_score", "winner", "detail")
PERSIST = [c.name for c in events.columns]


class Ledger:
    """Partidos de una liga en memoria: fusiona lo que entregan varias fuentes."""

    def __init__(self, sport, league, league_name):
        self.sport, self.league, self.league_name = sport, league, league_name
        self.rows, self.by_src, self.block = {}, {}, defaultdict(set)
        self.dirty, self.used = set(), set()
        with engine.connect() as c:
            for r in c.execute(select(events).where(
                    events.c.sport == sport, events.c.league == league)).mappings():
                self._add(dict(r))

    # -- índice --
    def _keys(self, r):
        if self.sport == "tennis":
            return [("s", tennis_parts(norm(r["home"], "tennis"))[0]),
                    ("s", tennis_parts(norm(r["away"], "tennis"))[0])]
        d = parse_iso(r.get("start_time"))
        if not d:
            return [("d", "?")]
        return [("d", (d + dt.timedelta(days=k)).date().isoformat()) for k in (-1, 0, 1)]

    def _add(self, r):
        meta_ = json.loads(r["src_ids"]) if r.get("src_ids") else {"ids": {"espn": r["event_id"]}, "flip": []}
        r["_ids"], r["_flip"] = meta_.get("ids", {}), set(meta_.get("flip", []))
        r["name_prio"] = r.get("name_prio") or 1
        self.rows[r["event_id"]] = r
        for s, i in r["_ids"].items():
            self.by_src[(s, str(i))] = r["event_id"]
        for k in self._keys(r):
            self.block[k].add(r["event_id"])
        know(self.sport, r["home"], r["name_prio"])
        know(self.sport, r["away"], r["name_prio"])

    def _candidates(self, inc):
        out = set()
        for k in self._keys(inc):
            out |= self.block.get(k, set())
        return out

    def _match(self, inc, prov):
        best, tol = None, TOL_DAYS.get(self.sport, DEFAULT_TOL)
        for k in self._candidates(inc):
            if (prov.name, k) in self.used:
                continue
            ex = self.rows[k]
            dd = day_diff(inc.get("start_time"), ex.get("start_time"))
            if dd is not None and dd > tol:
                continue
            ok, total, swapped, parts = pair_score(inc, ex, self.sport)
            if not ok:
                continue
            score = (total, -(dd or 0))
            if best is None or score > best[0]:
                best = (score, k, swapped, parts)
        return best

    # -- fusión --
    def merge(self, inc, prov):
        inc = dict(inc)
        inc["home"] = canon(self.sport, inc["home"], prov.prio)
        inc["away"] = canon(self.sport, inc["away"], prov.prio)
        sid = str(inc["src_id"])
        key, swapped = self.by_src.get((prov.name, sid)), False
        if key is not None:
            swapped = prov.name in self.rows[key]["_flip"]
        else:
            m = self._match(inc, prov)
            if m:
                _, key, swapped, _parts = m
        if key is None:
            if prov.lagging and inc["state"] == "pre":
                st = parse_iso(inc.get("start_time"))
                if st and st < utcnow() - dt.timedelta(hours=8):
                    return  # fuente con retraso: un "por jugar" vencido no es fiable
            self._create(inc, prov)
            return
        if swapped:
            inc = flip(inc)
        self.used.add((prov.name, key))
        self._update(self.rows[key], inc, prov, swapped)

    def _create(self, inc, prov):
        eid = f"{prov.name}:{inc['src_id']}"
        if len(eid) > 60:  # tablas creadas por versiones anteriores admiten 60 caracteres
            eid = f"{prov.name}:" + hashlib.md5(str(inc["src_id"]).encode()).hexdigest()[:16]
        r = dict(sport=self.sport, league=self.league, event_id=eid, league_name=self.league_name,
                 start_time=inc.get("start_time"), state=inc["state"], status=inc.get("status"),
                 home=inc["home"], away=inc["away"], home_score=inc.get("home_score"),
                 away_score=inc.get("away_score"), winner=inc.get("winner"),
                 detail=inc.get("detail"), sources=prov.name,
                 src_ids=json.dumps({"ids": {prov.name: str(inc["src_id"])}, "flip": []}),
                 name_prio=prov.prio,
                 raw=json.dumps(inc.get("raw"), ensure_ascii=False) if inc.get("raw") is not None else None,
                 fetched_at=utcnow().replace(tzinfo=None))
        self._add(r)
        self.used.add((prov.name, eid))
        self.dirty.add(eid)

    def _update(self, cur, inc, prov, swapped):
        before = {k: cur.get(k) for k in PERSIST}
        ir, cr = RANK.get(inc["state"], 0), RANK.get(cur["state"], 0)
        if ir > cr:
            for f in RESULT_FIELDS:
                cur[f] = inc.get(f)
        else:
            for f in RESULT_FIELDS:
                if cur.get(f) in (None, "") and inc.get(f) not in (None, ""):
                    cur[f] = inc[f]
        if prov.prio < cur["name_prio"]:   # la fuente nueva manda en nombres/hora/raw
            for old, new in ((cur["home"], inc["home"]), (cur["away"], inc["away"])):
                learn(self.sport, old, new)
            cur["home"], cur["away"] = inc["home"], inc["away"]
            know(self.sport, inc["home"], prov.prio)
            know(self.sport, inc["away"], prov.prio)
            if inc.get("start_time"):
                cur["start_time"] = inc["start_time"]
            if inc.get("raw") is not None:
                cur["raw"] = json.dumps(inc["raw"], ensure_ascii=False)
            cur["name_prio"] = prov.prio
            for k in self._keys(cur):
                self.block[k].add(cur["event_id"])
        else:   # aprende alias: el nombre de la fuente débil apunta al de la principal
            learn(self.sport, inc["home"], cur["home"])
            learn(self.sport, inc["away"], cur["away"])
            if not cur.get("start_time") and inc.get("start_time"):
                cur["start_time"] = inc["start_time"]
        cur["_ids"][prov.name] = str(inc["src_id"])
        self.by_src[(prov.name, str(inc["src_id"]))] = cur["event_id"]
        if swapped:
            cur["_flip"].add(prov.name)
        srcs = sorted(set((cur.get("sources") or "espn").split(",")) | {prov.name})
        cur["sources"] = ",".join(srcs)
        cur["src_ids"] = json.dumps({"ids": cur["_ids"], "flip": sorted(cur["_flip"])})
        after = {k: cur.get(k) for k in PERSIST}
        if after != before:
            cur["fetched_at"] = utcnow().replace(tzinfo=None)
            self.dirty.add(cur["event_id"])

    def save(self):
        rows = [{k: self.rows[e].get(k) for k in PERSIST} for e in self.dirty]
        upsert_table(events, rows, ["sport", "league", "event_id"])
        return len(rows)


def upsert_table(table, rows, keycols):
    if not rows:
        return
    rows = list({tuple(r[k] for k in keycols): r for r in rows}.values())
    dialect = engine.dialect.name
    with engine.begin() as conn:
        if dialect in ("postgresql", "sqlite"):
            if dialect == "postgresql":
                from sqlalchemy.dialects.postgresql import insert as dinsert
            else:
                from sqlalchemy.dialects.sqlite import insert as dinsert
            stmt = dinsert(table)
            upd = {c.name: stmt.excluded[c.name] for c in table.columns if c.name not in keycols}
            conn.execute(stmt.on_conflict_do_update(index_elements=keycols, set_=upd), rows)
        else:
            for r in rows:
                conn.execute(delete(table).where(*[table.c[k] == r[k] for k in keycols]))
            conn.execute(table.insert(), rows)


def load_aliases():
    ALIASES.clear()
    KNOWN.clear()
    NEW_ALIASES.clear()
    with engine.connect() as c:
        for r in c.execute(select(aliases_t)):
            ALIASES[(r.sport, r.alias)] = r.canonical


# ─────────────────────────── poda ──────────────────────────────
def prune():
    """Deja por equipo/jugador sus últimos KEEP_PAST jugados y KEEP_NEXT por jugar."""
    with engine.connect() as conn:
        rows = conn.execute(select(
            events.c.sport, events.c.league, events.c.event_id, events.c.home,
            events.c.away, events.c.start_time, events.c.state, events.c.status)).all()
    keep, past, nxt = set(), defaultdict(list), defaultdict(list)
    for r in rows:
        key = (r.sport, r.league, r.event_id)
        status = (r.status or "").lower()
        if r.state == "in":
            st = parse_iso(r.start_time)
            if not st or st > utcnow() - dt.timedelta(hours=12):
                keep.add(key)
        elif r.state == "post" and not any(b in status for b in NOT_PLAYED):
            for p in (r.home, r.away):
                past[(r.sport, p)].append((r.start_time or "", key))
        elif r.state == "pre":
            for p in (r.home, r.away):
                nxt[(r.sport, p)].append((r.start_time or "", key))
    for lst in past.values():
        lst.sort(reverse=True)
        keep.update(k for _, k in lst[:KEEP_PAST])
    for lst in nxt.values():
        lst.sort()
        keep.update(k for _, k in lst[:KEEP_NEXT])
    drop = [(r.sport, r.league, r.event_id) for r in rows
            if (r.sport, r.league, r.event_id) not in keep]
    with engine.begin() as conn:
        for i in range(0, len(drop), 400):
            conn.execute(delete(events).where(
                tuple_(events.c.sport, events.c.league, events.c.event_id).in_(drop[i:i + 400])))
    log.info("Poda: %d filas eliminadas, %d conservadas", len(drop), len(keep))


# ───────────────────────── orquestación ────────────────────────
def save_health(stats):
    now = utcnow().replace(tzinfo=None)
    with engine.connect() as c:
        prev = {r.source: r for r in c.execute(select(health_t))}
    rows = []
    for p in PROVIDERS:
        s = stats.get(p.name)
        old = prev.get(p.name)
        if s is None or (s["ok"] == 0 and s["fail"] == 0 and p.enabled()):
            continue  # no se consultó en esta pasada: conserva el estado anterior
        if not p.enabled():
            status, detail, last_ok = "off", p.why_off(), old.last_ok if old else None
        elif s["fail"] == 0:
            status, detail, last_ok = "ok", f"{s['ok']} ligas, {s['rows']} filas", now
        elif s["ok"] > 0:
            status, detail, last_ok = "partial", s["err"], now
        else:
            status, detail, last_ok = "fail", s["err"], old.last_ok if old else None
        rows.append(dict(source=p.name, label=p.label, status=status, last_ok=last_ok,
                         last_run=now, detail=(detail or "")[:300]))
    upsert_table(health_t, rows, ["source"])


def run_sync(deep=False):
    ensure_schema()
    load_aliases()
    with engine.connect() as conn:
        total = conn.execute(select(func.count()).select_from(events)).scalar()
    with engine.connect() as conn:
        last_deep = conn.execute(select(health_t.c.last_run).where(health_t.c.source == "_deep")).scalar()
    stale = last_deep is None or utcnow().replace(tzinfo=None) - last_deep > dt.timedelta(hours=DEEP_EVERY_HOURS)
    if total == 0 or stale:
        deep = True  # primera ejecución, o ninguna pasada profunda reciente
    today = utcnow().date()
    if deep:
        start, end = today - dt.timedelta(DEEP_BACK), today + dt.timedelta(DEEP_FWD)
    else:
        start, end = today - dt.timedelta(LIVE_BACK), today + dt.timedelta(LIVE_FWD)
    log.info("Sync %s: %s .. %s", "PROFUNDO" if deep else "ligero", start, end)

    stats = {p.name: dict(ok=0, fail=0, rows=0, err="") for p in PROVIDERS}
    dead, t0 = set(), time.monotonic()
    for sport, league, name in LEAGUES:
        if time.monotonic() - t0 > RUN_BUDGET:
            log.warning("Presupuesto de tiempo agotado: omito %s y las ligas siguientes", name)
            break
        ledger = Ledger(sport, league, name)
        primary_rows, primary_fail = 0, False
        for p in PROVIDERS:
            if not p.supports(league):
                continue
            if not p.enabled():
                continue  # sin configurar: se reporta como "off" en la salud de fuentes
            if p.name in dead:
                continue  # cortacircuitos: cayó en 3 ligas seguidas en esta pasada
            if p.backup and not deep and primary_rows > 0 and not primary_fail:
                continue  # el respaldo solo trabaja si las demás fallan (o en la pasada profunda)
            try:
                got = p.fetch(sport, league, name, start, end, deep)
            except Exception as e:  # noqa: BLE001 - una fuente caída no debe frenar a las demás
                stats[p.name]["fail"] += 1
                stats[p.name]["err"] = f"{league}: {e}"[:200]
                log.warning("[%s] %s FALLÓ: %s", p.name, name, e)
                if not p.backup:
                    primary_fail = True
                if stats[p.name]["ok"] == 0 and stats[p.name]["fail"] >= 3:
                    dead.add(p.name)
                    log.warning("[%s] no responde: la omito el resto de la pasada", p.name)
                continue
            bad = 0
            for r in got:
                try:
                    ledger.merge(r, p)
                except Exception:  # noqa: BLE001 - una fila rara no debe descartar la fuente
                    bad += 1
            if bad:
                log.warning("[%s] %s: %d filas ignoradas por formato inesperado", p.name, name, bad)
            stats[p.name]["ok"] += 1
            stats[p.name]["rows"] += len(got)
            if not p.backup:
                primary_rows += len(got)
            log.info("[%-12s] %-22s %4d filas", p.name, name, len(got))
        try:
            n = ledger.save()
            log.info("%-22s %d filas nuevas/actualizadas", name, n)
        except Exception:  # noqa: BLE001
            log.exception("No se pudo guardar %s; continúo con la siguiente liga", name)
    upsert_table(aliases_t, NEW_ALIASES, ["sport", "alias"])
    if NEW_ALIASES:
        log.info("Alias aprendidos: %d", len(NEW_ALIASES))
    for p in PROVIDERS:
        if not p.enabled():
            stats[p.name]["err"] = p.why_off()
    save_health(stats)
    if deep:
        now = utcnow().replace(tzinfo=None)
        upsert_table(health_t, [dict(source="_deep", label="pasada profunda", status="ok",
                                     last_ok=now, last_run=now, detail="")], ["source"])
    prune()


def run_check():
    """Diagnóstico: cada fuente x cada liga que soporta (ventana ligera)."""
    today = utcnow().date()
    start, end = today - dt.timedelta(LIVE_BACK), today + dt.timedelta(LIVE_FWD + 5)
    for sport, league, name in LEAGUES:
        for p in PROVIDERS:
            if not p.supports(league):
                continue
            if not p.enabled():
                log.info("%-22s %-13s APAGADA: %s", name, p.name, p.why_off())
                continue
            try:
                rows = p.fetch(sport, league, name, start, end, False)
                log.info("%-22s %-13s OK    %3d filas", name, p.name, len(rows))
            except Exception as e:  # noqa: BLE001
                log.info("%-22s %-13s FALLA  %s", name, p.name, e)


# ─────────────────────────── salidas ───────────────────────────
PUBLIC_COLS = ["sport", "league", "league_name", "event_id", "start_time", "state",
               "status", "home", "away", "home_score", "away_score", "winner", "detail", "sources"]


def _public_rows():
    cols = [events.c[c] for c in PUBLIC_COLS]
    with engine.connect() as conn:
        return [dict(zip(PUBLIC_COLS, r)) for r in
                conn.execute(select(*cols).order_by(events.c.start_time))]


def run_site(outdir):
    os.makedirs(outdir, exist_ok=True)
    rows = _public_rows()
    with engine.connect() as conn:
        health = [dict(source=r.source, label=r.label, status=r.status,
                       last_ok=r.last_ok.strftime("%Y-%m-%dT%H:%M:%SZ") if r.last_ok else None,
                       detail=r.detail) for r in conn.execute(select(health_t)) if not r.source.startswith("_")]
    order = {p.name: i for i, p in enumerate(PROVIDERS)}
    health.sort(key=lambda h: order.get(h["source"], 99))
    with open(os.path.join(outdir, "data.json"), "w", encoding="utf-8") as f:
        json.dump({"generated_at": utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"), "events": rows,
                   "sources": health}, f, ensure_ascii=False, separators=(",", ":"))
    with open(os.path.join(outdir, "index.html"), "w", encoding="utf-8") as f:
        f.write(INDEX_HTML)
    log.info("Sitio generado en %s (%d eventos)", outdir, len(rows))


def run_export(out):
    rows = _public_rows()
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PUBLIC_COLS)
        w.writeheader()
        w.writerows(rows)
    log.info("Exportadas %d filas a %s", len(rows), out)


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("sync"); sp.add_argument("--deep", action="store_true")
    sub.add_parser("live")  # alias de sync (compatibilidad)
    sub.add_parser("check")
    lp = sub.add_parser("loop"); lp.add_argument("--interval", type=int, default=600)
    st = sub.add_parser("site"); st.add_argument("--out", default="site")
    ep = sub.add_parser("export"); ep.add_argument("--out", default="datos.csv")
    a = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    ensure_schema()
    if a.cmd in ("sync", "live"):
        run_sync(deep=getattr(a, "deep", False))
    elif a.cmd == "check":
        run_check()
    elif a.cmd == "site":
        run_site(a.out)
    elif a.cmd == "export":
        run_export(a.out)
    elif a.cmd == "loop":
        while True:
            try:
                run_sync()
                run_site("site")
            except Exception:
                log.exception("Fallo en la pasada; reintento en el próximo ciclo")
            time.sleep(a.interval)


INDEX_HTML = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Marcadores deportivos</title>
<style>
:root{--bg:#f4f6f8;--card:#fff;--text:#131820;--muted:#64707f;--line:#e3e7ec;--accent:#0b63c5;
--win:#0f7a55;--loss:#c2301b;--draw:#8a6a00;--live:#d92d20;--hover:#f0f4f9}
@media (prefers-color-scheme:dark){:root{--bg:#0e1116;--card:#161a22;--text:#e7eaef;--muted:#929cab;
--line:#262c37;--accent:#6aa9ff;--win:#4cc79b;--loss:#ff8070;--draw:#e6c34d;--live:#ff6b5e;--hover:#1c222c}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
header{display:flex;flex-wrap:wrap;align-items:baseline;gap:8px 16px;max-width:980px;margin:0 auto;padding:22px 16px 6px}
h1{margin:0;font-size:22px;letter-spacing:-.01em}
#updated{color:var(--muted);font-size:13px}
#updated.old{color:var(--loss)}
main{max-width:980px;margin:0 auto;padding:8px 16px 48px}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px}
.controls{display:grid;grid-template-columns:1fr 1fr 1.4fr auto;gap:12px;padding:14px;align-items:end;margin-bottom:16px}
label{display:flex;flex-direction:column;gap:4px;font-size:12px;color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.04em}
select,input,button{font:inherit;color:var(--text);background:var(--card);border:1px solid var(--line);border-radius:8px;padding:9px 10px;min-width:0;width:100%}
select:focus,input:focus,button:focus-visible{outline:2px solid var(--accent);outline-offset:1px}
button{cursor:pointer;width:auto}
button:hover{background:var(--hover)}
h2{font-size:13px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);margin:22px 4px 8px;display:flex;gap:8px;align-items:center}
h2 .n{background:var(--line);color:var(--text);border-radius:99px;padding:0 8px;font-size:12px}
ul{list-style:none;margin:0;padding:0}
.list{overflow:hidden}
.match{display:grid;grid-template-columns:170px 1fr 190px;gap:10px;align-items:center;padding:11px 14px;border-top:1px solid var(--line)}
.match:first-child{border-top:0}
.match.live{background:color-mix(in srgb,var(--live) 7%,transparent)}
.when{display:flex;gap:8px;align-items:center;color:var(--muted);font-size:13px}
.teams{display:grid;grid-template-columns:1fr auto 1fr;gap:12px;align-items:center}
.t{cursor:pointer;color:var(--text);text-decoration:none}
.t:hover{color:var(--accent);text-decoration:underline}
.t.home{text-align:right}
.t.w{font-weight:700}
.t.me{color:var(--accent)}
.score{font-variant-numeric:tabular-nums;font-weight:700;min-width:56px;text-align:center;background:var(--hover);border-radius:6px;padding:2px 8px}
.score.vs{font-weight:400;color:var(--muted);background:none}
.meta{color:var(--muted);font-size:12.5px;text-align:right}
.chip{display:inline-grid;place-items:center;width:24px;height:24px;border-radius:6px;font-weight:700;font-size:12px;flex:none}
.chip.G{background:color-mix(in srgb,var(--win) 18%,transparent);color:var(--win)}
.chip.P{background:color-mix(in srgb,var(--loss) 16%,transparent);color:var(--loss)}
.chip.E{background:color-mix(in srgb,var(--draw) 18%,transparent);color:var(--draw)}
.chip.N{background:var(--line);color:var(--muted)}
.livebadge{color:var(--live);font-weight:700;font-size:11px;letter-spacing:.05em}
.hero{padding:16px 18px;display:flex;flex-wrap:wrap;gap:12px 24px;align-items:center;justify-content:space-between}
.hero h3{margin:0;font-size:20px}
.hero .sub{color:var(--muted);font-size:13px;margin-top:2px}
.form{display:flex;gap:5px;align-items:center}
.form .chip{width:28px;height:28px}
.summary{color:var(--muted);font-size:13px;margin-top:6px;text-align:right}
.empty{padding:28px;text-align:center;color:var(--muted)}
.more{display:block;margin:12px auto 0}
.note{color:var(--muted);font-size:12.5px;margin:8px 4px 0}
.sources{max-width:980px;margin:0 auto;padding:0 16px 40px;color:var(--muted);font-size:12.5px}
.sources h4{margin:0 0 8px;font-size:12px;text-transform:uppercase;letter-spacing:.06em}
.src{display:inline-flex;gap:6px;align-items:center;margin:0 6px 6px 0;padding:3px 10px;border:1px solid var(--line);border-radius:99px;background:var(--card)}
.src i{width:8px;height:8px;border-radius:50%;background:var(--muted);display:inline-block}
.src.ok i{background:var(--win)}.src.partial i{background:var(--draw)}.src.fail i{background:var(--loss)}.src.off i{background:var(--muted);opacity:.5}
@media (max-width:760px){
 .controls{grid-template-columns:1fr 1fr}.controls label:nth-child(3){grid-column:1/-1}
 .match{grid-template-columns:1fr;gap:4px}.meta{text-align:left}.summary{text-align:left}
}
</style>
</head>
<body>
<header><h1>Marcadores deportivos</h1><span id="updated"></span></header>
<main>
 <section class="card controls">
  <label>Deporte<select id="sport"></select></label>
  <label>Liga / torneo<select id="league"></select></label>
  <label><span id="whoLabel">Equipo</span>
   <input id="who" list="whoList" placeholder="Escribe para buscar…" autocomplete="off">
   <datalist id="whoList"></datalist></label>
  <button id="clear" type="button">Limpiar</button>
 </section>
 <div id="view"></div>
</main>
<footer class="sources" id="sources"></footer>
<script>
const SPORTS={soccer:"Fútbol",basketball:"Baloncesto",football:"Fútbol americano",baseball:"Béisbol",hockey:"Hockey sobre hielo",tennis:"Tenis",rugby:"Rugby"};
const $=id=>document.getElementById(id);
let DATA=[],S={sport:"",league:"",who:"",limit:30};

const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const fmt=iso=>{const d=new Date(iso);return isNaN(d)?"":d.toLocaleString("es",{weekday:"short",day:"numeric",month:"short",hour:"2-digit",minute:"2-digit"})};
function ago(iso){if(!iso)return"nunca";const m=Math.round((Date.now()-utc(iso))/60000);
  return m<2?"ahora":m<60?`hace ${m} min`:m<2880?`hace ${Math.round(m/60)} h`:`hace ${Math.round(m/1440)} d`}
function renderSources(list){
  const el=$("sources");if(!list||!list.length){el.innerHTML="";return}
  const txt={ok:"funciona",partial:"parcial",fail:"con fallos",off:"sin configurar"};
  el.innerHTML='<h4>Fuentes de datos</h4>'+list.map(s=>`<span class="src ${esc(s.status)}" title="${esc(s.detail||"")}"><i></i>${esc(s.label)} · ${txt[s.status]||s.status}${s.last_ok?" · "+ago(s.last_ok):""}</span>`).join("");
}
const utc=s=>new Date(/(Z|[+-]\d\d:\d\d)$/i.test(s)?s:s+"Z");
const byStart=(a,b)=>(a.start_time||"").localeCompare(b.start_time||"");

function winnerOf(e){
  if(e.winner) return e.winner;
  const h=parseFloat(e.home_score),a=parseFloat(e.away_score);
  if(isNaN(h)||isNaN(a)) return null;
  return h>a?"home":a>h?"away":"draw";
}
function outcome(e,who){
  if(e.state!=="post") return "";
  const w=winnerOf(e); if(!w) return "N"; if(w==="draw") return "E";
  return (w==="home")===(e.home===who)?"G":"P";
}

function matchRow(e,who){
  const live=e.state==="in",done=e.state==="post",w=done?winnerOf(e):null;
  const hasScore=(live||done)&&e.home_score!=null&&e.away_score!=null;
  const chip=who&&done?`<span class="chip ${outcome(e,who)}">${({G:"G",P:"P",E:"E",N:"–"})[outcome(e,who)]}</span>`:"";
  const cls=(side,name)=>`t ${side} ${w===side?"w":""} ${who===name?"me":""}`;
  const meta=[e.league_name,e.detail,live&&e.status?e.status:""].filter(Boolean).map(esc).join(" · ");
  return `<li class="match ${live?"live":""}">
   <div class="when">${chip}<span>${fmt(e.start_time)}</span>${live?'<span class="livebadge">EN VIVO</span>':""}</div>
   <div class="teams">
    <a class="${cls("home",e.home)}" data-who="${esc(e.home)}">${esc(e.home)}</a>
    <span class="score ${hasScore?"":"vs"}">${hasScore?`${esc(e.home_score)} – ${esc(e.away_score)}`:"vs"}</span>
    <a class="${cls("away",e.away)}" data-who="${esc(e.away)}">${esc(e.away)}</a>
   </div><div class="meta" title="Fuentes: ${esc(e.sources||"espn")}">${meta}</div></li>`;
}
const section=(title,items,who,extra="")=>items.length?
  `<h2>${title}<span class="n">${items.length}</span></h2><ul class="card list">${items.map(e=>matchRow(e,who)).join("")}</ul>${extra}`:"";

function sportEvents(){return DATA.filter(e=>e.sport===S.sport)}
function leagueEvents(){return sportEvents().filter(e=>!S.league||e.league===S.league)}
function participants(){
  const s=new Set();leagueEvents().forEach(e=>{s.add(e.home);s.add(e.away)});
  return [...s].sort((a,b)=>a.localeCompare(b,"es"));
}

function fillControls(){
  const sports=Object.keys(SPORTS).filter(k=>DATA.some(e=>e.sport===k));
  $("sport").innerHTML=sports.map(k=>`<option value="${k}">${SPORTS[k]}</option>`).join("");
  if(!sports.includes(S.sport)) S.sport=sports[0]||"";
  $("sport").value=S.sport;
  const leagues=new Map();sportEvents().forEach(e=>leagues.set(e.league,e.league_name));
  $("league").innerHTML='<option value="">Todas las ligas</option>'+[...leagues].sort((a,b)=>a[1].localeCompare(b[1],"es")).map(([id,n])=>`<option value="${esc(id)}">${esc(n)}</option>`).join("");
  if(S.league&&!leagues.has(S.league)) S.league="";
  $("league").value=S.league;
  $("whoLabel").textContent=S.sport==="tennis"?"Jugador":"Equipo";
  $("whoList").innerHTML=participants().map(p=>`<option value="${esc(p)}">`).join("");
  $("who").value=S.who;
}

function render(){
  fillControls();
  location.hash=[S.sport,S.league,S.who].map(encodeURIComponent).join("/");
  const v=$("view");
  if(!DATA.length){v.innerHTML='<div class="card empty">Aún no hay datos. Espera a que termine la primera ejecución del recolector.</div>';return}
  if(S.who){
    const mine=sportEvents().filter(e=>e.home===S.who||e.away===S.who);
    const live=mine.filter(e=>e.state==="in").sort(byStart);
    const next=mine.filter(e=>e.state==="pre").sort(byStart).slice(0,1);
    const past=mine.filter(e=>e.state==="post").sort(byStart).reverse().slice(0,10);
    const res=past.map(e=>outcome(e,S.who));
    const cnt=k=>res.filter(r=>r===k).length;
    const formChips=past.slice(0,5).reverse().map(e=>{const o=outcome(e,S.who);return `<span class="chip ${o}">${o==="N"?"–":o}</span>`}).join("");
    const leagues=[...new Set(mine.map(e=>e.league_name))].join(" · ");
    v.innerHTML=`<div class="card hero"><div><h3>${esc(S.who)}</h3><div class="sub">${SPORTS[S.sport]||""} · ${esc(leagues)}</div></div>
      <div><div class="form" title="Últimos 5, el más reciente a la derecha">${formChips}</div>
      <div class="summary">Últimos ${past.length}: ${cnt("G")} G · ${cnt("E")} E · ${cnt("P")} P</div></div></div>`
      +section("En vivo",live,S.who)+section("Próximo partido",next,S.who)+section("Últimos 10 partidos",past,S.who)
      +'<p class="note">Incluye todas las competiciones en las que participa. G = ganó, E = empató, P = perdió.</p>';
  }else{
    const ev=leagueEvents();
    const live=ev.filter(e=>e.state==="in").sort(byStart);
    const next=ev.filter(e=>e.state==="pre").sort(byStart);
    const past=ev.filter(e=>e.state==="post").sort(byStart).reverse();
    const more=(arr)=>arr.length>S.limit?`<button class="more" data-more="1" type="button">Mostrar más</button>`:"";
    v.innerHTML=section("En vivo",live,"")
      +section("Próximos partidos",next.slice(0,S.limit),"",more(next))
      +section("Resultados recientes",past.slice(0,S.limit),"",more(past))
      +'<p class="note">Haz clic en un equipo o jugador para ver sus últimos 10 partidos y el próximo.</p>';
  }
}

function setWho(name){S.who=name;S.limit=30;render();window.scrollTo({top:0,behavior:"smooth"})}
$("sport").onchange=e=>{S={sport:e.target.value,league:"",who:"",limit:30};render()};
$("league").onchange=e=>{S.league=e.target.value;S.who="";S.limit=30;render()};
$("who").oninput=e=>{const v=e.target.value;if(v===""){S.who="";render()}else if(participants().includes(v)){setWho(v)}};
$("clear").onclick=()=>{S.league="";S.who="";S.limit=30;render()};
document.addEventListener("click",e=>{
  const t=e.target.closest("[data-who]");if(t){setWho(t.dataset.who);return}
  if(e.target.closest("[data-more]")){S.limit+=30;render()}
});

fetch("data.json?"+Date.now()).then(r=>r.json()).then(j=>{
  DATA=j.events||[];renderSources(j.sources);
  const age=(Date.now()-utc(j.generated_at))/60000;
  const u=$("updated");
  u.textContent="Actualizado "+utc(j.generated_at).toLocaleString("es",{day:"numeric",month:"short",hour:"2-digit",minute:"2-digit"})+
    (age>90?" · los datos pueden estar desactualizados":"");
  if(age>90) u.className="old";
  const [s,l,w]=location.hash.slice(1).split("/").map(x=>x?decodeURIComponent(x):"");
  S.sport=s||"";S.league=l||"";S.who=w||"";
  render();
}).catch(()=>{$("view").innerHTML='<div class="card empty">No se pudo cargar data.json.</div>'});
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
