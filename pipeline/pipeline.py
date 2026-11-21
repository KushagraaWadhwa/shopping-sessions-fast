#!/usr/bin/env python3
"""Exact shopping sessions pipeline. CSV validation is deliberately explicit."""
import bisect
import glob
import json
import os
import re
from datetime import date, datetime, timezone

import duckdb

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "24"))
DAY = 86_400_000
WINDOW = 604_800_000
ASCII_DIGITS = re.compile(r"^[0-9]+$")
CSV_LINE = re.compile(r'^(?:[^",\n]*|"[^"\n]*")(?:,(?:[^",\n]*|"[^"\n]*"))*\n?$')


def rows(path, expected):
    """Read the specified one-line CSV dialect; return only well-formed records."""
    with open(path, "r", encoding="utf-8", newline="") as f:
        it = iter(f)
        next(it, None)
        for line in it:
            line = line[:-1] if line.endswith("\n") else line
            if not line:
                continue
            if not CSV_LINE.fullmatch(line):
                continue
            raw_fields = line.split(",")
            if len(raw_fields) != expected:
                continue
            fields = [x[1:-1] if x.startswith('"') and x.endswith('"') else x for x in raw_fields]
            yield [None if x == "" else x for x in fields]


def epoch_ms(s):
    if s is None:
        return None
    if ASCII_DIGITS.fullmatch(s):
        try:
            n = int(s)
            return n if n <= 9_223_372_036_854_775_807 else None
        except ValueError:
            return None
    try:
        if len(s) == 20 and s[10] == "T" and s.endswith("Z"):
            d = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        elif len(s) == 25 and s[10] == "T" and s[19] in "+-" and s[22] == ":":
            hh, mm = int(s[20:22]), int(s[23:25])
            if hh > 23 or mm > 59:
                return None
            d = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            delta = (hh * 60 + mm) * (1 if s[19] == "+" else -1)
            return int(d.timestamp() * 1000) - delta * 60_000
        else:
            return None
        return int(d.timestamp() * 1000)
    except (ValueError, OverflowError):
        return None


class DSU:
    def __init__(self):
        self.p = {}
    def find(self, x):
        p = self.p.setdefault(x, x)
        if p != x:
            self.p[x] = self.find(p)
        return self.p[x]
    def union(self, a, b):
        a, b = self.find(a), self.find(b)
        if a != b:
            # Names are explicitly the byte-wise smallest ID.
            if a.encode() <= b.encode(): self.p[b] = a
            else: self.p[a] = b


with open(os.path.join(DATA, "manifest.json"), encoding="utf-8") as f:
    manifest = json.load(f)
start, end = epoch_ms(manifest["window_start"]), epoch_ms(manifest["window_end"])

dsu = DSU()
for path in glob.glob(os.path.join(DATA, "links", "*.csv")):
    for a, b in rows(path, 2):
        if a and b:
            dsu.union(a, b)

products = {}
for p, c in rows(os.path.join(DATA, "products.csv"), 2):
    if p is not None:
        products[p] = c or "unknown"
campaigns = {}
for c, ch in rows(os.path.join(DATA, "campaigns.csv"), 2):
    if c is not None:
        campaigns[c] = ch

# Dedup first, exactly by earliest ingest time. The fixture dialect contains no multiline fields.
events = {}
for path in glob.glob(os.path.join(DATA, "events", "*.csv")):
    for r in rows(path, 7):
        eid, uid, ts, ingest, typ, prod, price = r
        if eid is None:
            continue
        key = (epoch_ms(ingest), tuple("" if x is None else x for x in r))
        if eid not in events or key < events[eid][0]:
            events[eid] = (key, r)
clean = []
for (_, r) in events.values():
    eid, uid, ts0, ing0, typ, prod, price0 = r
    ts, ing = epoch_ms(ts0), epoch_ms(ing0)
    if not uid or ts is None or not start <= ts < end or typ not in ("view", "cart", "purchase") or not prod:
        continue
    price = None
    if typ == "purchase":
        if price0 is None or not ASCII_DIGITS.fullmatch(price0): continue
        price = int(price0)
        if not 1 <= price <= 10_000_000: continue
    if ing - ts > DAY: continue
    clean.append((dsu.find(uid), ts, typ, prod, price))

clicks = {}
for path in glob.glob(os.path.join(DATA, "clicks", "*.csv")):
    for r in rows(path, 4):
        cid, uid, camp, ts0 = r
        if cid and cid not in clicks: clicks[cid] = r
valid_clicks = {}
for cid, (cid0, uid, camp, ts0) in clicks.items():
    ts = epoch_ms(ts0)
    if not uid or ts is None or not start - WINDOW <= ts < end or camp not in campaigns: continue
    valid_clicks.setdefault(dsu.find(uid), []).append((ts, cid, camp))
for v in valid_clicks.values(): v.sort()
click_times = {p: [x[0] for x in v] for p, v in valid_clicks.items()}

con = duckdb.connect()
con.execute(f"SET threads={NCPU}")
con.execute(f"SET memory_limit='{max(1, int(MEM_GB * .8))}GB'")
os.makedirs(os.path.join(SCRATCH, "duckdb_tmp"), exist_ok=True)
temp_dir = os.path.join(SCRATCH, "duckdb_tmp").replace("'", "''")
con.execute(f"SET temp_directory='{temp_dir}'")
con.execute("SET preserve_insertion_order=false")

# Push event aggregation and session boundaries to DuckDB; each person sorts once.
pa = __import__("pyarrow")
con.register("evt_in", pa.Table.from_pylist([
    {"person": p, "ts": t, "typ": ty, "prod": pr, "price": pc} for p,t,ty,pr,pc in clean
], schema=pa.schema([("person",pa.string()),("ts",pa.int64()),("typ",pa.string()),("prod",pa.string()),("price",pa.int64())])))
con.register("prod_in", pa.Table.from_pylist([{"prod":p,"category":c} for p,c in products.items()], schema=pa.schema([("prod",pa.string()),("category",pa.string())])))
numbered_sql = """
WITH ordered AS (
 SELECT *, CASE WHEN lag(ts) OVER (PARTITION BY person ORDER BY ts) IS NULL OR
   ts-lag(ts) OVER (PARTITION BY person ORDER BY ts)>1800000 THEN 1 ELSE 0 END AS new_s
 FROM evt_in
), numbered AS (
 SELECT *, sum(new_s) OVER (PARTITION BY person ORDER BY ts ROWS UNBOUNDED PRECEDING) AS sn
 FROM ordered
)
SELECT * FROM numbered
"""
con.execute("CREATE TEMP TABLE numbered_events AS " + numbered_sql)
session_sql = """
WITH sess AS (
 SELECT person, sn, min(ts) AS st, count(*) AS ev,
   count(*) FILTER (WHERE typ='purchase')>0 AS purchasing,
   coalesce(sum(price) FILTER (WHERE typ='purchase'),0)::BIGINT AS revenue
 FROM numbered_events GROUP BY person,sn
)
SELECT s.*, NULL::VARCHAR AS top_category FROM sess s
"""
con.execute("CREATE TEMP TABLE sessions AS " + session_sql)
sess = con.execute("SELECT person,sn,st,ev,purchasing,revenue,top_category FROM sessions ORDER BY person,st").fetchall()

credited = []
assists = []
for person, sn, st, ev, purchasing, revenue, topcat in sess:
    cs = valid_clicks.get(person, ())
    times = click_times.get(person, ())
    lo, hi = bisect.bisect_left(times, st-WINDOW), bisect.bisect_right(times, st)
    qualifying = cs[lo:hi]
    if qualifying:
        chosen = min(qualifying, key=lambda x: (-x[0], x[1].encode()))
        camp = chosen[2]
        assists.extend((person, sn, st, c) for c in {x[2] for x in qualifying})
    else:
        camp = "organic"
    credited.append((person,sn,st,ev,purchasing,revenue,topcat,camp))

con.register("sess_in", pa.Table.from_pylist([
 {"person":p,"sn":sn,"st":st,"ev":ev,"purchasing":pu,"revenue":rev,"top_category":tc,"campaign":ca}
 for p,sn,st,ev,pu,rev,tc,ca in credited], schema=pa.schema([("person",pa.string()),("sn",pa.int64()),("st",pa.int64()),("ev",pa.int64()),("purchasing",pa.bool_()),("revenue",pa.int64()),("top_category",pa.string()),("campaign",pa.string())])))
con.register("assist_in", pa.Table.from_pylist([{"person":p,"sn":sn,"st":st,"campaign":c} for p,sn,st,c in assists], schema=pa.schema([("person",pa.string()),("sn",pa.int64()),("st",pa.int64()),("campaign",pa.string())])))
con.register("camp_in", pa.Table.from_pylist([{"campaign":c,"channel":ch} for c,ch in campaigns.items()], schema=pa.schema([("campaign",pa.string()),("channel",pa.string())])))
os.makedirs(os.path.join(OUT,"user_daily"),exist_ok=True)
os.makedirs(os.path.join(OUT,"campaign_daily"),exist_ok=True)
con.execute(f"""COPY (WITH cats AS (
 SELECT n.person,CAST(to_timestamp(s.st/1000) AS DATE) AS session_day,coalesce(p.category,'unknown') AS category,sum(n.price)::BIGINT AS rev
 FROM numbered_events n JOIN sess_in s ON n.person=s.person AND n.sn=s.sn
 LEFT JOIN prod_in p ON n.prod=p.prod WHERE n.typ='purchase'
 GROUP BY n.person,session_day,category
), ranked AS (SELECT *, row_number() OVER(PARTITION BY person,session_day ORDER BY rev DESC,category COLLATE "binary") AS rn FROM cats),
daily AS (SELECT person,CAST(to_timestamp(st/1000) AS DATE) AS session_day,count(*)::BIGINT AS sessions,sum(ev)::BIGINT AS events,sum(revenue)::BIGINT AS revenue_cents
 FROM sess_in GROUP BY person,session_day)
 SELECT d.person AS user_id,d.session_day AS "day",d.sessions,d.events,d.revenue_cents,r.category AS top_category FROM daily d
 LEFT JOIN ranked r ON r.person=d.person AND r.session_day=d.session_day AND r.rn=1
 ) TO '{os.path.join(OUT,'user_daily','part-0.parquet')}' (FORMAT parquet)""")
con.execute(f"""COPY (WITH credited_daily AS (
 SELECT campaign, CAST(to_timestamp(st/1000) AS DATE) AS session_day,count(*)::BIGINT AS sessions,
 count(*) FILTER(WHERE purchasing)::BIGINT AS purchasing_sessions,sum(revenue)::BIGINT AS revenue_cents
 FROM sess_in GROUP BY campaign,session_day
), assist_daily AS (SELECT campaign,CAST(to_timestamp(st/1000) AS DATE) AS session_day,count(*)::BIGINT AS assisted_sessions
 FROM (SELECT DISTINCT person,sn,st,campaign FROM assist_in) GROUP BY campaign,session_day), keys AS (
 SELECT campaign,session_day FROM credited_daily UNION SELECT campaign,session_day FROM assist_daily)
 SELECT k.campaign AS campaign_id,coalesce(c.channel,'organic') AS channel,k.session_day AS "day",
 coalesce(d.sessions,0)::BIGINT sessions,coalesce(d.purchasing_sessions,0)::BIGINT purchasing_sessions,
 coalesce(d.revenue_cents,0)::BIGINT revenue_cents,coalesce(a.assisted_sessions,0)::BIGINT assisted_sessions
 FROM keys k LEFT JOIN credited_daily d USING(campaign,session_day) LEFT JOIN assist_daily a USING(campaign,session_day)
 LEFT JOIN camp_in c ON k.campaign=c.campaign
 ) TO '{os.path.join(OUT,'campaign_daily','part-0.parquet')}' (FORMAT parquet)""")
print(f"wrote {len(sess)} sessions")

