"""
GTFS-Realtime collector for the NTA (Transport for Ireland) feed.

Polls the TripUpdates and Vehicles endpoints at a fixed interval, parses the
protobuf messages and appends flat rows to daily gzipped CSV files:

    data/raw/trip_updates/YYYY-MM-DD.csv.gz   one row per stop_time_update
    data/raw/vehicles/YYYY-MM-DD.csv.gz       one row per vehicle position

Optionally also keeps the raw protobuf snapshots (--save-raw) so the data can be
re-parsed later if the parsing logic changes.

Usage:
    export NTA_API_KEY=your_primary_key      (or put it in a .env file)
    python collector.py                      # all routes, every 60 s
    python collector.py --routes 3925_119 3925_120 --interval 60
    python collector.py --once               # single poll, for testing
"""

import argparse
import csv
import gzip
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from google.transit import gtfs_realtime_pb2

BASE_URL = "https://api.nationaltransport.ie/gtfsr/v2"
ENDPOINTS = {
    "trip_updates": f"{BASE_URL}/TripUpdates",
    "vehicles": f"{BASE_URL}/Vehicles",
}

TRIP_UPDATE_FIELDS = [
    "fetch_ts", "feed_ts", "trip_id", "route_id", "direction_id", "start_date",
    "start_time", "trip_schedule_rel", "vehicle_id", "trip_delay", "update_ts",
    "stop_sequence", "stop_id", "arrival_delay", "arrival_time",
    "departure_delay", "departure_time", "stop_schedule_rel",
]

VEHICLE_FIELDS = [
    "fetch_ts", "feed_ts", "vehicle_id", "trip_id", "route_id", "direction_id",
    "start_date", "start_time", "lat", "lon", "bearing", "speed",
    "current_stop_sequence", "stop_id", "current_status", "vehicle_ts",
]

log = logging.getLogger("collector")
_running = True


def _stop(signum, frame):
    global _running
    log.info("Stop signal received, finishing current poll...")
    _running = False


def _read_text_any(path: Path) -> str:
    """Read a small text file whatever its encoding (PowerShell often writes
    UTF-16 or UTF-8 with a BOM, which would otherwise hide the key)."""
    raw = path.read_bytes()
    for enc in ("utf-8-sig", "utf-16"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1")


def load_api_key() -> str:
    key = os.environ.get("NTA_API_KEY")
    here = Path(__file__).resolve().parent
    env_file = here / ".env"
    if not key and env_file.exists():
        for line in _read_text_any(env_file).splitlines():
            line = line.strip().lstrip("﻿")
            if line.startswith("NTA_API_KEY="):
                key = line.split("=", 1)[1].strip().strip('"').strip("'")
    if not key:
        hint = ""
        if not env_file.exists():
            stray = [p.name for p in here.glob("*env*") if p.is_file()]
            hint = (f"\n  No .env file in {here}"
                    + (f" (found: {', '.join(stray)})" if stray else ""))
        elif key == "":
            hint = f"\n  {env_file} has an empty NTA_API_KEY= line"
        else:
            hint = f"\n  {env_file} exists but has no NTA_API_KEY= line"
        sys.exit("No API key found. Set NTA_API_KEY or create a .env file." + hint)
    return key


def fetch_feed(session, url, api_key, timeout=30):
    """Download one GTFS-R feed and return (raw_bytes, parsed FeedMessage)."""
    resp = session.get(
        url,
        headers={"x-api-key": api_key, "Cache-Control": "no-cache"},
        timeout=timeout,
    )
    resp.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(resp.content)
    return resp.content, feed


def _opt(msg, field):
    """Return a protobuf field value, or None if it is not set."""
    try:
        return getattr(msg, field) if msg.HasField(field) else None
    except ValueError:  # field is not optional (e.g. repeated)
        return getattr(msg, field)


def parse_trip_updates(feed, fetch_ts, routes=None):
    rel_trip = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name
    rel_stop = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name
    feed_ts = feed.header.timestamp or None
    rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip = tu.trip
        if routes and trip.route_id not in routes:
            continue
        base = {
            "fetch_ts": fetch_ts,
            "feed_ts": feed_ts,
            "trip_id": trip.trip_id,
            "route_id": trip.route_id,
            "direction_id": _opt(trip, "direction_id"),
            "start_date": trip.start_date,
            "start_time": trip.start_time,
            "trip_schedule_rel": rel_trip(trip.schedule_relationship),
            "vehicle_id": tu.vehicle.id if tu.HasField("vehicle") else None,
            "trip_delay": _opt(tu, "delay"),
            "update_ts": _opt(tu, "timestamp"),
        }
        if not tu.stop_time_update:  # e.g. cancelled trips have no stop updates
            rows.append({**base})
            continue
        for stu in tu.stop_time_update:
            arr = stu.arrival if stu.HasField("arrival") else None
            dep = stu.departure if stu.HasField("departure") else None
            rows.append({
                **base,
                "stop_sequence": _opt(stu, "stop_sequence"),
                "stop_id": _opt(stu, "stop_id"),
                "arrival_delay": _opt(arr, "delay") if arr else None,
                "arrival_time": _opt(arr, "time") if arr else None,
                "departure_delay": _opt(dep, "delay") if dep else None,
                "departure_time": _opt(dep, "time") if dep else None,
                "stop_schedule_rel": rel_stop(stu.schedule_relationship),
            })
    return rows


def parse_vehicles(feed, fetch_ts, routes=None):
    status_name = gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name
    feed_ts = feed.header.timestamp or None
    rows = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        vp = entity.vehicle
        trip = vp.trip
        if routes and trip.route_id not in routes:
            continue
        pos = vp.position if vp.HasField("position") else None
        rows.append({
            "fetch_ts": fetch_ts,
            "feed_ts": feed_ts,
            "vehicle_id": vp.vehicle.id if vp.HasField("vehicle") else None,
            "trip_id": trip.trip_id,
            "route_id": trip.route_id,
            "direction_id": _opt(trip, "direction_id"),
            "start_date": trip.start_date,
            "start_time": trip.start_time,
            "lat": pos.latitude if pos else None,
            "lon": pos.longitude if pos else None,
            "bearing": _opt(pos, "bearing") if pos else None,
            "speed": _opt(pos, "speed") if pos else None,
            "current_stop_sequence": _opt(vp, "current_stop_sequence"),
            "stop_id": _opt(vp, "stop_id"),
            "current_status": status_name(vp.current_status)
            if vp.HasField("current_status") else None,
            "vehicle_ts": _opt(vp, "timestamp"),
        })
    return rows


def append_rows(path: Path, fields, rows):
    """Append rows to a gzipped CSV (gzip supports appended members)."""
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with gzip.open(path, "at", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new_file:
            writer.writeheader()
        writer.writerows(rows)


def save_raw(out_dir: Path, name: str, raw: bytes, now: datetime):
    path = out_dir / "pb" / name / now.strftime("%Y-%m-%d") / f"{now:%H%M%S}.pb.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as f:
        f.write(raw)


def _retry_after(resp, default):
    """Seconds to wait from a 429 response's Retry-After header, if present."""
    try:
        return max(1, int(float(resp.headers.get("Retry-After", ""))))
    except (TypeError, ValueError, AttributeError):
        return default


def poll_feed(session, api_key, out_dir, routes, keep_raw, name):
    """Poll one feed and save it. Returns None on success, or the number of
    seconds to back off if the API rate-limited the request."""
    now = datetime.now(timezone.utc)
    fetch_ts = int(now.timestamp())
    day = now.strftime("%Y-%m-%d")
    try:
        raw, feed = fetch_feed(session, ENDPOINTS[name], api_key)
    except requests.HTTPError as e:
        resp = e.response
        code = resp.status_code if resp is not None else "?"
        if code == 429:
            wait = _retry_after(resp, 60)
            log.warning("%s: HTTP 429 rate limited - waiting %ss before retrying "
                        "this feed", name, wait)
            return wait
        log.warning("%s: HTTP %s (%s)", name, code,
                    "check your API key" if code in (401, 403) else "error")
        return None
    except Exception as e:  # network error or bad protobuf
        log.warning("%s: %s", name, e)
        return None

    if name == "trip_updates":
        rows = parse_trip_updates(feed, fetch_ts, routes)
        fields = TRIP_UPDATE_FIELDS
    else:
        rows = parse_vehicles(feed, fetch_ts, routes)
        fields = VEHICLE_FIELDS
    append_rows(out_dir / name / f"{day}.csv.gz", fields, rows)
    if keep_raw:
        save_raw(out_dir, name, raw, now)
    log.info("%s: %d entities, %d rows saved", name, len(feed.entity), len(rows))
    return None


def poll_once(session, api_key, out_dir, routes, keep_raw, feeds):
    for name in feeds:
        poll_feed(session, api_key, out_dir, routes, keep_raw, name)


def main():
    p = argparse.ArgumentParser(description="Collect NTA GTFS-Realtime snapshots.")
    p.add_argument("--interval", type=int, default=60,
                   help="seconds between polls (default 60)")
    p.add_argument("--routes", nargs="*", default=None,
                   help="only keep these GTFS route_ids (default: all routes)")
    p.add_argument("--out", default="data/raw", help="output directory")
    p.add_argument("--feeds", nargs="+", default=list(ENDPOINTS),
                   choices=list(ENDPOINTS), help="which feeds to poll")
    p.add_argument("--save-raw", action="store_true",
                   help="also keep the raw protobuf snapshots")
    p.add_argument("--once", action="store_true", help="poll once and exit")
    p.add_argument("--duration", type=int, default=None,
                   help="stop cleanly after this many seconds (used on GitHub "
                        "Actions, where a job may run at most 6 hours)")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("collector.log")],
    )
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    api_key = load_api_key()
    out_dir = Path(args.out)
    routes = set(args.routes) if args.routes else None
    session = requests.Session()

    log.info("Polling %s every %ss -> %s (routes: %s)%s",
             ", ".join(args.feeds), args.interval, out_dir,
             ", ".join(sorted(routes)) if routes else "all",
             f", stopping after {args.duration}s" if args.duration else "")

    if args.once:
        poll_once(session, api_key, out_dir, routes, args.save_raw, args.feeds)
        log.info("Collector stopped.")
        return

    # Stagger the feeds evenly across the interval so the API never sees two
    # requests at once (e.g. 60 s interval, 2 feeds -> one request every 30 s).
    offset = args.interval / len(args.feeds)
    t0 = time.monotonic()
    next_due = {name: t0 + i * offset for i, name in enumerate(args.feeds)}

    deadline = t0 + args.duration if args.duration else None

    while _running:
        now = time.monotonic()
        if deadline and now >= deadline:
            log.info("Reached --duration of %ss.", args.duration)
            break
        for name in args.feeds:
            if now >= next_due[name]:
                backoff = poll_feed(session, api_key, out_dir, routes,
                                    args.save_raw, name)
                # keep the original rhythm; push back only if rate limited
                next_due[name] += args.interval
                while next_due[name] <= time.monotonic():
                    next_due[name] += args.interval
                if backoff:
                    next_due[name] = max(next_due[name], time.monotonic() + backoff)
        time.sleep(0.5)
    log.info("Collector stopped.")


if __name__ == "__main__":
    main()
