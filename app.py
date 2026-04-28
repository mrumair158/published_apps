import streamlit as st
import requests
import pandas as pd
import math
import io
import time
from datetime import datetime
from urllib.parse import urlparse, parse_qs

st.set_page_config(page_title="Google Maps Scraper", page_icon="🗺️", layout="wide")
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@400;500;600&display=swap');
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
    .block-container { padding-top: 1.5rem; max-width: 1400px; }
    div[data-testid="metric-container"] { background: rgba(240,253,244,0.08); border-radius:10px; padding:14px; border:1px solid #bbf7d0; }
    .stDataFrame { font-family: 'DM Mono', monospace; font-size: 13px; }
    .api-note { background:rgba(59,130,246,0.1); border-left:4px solid #3b82f6; padding:10px 14px; border-radius:4px; font-size:13px; margin-bottom:10px; color:inherit; }
    .warn-note { background:rgba(249,115,22,0.1); border-left:4px solid #f97316; padding:10px 14px; border-radius:4px; font-size:13px; margin-bottom:10px; color:inherit; }
    .good-note { background:rgba(34,197,94,0.1); border-left:4px solid #22c55e; padding:10px 14px; border-radius:4px; font-size:13px; margin-bottom:10px; color:inherit; }
</style>
""", unsafe_allow_html=True)

SERPAPI_ENDPOINT = "https://serpapi.com/search"

#  GEOCODING  — get city center + bounding box from location name, via OpenStreetMap API

def geocode(location: str) -> dict | None:
    """
    Returns {lat, lng, display_name, boundingbox} or None.
    boundingbox = [min_lat, max_lat, min_lng, max_lng]
    """
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": location, "format": "json", "limit": 1},
            headers={"User-Agent": "gmaps-streamlit-scraper/2.0"},
            timeout=10,
        )
        data = r.json()
        if not data:
            return None
        d = data[0]
        bb = d.get("boundingbox", [])         # [min_lat, max_lat, min_lng, max_lng]
        return {
            "lat":          float(d["lat"]),
            "lng":          float(d["lon"]),
            "display_name": d.get("display_name", location),
            "bbox":         [float(x) for x in bb] if len(bb) == 4 else None,
        }
    except Exception:
        return None


#  GRID  — tile the bounding box into NxN cells and return their center GPS coordinates

def make_grid(center_lat: float, center_lng: float,
              radius_km: float, n: int) -> list[tuple[float, float]]:
    """
    NxN grid of (lat, lng) evenly spaced within ±radius_km of center.
    n=1 -> single center point (standard search).
    """
    if n == 1:
        return [(center_lat, center_lng)]
    lat_d = radius_km / 111.0
    lng_d = radius_km / (111.0 * math.cos(math.radians(center_lat)))
    pts = []
    for i in range(n):
        for j in range(n):
            lat = center_lat - lat_d + (2 * lat_d * i / (n - 1))
            lng = center_lng - lng_d + (2 * lng_d * j / (n - 1))
            pts.append((lat, lng))
    return pts


#  SERPAPI  — get all pages of results for one keyword + one grid cell
def fetch_all_pages(query: str, ll: str, api_key: str, max_pages: int) -> list[dict]:
    """
    Fetches up to max_pages×20 results for one grid cell.
    KEY FIX: query = keyword only (no city name).
              ll   = GPS of this grid cell.
    """
    all_results = []
    params = {
        "engine":  "google_maps",
        "q":       query,
        "ll":      ll,
        "type":    "search",
        "hl":      "en",
        "start":   0,
        "api_key": api_key,
    }

    for page in range(max_pages):
        try:
            r = requests.get(SERPAPI_ENDPOINT, params=params, timeout=30)
            if r.status_code != 200:
                break
            data = r.json()
            if "error" in data:
                # handling api key errors (bad key / quota)
                if any(x in data["error"] for x in ["Invalid", "quota", "out of searches", "account"]):
                    raise ValueError(data["error"])
                break
            batch = data.get("local_results", [])
            if not batch:
                break
            all_results.extend(batch)
            # Follow pagination URL SerpAPI provides
            next_url = data.get("serpapi_pagination", {}).get("next")
            if not next_url:
                break
            # Rebuild params from next URL (has correct start offset + ll embedded)
            qs = parse_qs(urlparse(next_url).query)
            params = {k: v[0] for k, v in qs.items()}
            params["api_key"] = api_key
            time.sleep(0.3)   # polite delay between pages
        except ValueError:
            raise
        except Exception:
            break

    return all_results


#  LOCATION FILTER  — drop results clearly outside the target city

def within_bbox(lat: float | None, lng: float | None, bbox: list | None) -> bool:
    """
    Returns True if point is within bounding box, or if we can't tell.
    bbox = [min_lat, max_lat, min_lng, max_lng]
    """
    if not bbox or lat is None or lng is None:
        return True   # can't filter, keep it
    min_lat, max_lat, min_lng, max_lng = bbox
    # Add a small buffer (20%) so we don't cut off suburb listings
    lat_buf = (max_lat - min_lat) * 0.2
    lng_buf = (max_lng - min_lng) * 0.2
    return (min_lat - lat_buf <= lat <= max_lat + lat_buf and
            min_lng - lng_buf <= lng <= max_lng + lng_buf)


#  RECORD BUILDER

def build_record(p: dict, keyword: str, location: str) -> dict:
    gps = p.get("gps_coordinates", {})
    lat = gps.get("latitude")
    lng = gps.get("longitude")

    # Hours: prefer operating_hours dict over plain hours string
    oh = p.get("operating_hours", {})
    if oh and isinstance(oh, dict):
        hours_str = " | ".join(f"{d}: {t}" for d, t in list(oh.items())[:3])
    else:
        hours_str = p.get("hours") or None

    place_id = p.get("place_id") or p.get("data_id", "")

    return {
        "place_id":        place_id,
        "name":            p.get("title"),
        "rating":          p.get("rating"),
        "reviews":         p.get("reviews"),
        "address":         p.get("address"),
        "phone":           p.get("phone"),
        "website":         p.get("website"),
        "category":        p.get("type"),
        "hours":           hours_str,
        "price_range":     p.get("price"),
        "lat":             lat,
        "lng":             lng,
        "maps_url":        (p.get("links") or {}).get("directions") or
                           (f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id else None),
        "thumbnail":       p.get("thumbnail"),
        "business_status": ("OPEN"   if str(p.get("open_state", "")).lower().startswith("open")  else
                            "CLOSED" if str(p.get("open_state", "")).lower().startswith("close") else ""),
        "_keyword":        keyword,
        "_location":       location,
    }


#  MAIN — one keyword × one location

def scrape_one(keyword: str, location: str, api_key: str,
               grid_n: int, radius_km: float, zoom: int,
               max_pages: int, stop_fn, progress_fn) -> tuple:
    """
    Full pipeline for one keyword+location:
      1. Geocode -> center + bounding box
      2. Build NxN grid
      3. For each cell: fetch all pages, collect raw results
      4. Deduplicate by place_id
      5. Filter by bounding box (drop results clearly outside the city)
      6. Return clean records
    """
    try:
        geo = geocode(location)
        if geo is None:
            raise ValueError(f"Could not geocode '{location}'. Try adding country, e.g. 'Auckland, New Zealand'.")

        center_lat, center_lng = geo["lat"], geo["lng"]
        bbox = geo.get("bbox")

        grid = make_grid(center_lat, center_lng, radius_km, grid_n)
        seen: dict[str, dict] = {}          # place_id -> record
        out_of_area = 0

        for idx, (lat, lng) in enumerate(grid):
            if stop_fn():
                break

            ll = f"@{lat},{lng},{zoom}z"
            # ── KEY: query = keyword only, location handled by GPS
            raw = fetch_all_pages(keyword, ll, api_key, max_pages)

            for p in raw:
                pid = p.get("place_id") or p.get("data_id") or ""
                if not pid:
                    # Use name+address as fallback key
                    pid = f"{p.get('title','')}|{p.get('address','')}"
                if pid in seen:
                    continue

                rec = build_record(p, keyword, location)

                # Location relevance filter — drop results clearly outside bbox
                if bbox and rec["lat"] is not None:
                    if not within_bbox(rec["lat"], rec["lng"], bbox):
                        out_of_area += 1
                        continue

                seen[pid] = rec

            progress_fn(idx + 1, len(grid), len(seen), out_of_area)
            time.sleep(0.1)

        return keyword, location, list(seen.values()), None, out_of_area

    except Exception as e:
        return keyword, location, [], str(e), 0


#  COLUMNS

COL_META = {
    "name":            "Business Name",
    "rating":          "Rating",
    "reviews":         "Review Count",
    "phone":           "Phone",
    "website":         "Website",
    "address":         "Address",
    "category":        "Category",
    "hours":           "Opening Hours",
    "price_range":     "Price Range",
    "lat":             "Latitude",
    "lng":             "Longitude",
    "maps_url":        "Google Maps URL",
    "thumbnail":       "Thumbnail URL",
    "business_status": "Status",
}


#  SIDEBAR

with st.sidebar:
    st.header("⚙️ Configuration")

    api_key = st.text_input("SerpAPI Key", type="password", placeholder="your-serpapi-key…")
    st.markdown("""<div class="api-note">
    <a href="https://serpapi.com/" target="_blank">serpapi.com</a> → Sign up → Dashboard → API Key<br>
    Free: 100 searches/month · Paid from $50/mo (~5,000 searches)
    </div>""", unsafe_allow_html=True)

    st.divider()
    st.subheader("Grid Strategy")

    st.markdown("""<div class="good-note">
    <b>How grid works:</b> City is geocoded (free, via OpenStreetMap) → split into NxN zones →
    each zone searched independently with GPS coordinates. <b>Query = keyword only</b>
    (no city name) so Google returns businesses near each GPS point, not text-matched noise.
    Results outside the city bounding box are automatically filtered out.
    </div>""", unsafe_allow_html=True)

    grid_n = st.select_slider("Grid density (N×N)", options=[1,2,3,4,5], value=3,
        help="1×1=single search. 3×3=9 cells. 5×5=25 cells. More cells = more unique results.")

    radius_km = st.slider("Search radius (km)", 2, 60, 15,
        help="Distance from city center the grid covers. 15km suits most cities.")

    zoom = st.slider("Zoom level per cell", 11, 16, 14,
        help="14z = neighbourhood. 12z = broader area. Higher zoom = more focused results per cell.")

    max_pages = st.slider("Pages per grid cell", 1, 5, 3,
        help="20 results/page × pages. 3 pages = up to 60 per cell.")

    cells       = grid_n ** 2
    max_per_kw  = cells * max_pages * 20
    api_calls   = cells * max_pages

    st.markdown(f"""<div class="warn-note">
    <b>Per keyword×location:</b><br>
    {cells} cells × {max_pages} pages × 20 results = <b>up to {max_per_kw:,} results</b><br>
    SerpAPI calls: <b>{api_calls}</b>
    </div>""", unsafe_allow_html=True)

    st.divider()
    st.subheader("Columns to Export")
    defaults = {"name","rating","reviews","phone","website","address","category"}
    selected_cols = [c for c,lbl in COL_META.items()
                     if st.checkbox(lbl, value=(c in defaults), key=f"col_{c}")]


#  MAIN Page

st.title("🗺️ Google Maps Business Scraper")
st.caption("Grid-based tiling · GPS-anchored queries · Location-filtered results · Powered by SerpAPI")

c1, c2 = st.columns(2)
with c1:
    st.subheader("🔍 Keywords")
    kw_raw    = st.text_area("One per line", value="watch store\nwatch shop\njewellery store",
                              height=150, label_visibility="collapsed")
    keywords  = [k.strip() for k in kw_raw.strip().splitlines() if k.strip()]
    st.caption(f"{len(keywords)} keyword(s)")
with c2:
    st.subheader("📍 Locations")
    loc_raw   = st.text_area("One per line", value="Auckland, New Zealand\nWellington, New Zealand",
                              height=150, label_visibility="collapsed")
    locations = [l.strip() for l in loc_raw.strip().splitlines() if l.strip()]
    st.caption(f"{len(locations)} location(s)")

pairs          = [(k, l) for k in keywords for l in locations]
total_calls    = len(pairs) * cells * max_pages
total_est      = len(pairs) * max_per_kw

st.info(
    f"**{len(pairs)} pairs** · {cells} cells × {max_pages} pages = "
    f"**{total_calls:,} SerpAPI calls** · Up to **{total_est:,} results** before dedup",
    icon="📊"
)

for k, v in [("running",False),("results",[]),("stop",False),("stats",{})]:
    if k not in st.session_state: st.session_state[k] = v

rb, sb, _ = st.columns([2,1,5])
with rb:
    run_btn = st.button("▶  Start Scraping", type="primary",
                        disabled=st.session_state.running or not api_key)
with sb:
    if st.button("⏹  Stop", disabled=not st.session_state.running):
        st.session_state.stop = True


# ── Run loop 

if run_btn and api_key and keywords and locations:
    st.session_state.update(running=True, stop=False, results=[], stats={})

    prog       = st.progress(0.0, text="Starting…")
    log_box    = st.empty()
    log_lines  = []
    done_pairs = 0
    total_oof  = 0   # out-of-area filtered

    def stop_fn():
        return st.session_state.stop

    for kw, loc in pairs:
        if st.session_state.stop:
            log_lines.append("⛔ Stopped by user")
            log_box.code("\n".join(log_lines[-30:]))
            break

        log_lines.append(f"🌐 Geocoding [{loc}]…")
        log_box.code("\n".join(log_lines[-30:]))

        def progress_fn(cell_i, total_cells, unique, oof, _kw=kw, _loc=loc):
            frac = (done_pairs + cell_i/total_cells) / len(pairs)
            prog.progress(min(frac, 1.0),
                text=f"[{_kw} | {_loc}] Cell {cell_i}/{total_cells} · "
                     f"{unique} unique · {oof} filtered out-of-area")

        kw_out, loc_out, records, err, oof = scrape_one(
            keyword=kw, location=loc, api_key=api_key,
            grid_n=grid_n, radius_km=radius_km, zoom=zoom,
            max_pages=max_pages, stop_fn=stop_fn, progress_fn=progress_fn,
        )
        done_pairs += 1
        total_oof  += oof

        if err:
            log_lines.append(f"❌ [{kw} | {loc}] {err}")
        else:
            st.session_state.results.extend(records)
            log_lines.append(
                f"✅ [{kw} | {loc}] → {len(records)} unique"
                + (f" ({oof} out-of-area removed)" if oof else "")
            )

        prog.progress(done_pairs/len(pairs),
            text=f"{done_pairs}/{len(pairs)} pairs · {len(st.session_state.results)} total")
        log_box.code("\n".join(log_lines[-30:]))

    st.session_state.running = False
    prog.progress(1.0, text=f"Done — {len(st.session_state.results)} records · {total_oof} out-of-area filtered")
    log_lines.append(f"🏁 Finished. {len(st.session_state.results)} records. {total_oof} filtered.")
    log_box.code("\n".join(log_lines[-30:]))


# ── Results ───────────────────────────────────────────────────────────────────

if st.session_state.results:
    st.divider()

    df = pd.DataFrame(st.session_state.results)

    # Global dedup across all keyword×location pairs
    if "place_id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["place_id"])
        dupes = before - len(df)
    else:
        dupes = 0

    # Build display columns
    ordered = [c for c in selected_cols if c in df.columns]
    meta    = [c for c in ["_keyword","_location","place_id"] if c in df.columns]
    df_view = df[ordered + meta].copy()

    rename  = {**{c: COL_META[c] for c in COL_META}, "_keyword":"Keyword",
               "_location":"Location", "place_id":"Place ID"}
    df_view.rename(columns=rename, inplace=True)

    st.subheader(f"📋 {len(df_view)} unique businesses" +
                 (f"  _(+{dupes} duplicates removed)_" if dupes else ""))

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Records",  len(df_view))
    m2.metric("Unique Names",   df_view["Business Name"].nunique() if "Business Name" in df_view else "–")
    m3.metric("With Phone",     int(df_view["Phone ☎️"].notna().sum())   if "Phone ☎️"   in df_view else "–")
    m4.metric("With Website",   int(df_view["Website 🌐"].notna().sum()) if "Website 🌐" in df_view else "–")

    # Filter / search
    q = st.text_input("🔎 Filter table", placeholder="Search name, address, category…")
    view = df_view[df_view.apply(
        lambda r: r.astype(str).str.contains(q, case=False, na=False).any(), axis=1
    )] if q else df_view
    if q: st.caption(f"{len(view)} matching records")

    st.dataframe(view, use_container_width=True, height=460)

    # Export
    st.subheader("⬇️ Export")
    e1, e2, e3 = st.columns(3)
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    with e1:
        buf = io.StringIO(); df_view.to_csv(buf, index=False)
        st.download_button("📄 CSV", buf.getvalue(), f"gmaps_{ts}.csv",
                           "text/csv", use_container_width=True)
    with e2:
        st.download_button("📦 JSON", df_view.to_json(orient="records", indent=2),
                           f"gmaps_{ts}.json", "application/json", use_container_width=True)
    with e3:
        xbuf = io.BytesIO(); df_view.to_excel(xbuf, index=False, engine="openpyxl")
        st.download_button("📊 Excel", xbuf.getvalue(), f"gmaps_{ts}.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)

    if st.button("🗑️ Clear results"):
        st.session_state.results = []
        st.rerun()

elif not st.session_state.running:
    st.markdown("""<div class="api-note">
    👆 <b>How to start:</b><br>
    1. Enter your SerpAPI key in the sidebar<br>
    2. Add keywords (one per line) — e.g. <i>watch store</i>, <i>jewellery shop</i><br>
    3. Add locations with country — e.g. <i>Auckland, New Zealand</i><br>
    4. Hit <b>▶ Start Scraping</b><br><br>
    <b>Tip:</b> Add country to locations for best geocoding accuracy.
    The grid tiles the city area and each cell is searched by GPS, not by text —
    so results are always geographically relevant.
    </div>""", unsafe_allow_html=True)
