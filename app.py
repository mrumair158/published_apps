import streamlit as st
import requests
import pandas as pd
import math
import io
import time
from datetime import datetime

st.set_page_config(page_title="Google Maps Scraper", page_icon="🗺️", layout="wide")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@400;500;600&display=swap');
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
    .block-container { padding-top: 1.5rem; max-width: 1400px; }
    div[data-testid="metric-container"] {
        background: #f0fdf4; border-radius: 10px; padding: 14px;
        border: 1px solid #bbf7d0;
    }
    .stDataFrame { font-family: 'DM Mono', monospace; font-size: 13px; }
    .api-note  { background:#eff6ff; border-left:4px solid #3b82f6; padding:10px 14px;
                 border-radius:4px; font-size:13px; margin-bottom:10px; }
    .warn-note { background:#fff7ed; border-left:4px solid #f97316; padding:10px 14px;
                 border-radius:4px; font-size:13px; margin-bottom:10px; }
</style>
""", unsafe_allow_html=True)

SERPAPI_ENDPOINT = "https://serpapi.com/search"


# ─────────────────────────── Geocoding via Nominatim (free) ──────────────────

def geocode_location(location: str) -> tuple[float, float] | None:
    """Free geocoding via OpenStreetMap Nominatim — no API key needed."""
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": location, "format": "json", "limit": 1},
            headers={"User-Agent": "gmaps-streamlit-scraper/1.0"},
            timeout=10,
        )
        data = resp.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception:
        pass
    return None


# ─────────────────────────── Geo-grid generator ──────────────────────────────

def generate_grid_points(center_lat: float, center_lng: float,
                          radius_km: float, grid_size: int) -> list[tuple[float, float]]:
    """NxN grid of (lat, lng) points covering ±radius_km around center."""
    lat_delta = radius_km / 111.0
    lng_delta = radius_km / (111.0 * math.cos(math.radians(center_lat)))
    steps = max(grid_size - 1, 1)
    points = []
    for i in range(grid_size):
        for j in range(grid_size):
            lat = center_lat - lat_delta + (2 * lat_delta * i / steps)
            lng = center_lng - lng_delta + (2 * lng_delta * j / steps)
            points.append((lat, lng))
    return points


# ─────────────────────────── SerpAPI search ──────────────────────────────────

def serpapi_maps_search(
    query: str,
    api_key: str,
    ll: str,
    max_pages: int = 5,
) -> list[dict]:
    """
    Fetch up to max_pages×20 results from SerpAPI Google Maps.
    Paginates via the `start` parameter (0, 20, 40 … up to 100).
    ll format: "@lat,lng,14z"
    """
    results = []
    for page in range(max_pages):
        start = page * 20
        params = {
            "engine":  "google_maps",
            "q":       query,
            "ll":      ll,
            "type":    "search",
            "start":   start,
            "hl":      "en",
            "api_key": api_key,
        }
        resp = requests.get(SERPAPI_ENDPOINT, params=params, timeout=30)
        if resp.status_code != 200:
            raise ValueError(f"SerpAPI HTTP {resp.status_code}: {resp.text[:300]}")

        data = resp.json()

        if "error" in data:
            raise ValueError(f"SerpAPI error: {data['error']}")

        local_results = data.get("local_results", [])
        if not local_results:
            break  # No more results on this page

        results.extend(local_results)

        # Stop early if SerpAPI says there's no next page
        if "next" not in data.get("serpapi_pagination", {}):
            break

    return results


# ─────────────────────────── Record builder ──────────────────────────────────

def record_from_serpapi(place: dict, keyword: str, location: str) -> dict:
    """Flatten a SerpAPI local_results item into a clean record."""
    gps = place.get("gps_coordinates", {})
    operating = place.get("operating_hours", {})
    if operating:
        hours_str = " | ".join(f"{d}: {t}" for d, t in operating.items())
    else:
        hours_str = place.get("hours") or None

    return {
        "place_id":        place.get("place_id") or place.get("data_id", ""),
        "name":            place.get("title"),
        "rating":          place.get("rating"),
        "reviews":         place.get("reviews"),
        "address":         place.get("address"),
        "phone":           place.get("phone"),
        "website":         place.get("website"),
        "category":        place.get("type"),
        "hours":           hours_str,
        "price_range":     place.get("price"),
        "lat":             gps.get("latitude"),
        "lng":             gps.get("longitude"),
        "maps_url":        (place.get("links") or {}).get("directions") or
                           (f"https://www.google.com/maps/place/?q=place_id:{place.get('place_id','')}"
                            if place.get("place_id") else None),
        "thumbnail":       place.get("thumbnail"),
        "business_status": "OPEN"   if (place.get("open_state") or "").lower().startswith("open") else
                           ("CLOSED" if place.get("open_state") else ""),
        "_keyword":        keyword,
        "_location":       location,
    }


# ─────────────────────────── Main worker ─────────────────────────────────────

def run_query_with_grid(
    keyword: str,
    location: str,
    api_key: str,
    radius_km: float,
    grid_size: int,
    zoom: int,
    max_pages: int,
    stop_flag,
    progress_cb,
) -> tuple[str, str, list[dict], str | None]:
    """
    1. Geocode location → center lat/lng (free, via Nominatim/OSM)
    2. Generate NxN grid of GPS points
    3. For each point, run SerpAPI search biased to that point + paginate
    4. Deduplicate everything by place_id
    """
    try:
        center = geocode_location(location)

        if center is None:
            # Fallback: no GPS bias, just keyword + location text
            points = [(None, None)]
        else:
            points = generate_grid_points(center[0], center[1], radius_km, grid_size)

        seen: dict[str, dict] = {}

        for idx, point in enumerate(points):
            if stop_flag():
                break

            if point[0] is None:
                ll = "@0,0,2z"
                q  = f"{keyword} {location}"
            else:
                ll = f"@{point[0]},{point[1]},{zoom}z"
                q  = keyword

            try:
                raw = serpapi_maps_search(q, api_key, ll, max_pages=max_pages)
                for p in raw:
                    pid = p.get("place_id") or p.get("data_id") or p.get("title", "")
                    if pid and pid not in seen:
                        seen[pid] = record_from_serpapi(p, keyword, location)
            except Exception as e:
                err_str = str(e)
                # Bubble up fatal errors (bad key / quota exhausted)
                if any(x in err_str for x in ["Invalid API key", "Your account", "quota", "out of searches"]):
                    raise
                # Otherwise skip this grid cell silently

            progress_cb(idx + 1, len(points), len(seen))

        return keyword, location, list(seen.values()), None

    except Exception as e:
        return keyword, location, [], str(e)


# ─────────────────────────── Column meta ─────────────────────────────────────

col_meta = {
    "name":            "Business Name",
    "rating":          "Rating ⭐",
    "reviews":         "Review Count",
    "phone":           "Phone ☎️",
    "website":         "Website 🌐",
    "address":         "Address 📍",
    "category":        "Category 🏷️",
    "hours":           "Opening Hours 🕐",
    "price_range":     "Price Range 💰",
    "lat":             "Latitude",
    "lng":             "Longitude",
    "maps_url":        "Google Maps URL",
    "thumbnail":       "Thumbnail URL",
    "business_status": "Business Status",
}


# ─────────────────────────── Sidebar ─────────────────────────────────────────

with st.sidebar:
    st.header("⚙️ Configuration")

    api_key = st.text_input(
        "SerpAPI Key",
        type="password",
        placeholder="your-serpapi-key…",
        help="Get your key at serpapi.com"
    )

    st.markdown("""
    <div class="api-note">
    🔑 <b>Get your SerpAPI key:</b><br>
    1. <a href="https://serpapi.com/" target="_blank">serpapi.com</a> → Sign up<br>
    2. Dashboard → API Key<br>
    Free plan: 100 searches/month<br>
    Paid plans from $50/mo (~5,000 searches)
    </div>
    """, unsafe_allow_html=True)

    st.divider()
    st.subheader("🗂️ Grid Strategy")

    st.markdown("""
    <div class="warn-note">
    ⚙️ <b>How it works:</b> Each location is geocoded (free via OpenStreetMap),
    divided into an NxN grid, and searched at each grid point with up to
    5 pages × 20 results = 100 per cell. A 3×3 grid gives up to
    <b>900 unique results</b> per keyword+location pair.
    </div>
    """, unsafe_allow_html=True)

    grid_size = st.select_slider(
        "Grid density (NxN)",
        options=[1, 2, 3, 4, 5],
        value=3,
        help="1×1 = plain search (~100 results). 5×5 = 25 cells (~2,500 results)."
    )
    grid_labels = {1: "1×1 (~100)", 2: "2×2 (~400)", 3: "3×3 (~900)", 4: "4×4 (~1,600)", 5: "5×5 (~2,500)"}
    st.caption(f"**{grid_labels[grid_size]}** results per keyword+location (before dedup)")

    radius_km = st.slider("Coverage radius (km)", 1, 50, 10,
        help="How far from the city center to cover. 10km suits most cities.")

    zoom = st.slider("Map zoom level", 10, 16, 14,
        help="14z = neighbourhood level. Lower = wider area per cell.")

    max_pages = st.slider("Pages per grid cell", 1, 5, 5,
        help="20 results per page. 5 pages = up to 100 results per cell.")

    st.caption(
        f"Max per cell: **{max_pages * 20}** · "
        f"Grid cells: **{grid_size**2}** · "
        f"Max per pair: **~{grid_size**2 * max_pages * 20:,}**"
    )

    st.divider()
    st.subheader("Columns to Export")

    defaults = {"name", "rating", "reviews", "phone", "website", "address", "category"}
    selected_cols = [c for c, lbl in col_meta.items()
                     if st.checkbox(lbl, value=(c in defaults), key=f"col_{c}")]


# ─────────────────────────── Main ────────────────────────────────────────────

st.title("🗺️ Google Maps Business Scraper")
st.caption("Powered by **SerpAPI** · Grid-based tiling to bypass the per-search result limit")

col1, col2 = st.columns(2)
with col1:
    st.subheader("🔍 Keywords")
    kw_raw = st.text_area("One per line", value="watch store\nwatch shop\njewellery store",
                           height=150, label_visibility="collapsed")
    keywords  = [k.strip() for k in kw_raw.strip().splitlines() if k.strip()]
    st.caption(f"{len(keywords)} keyword(s)")

with col2:
    st.subheader("📍 Locations")
    loc_raw = st.text_area("One per line", value="Auckland\nWellington", height=150,
                            label_visibility="collapsed")
    locations = [l.strip() for l in loc_raw.strip().splitlines() if l.strip()]
    st.caption(f"{len(locations)} location(s)")

total_pairs    = len(keywords) * len(locations)
cells          = grid_size ** 2
total_api_calls = total_pairs * cells * max_pages
est_results    = total_api_calls * 20

st.info(
    f"**{total_pairs} keyword×location pairs** · "
    f"{cells} grid cells × {max_pages} pages = **~{total_api_calls:,} SerpAPI calls** · "
    f"Up to **~{est_results:,} results** before dedup",
    icon="📊"
)

# ─── Session state ────────────────────────────────────────────────────────────

for k, v in [("running", False), ("results", []), ("stop", False)]:
    if k not in st.session_state:
        st.session_state[k] = v

rb, sb, _ = st.columns([2, 1, 5])
with rb:
    run_btn = st.button("▶  Start Scraping", type="primary",
                        disabled=st.session_state.running or not api_key)
with sb:
    if st.button("⏹  Stop", disabled=not st.session_state.running):
        st.session_state.stop = True


# ─── Run ─────────────────────────────────────────────────────────────────────

if run_btn and api_key and keywords and locations:
    st.session_state.running = True
    st.session_state.stop    = False
    st.session_state.results = []

    prog      = st.progress(0, text="Starting…")
    logs      = st.empty()
    log_lines: list[str] = []
    pairs     = [(k, l) for k in keywords for l in locations]
    done      = 0

    def stop_flag():
        return st.session_state.stop

    for kw, loc in pairs:
        if st.session_state.stop:
            log_lines.append("⛔ Stopped by user")
            logs.code("\n".join(log_lines[-25:]), language=None)
            break

        log_lines.append(f"🔄 [{kw}] in [{loc}] — {grid_size}×{grid_size} grid, {max_pages} pages/cell…")
        logs.code("\n".join(log_lines[-25:]), language=None)

        def progress_cb(cell_idx, total_cells, unique_so_far, _kw=kw, _loc=loc):
            pair_pct = cell_idx / total_cells
            overall  = (done + pair_pct) / len(pairs)
            prog.progress(
                min(overall, 1.0),
                text=f"[{_kw} in {_loc}] Cell {cell_idx}/{total_cells} · {unique_so_far} unique so far"
            )

        kw_out, loc_out, records, err = run_query_with_grid(
            keyword     = kw,
            location    = loc,
            api_key     = api_key,
            radius_km   = radius_km,
            grid_size   = grid_size,
            zoom        = zoom,
            max_pages   = max_pages,
            stop_flag   = stop_flag,
            progress_cb = progress_cb,
        )

        done += 1

        if err:
            log_lines.append(f"❌ [{kw} in {loc}] Error: {err}")
        else:
            st.session_state.results.extend(records)
            log_lines.append(f"✅ [{kw} in {loc}] → {len(records)} unique businesses found")

        prog.progress(done / len(pairs),
                      text=f"{done}/{len(pairs)} pairs · {len(st.session_state.results)} total records")
        logs.code("\n".join(log_lines[-25:]), language=None)

    st.session_state.running = False
    prog.progress(1.0, text=f"Done — {len(st.session_state.results)} total records")
    log_lines.append(f"🏁 Finished. {len(st.session_state.results)} total records.")
    logs.code("\n".join(log_lines[-25:]), language=None)


# ─── Results ─────────────────────────────────────────────────────────────────

if st.session_state.results:
    st.divider()

    df = pd.DataFrame(st.session_state.results)

    # Global dedup across all pairs
    if "place_id" in df.columns:
        df = df.drop_duplicates(subset=["place_id"])

    ordered = [c for c in selected_cols if c in df.columns]
    meta    = [c for c in ["_keyword", "_location", "place_id"] if c in df.columns]
    df = df[ordered + meta]

    display_names = {c: col_meta.get(c, c) for c in col_meta}
    display_names.update({"_keyword": "Keyword", "_location": "Location", "place_id": "Place ID"})
    df.rename(columns=display_names, inplace=True)

    st.subheader(f"📋 Results — {len(df)} unique businesses")

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Records",  len(df))
    m2.metric("Unique Names",   df["Business Name"].nunique() if "Business Name" in df else "–")
    m3.metric("With Phone",     int(df["Phone ☎️"].notna().sum())   if "Phone ☎️"   in df else "–")
    m4.metric("With Website",   int(df["Website 🌐"].notna().sum()) if "Website 🌐" in df else "–")

    q = st.text_input("🔎 Filter table", placeholder="Type to search any column…")
    view = df[df.apply(lambda r: r.astype(str).str.contains(q, case=False, na=False).any(), axis=1)] if q else df
    if q:
        st.caption(f"{len(view)} matching records")

    st.dataframe(view, use_container_width=True, height=450)

    st.subheader("⬇️ Export")
    e1, e2, e3 = st.columns(3)
    ts = datetime.now().strftime("%Y%m%d_%H%M")

    with e1:
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        st.download_button("📄 CSV", buf.getvalue(),
                           f"gmaps_{ts}.csv", "text/csv", use_container_width=True)
    with e2:
        st.download_button("📦 JSON", df.to_json(orient="records", indent=2),
                           f"gmaps_{ts}.json", "application/json", use_container_width=True)
    with e3:
        xbuf = io.BytesIO()
        df.to_excel(xbuf, index=False, engine="openpyxl")
        st.download_button("📊 Excel", xbuf.getvalue(),
                           f"gmaps_{ts}.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)

    if st.button("🗑️ Clear results"):
        st.session_state.results = []
        st.rerun()

else:
    if not st.session_state.running:
        st.markdown("""
        <div class="api-note">
        👆 <b>How to start:</b> Enter your SerpAPI key in the sidebar,
        add keywords &amp; locations, then hit <b>Start Scraping</b>.<br><br>
        Locations are geocoded for free via OpenStreetMap — no extra API key needed.
        The grid tiles the area and searches each sub-zone to collect far more results
        than a single search allows.
        </div>
        """, unsafe_allow_html=True)