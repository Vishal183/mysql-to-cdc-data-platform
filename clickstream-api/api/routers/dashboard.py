"""Dashboard endpoints: aggregated analytics + HTML dashboard page."""

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse
from services.delta_reader import read_delta_table

router = APIRouter()


@router.get("/top-products")
def top_products(limit: int = Query(default=10, le=50), event_type: str = Query(default=None)):
    """Most interacted products ranked by event count."""
    df = read_delta_table("clickstream")
    if df is None or df.empty:
        return {"products": [], "message": "No clickstream data yet."}

    filtered = df[df["product_id"].notna()].copy()
    if event_type:
        filtered = filtered[filtered["event_type"] == event_type]

    product_counts = (
        filtered.groupby("product_id")
        .agg(total_events=("event_id", "count"), unique_users=("customer_id", "nunique"))
        .reset_index()
        .sort_values("total_events", ascending=False)
        .head(limit)
    )

    products = read_delta_table("products")
    if products is not None and not products.empty:
        product_counts = product_counts.merge(
            products[["product_id", "name", "category", "price"]],
            on="product_id", how="left",
        )

    return {"count": len(product_counts), "products": product_counts.to_dict(orient="records")}


@router.get("/event-breakdown")
def event_breakdown():
    """Overall event type distribution."""
    df = read_delta_table("clickstream")
    if df is None or df.empty:
        return {"breakdown": {}, "total": 0}

    breakdown = df["event_type"].value_counts().to_dict()
    return {
        "total": int(df.shape[0]),
        "unique_users": int(df["customer_id"].nunique()),
        "unique_products": int(df["product_id"].dropna().nunique()),
        "breakdown": {k: int(v) for k, v in breakdown.items()},
    }


@router.get("/top-categories")
def top_categories():
    """Most engaged categories by total event count."""
    df = read_delta_table("clickstream")
    products = read_delta_table("products")
    if df is None or df.empty or products is None:
        return {"categories": []}

    merged = df[df["product_id"].notna()].merge(
        products[["product_id", "category"]], on="product_id", how="left"
    )
    cat_counts = (
        merged.groupby("category")
        .agg(total_events=("event_id", "count"), unique_users=("customer_id", "nunique"))
        .reset_index()
        .sort_values("total_events", ascending=False)
    )
    return {"categories": cat_counts.to_dict(orient="records")}


@router.get("/activity-timeline")
def activity_timeline():
    """Event counts over time (grouped by minute)."""
    df = read_delta_table("clickstream")
    if df is None or df.empty:
        return {"timeline": []}

    df["created_at"] = df["created_at"].astype(str)
    # Group by minute
    df["minute"] = df["created_at"].str[:16]  # "2026-03-30T20:10"
    timeline = (
        df.groupby("minute")
        .agg(events=("event_id", "count"))
        .reset_index()
        .sort_values("minute")
    )
    return {"timeline": timeline.to_dict(orient="records")}


@router.get("", response_class=HTMLResponse)
def dashboard_page():
    """Interactive HTML dashboard with Plotly.js charts."""
    return DASHBOARD_HTML


DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Clickstream Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f1117; color: #e0e0e0; }
        .header { background: #1a1d27; padding: 20px 40px; border-bottom: 1px solid #2a2d37; }
        .header h1 { font-size: 24px; font-weight: 600; }
        .header p { color: #888; margin-top: 4px; font-size: 14px; }
        .stats-bar { display: flex; gap: 20px; padding: 20px 40px; }
        .stat-card { background: #1a1d27; border-radius: 8px; padding: 20px; flex: 1; border: 1px solid #2a2d37; }
        .stat-card .label { font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 1px; }
        .stat-card .value { font-size: 32px; font-weight: 700; margin-top: 4px; color: #60a5fa; }
        .charts { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; padding: 20px 40px; }
        .chart-card { background: #1a1d27; border-radius: 8px; padding: 20px; border: 1px solid #2a2d37; }
        .chart-card h3 { font-size: 16px; margin-bottom: 12px; color: #ccc; }
        .full-width { grid-column: 1 / -1; }
        table { width: 100%; border-collapse: collapse; margin-top: 12px; }
        th, td { text-align: left; padding: 10px 12px; border-bottom: 1px solid #2a2d37; font-size: 14px; }
        th { color: #888; font-weight: 500; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; }
        td { color: #e0e0e0; }
        .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 12px; }
        .badge-elec { background: #1e3a5f; color: #60a5fa; }
        .badge-cloth { background: #3b1f4a; color: #c084fc; }
        .badge-home { background: #1a3a2a; color: #4ade80; }
        .badge-sports { background: #3a2a1a; color: #fb923c; }
        .loading { color: #666; font-style: italic; padding: 40px; text-align: center; }
        .refresh-btn { float: right; background: #2563eb; color: white; border: none; padding: 8px 16px; border-radius: 6px; cursor: pointer; font-size: 13px; }
        .refresh-btn:hover { background: #1d4ed8; }
    </style>
</head>
<body>
    <div class="header">
        <button class="refresh-btn" onclick="loadAll()">Refresh Data</button>
        <h1>Clickstream Dashboard</h1>
        <p>Real-time analytics from CDC pipeline &rarr; Delta Lake</p>
    </div>

    <div class="stats-bar">
        <div class="stat-card"><div class="label">Total Events</div><div class="value" id="stat-events">-</div></div>
        <div class="stat-card"><div class="label">Unique Users</div><div class="value" id="stat-users">-</div></div>
        <div class="stat-card"><div class="label">Products Tracked</div><div class="value" id="stat-products">-</div></div>
        <div class="stat-card"><div class="label">Conversion Rate</div><div class="value" id="stat-conversion">-</div></div>
    </div>

    <div class="charts">
        <div class="chart-card">
            <h3>Top Products by Clicks</h3>
            <div id="chart-top-products" class="loading">Loading...</div>
        </div>
        <div class="chart-card">
            <h3>Event Type Distribution</h3>
            <div id="chart-events" class="loading">Loading...</div>
        </div>
        <div class="chart-card">
            <h3>Engagement by Category</h3>
            <div id="chart-categories" class="loading">Loading...</div>
        </div>
        <div class="chart-card">
            <h3>Activity Timeline</h3>
            <div id="chart-timeline" class="loading">Loading...</div>
        </div>
        <div class="chart-card full-width">
            <h3>Top 15 Most Clicked Products</h3>
            <div id="table-products" class="loading">Loading...</div>
        </div>
    </div>

    <script>
    const DARK_LAYOUT = {
        paper_bgcolor: '#1a1d27', plot_bgcolor: '#1a1d27',
        font: { color: '#ccc', size: 12 },
        margin: { t: 10, b: 40, l: 50, r: 20 },
        xaxis: { gridcolor: '#2a2d37' }, yaxis: { gridcolor: '#2a2d37' },
    };
    const COLORS = ['#60a5fa','#c084fc','#4ade80','#fb923c','#f472b6','#facc15','#34d399','#f87171'];
    const CAT_COLORS = { Electronics: '#60a5fa', Clothing: '#c084fc', Home: '#4ade80', Sports: '#fb923c' };

    async function fetchJSON(url) {
        const r = await fetch(url);
        return r.json();
    }

    function badgeFor(cat) {
        const cls = { Electronics:'badge-elec', Clothing:'badge-cloth', Home:'badge-home', Sports:'badge-sports' }[cat] || '';
        return `<span class="badge ${cls}">${cat}</span>`;
    }

    async function loadStats() {
        const d = await fetchJSON('/dashboard/event-breakdown');
        document.getElementById('stat-events').textContent = (d.total || 0).toLocaleString();
        document.getElementById('stat-users').textContent = (d.unique_users || 0).toLocaleString();
        document.getElementById('stat-products').textContent = (d.unique_products || 0).toLocaleString();
        const views = d.breakdown?.VIEW || 0;
        const carts = d.breakdown?.ADD_TO_CART || 0;
        const rate = views > 0 ? ((carts / views) * 100).toFixed(1) + '%' : '-';
        document.getElementById('stat-conversion').textContent = rate;
    }

    async function loadTopProducts() {
        const d = await fetchJSON('/dashboard/top-products?limit=15');
        if (!d.products?.length) { document.getElementById('chart-top-products').textContent = 'No data'; return; }
        const names = d.products.map(p => p.name || `Product ${p.product_id}`).reverse();
        const counts = d.products.map(p => p.total_events).reverse();
        const colors = d.products.map(p => CAT_COLORS[p.category] || '#60a5fa').reverse();
        Plotly.newPlot('chart-top-products', [{
            type: 'bar', orientation: 'h', x: counts, y: names,
            marker: { color: colors }, text: counts, textposition: 'outside',
        }], { ...DARK_LAYOUT, height: 350, margin: { ...DARK_LAYOUT.margin, l: 200 }, yaxis: { ...DARK_LAYOUT.yaxis, automargin: true } }, { responsive: true });

        // Table
        let html = '<table><thead><tr><th>#</th><th>Product</th><th>Category</th><th>Price</th><th>Events</th><th>Unique Users</th></tr></thead><tbody>';
        d.products.forEach((p, i) => {
            html += `<tr><td>${i+1}</td><td>${p.name||'-'}</td><td>${badgeFor(p.category||'-')}</td><td>$${(p.price||0).toFixed(2)}</td><td>${p.total_events}</td><td>${p.unique_users}</td></tr>`;
        });
        html += '</tbody></table>';
        document.getElementById('table-products').innerHTML = html;
    }

    async function loadEventBreakdown() {
        const d = await fetchJSON('/dashboard/event-breakdown');
        if (!d.breakdown || !Object.keys(d.breakdown).length) { document.getElementById('chart-events').textContent = 'No data'; return; }
        const labels = Object.keys(d.breakdown);
        const values = Object.values(d.breakdown);
        Plotly.newPlot('chart-events', [{
            type: 'pie', labels, values, hole: 0.45,
            marker: { colors: COLORS },
            textinfo: 'label+percent', textfont: { color: '#fff' },
        }], { ...DARK_LAYOUT, height: 300, showlegend: false }, { responsive: true });
    }

    async function loadCategories() {
        const d = await fetchJSON('/dashboard/top-categories');
        if (!d.categories?.length) { document.getElementById('chart-categories').textContent = 'No data'; return; }
        const cats = d.categories.map(c => c.category);
        const events = d.categories.map(c => c.total_events);
        const users = d.categories.map(c => c.unique_users);
        const colors = cats.map(c => CAT_COLORS[c] || '#60a5fa');
        Plotly.newPlot('chart-categories', [
            { type: 'bar', name: 'Events', x: cats, y: events, marker: { color: colors } },
            { type: 'bar', name: 'Unique Users', x: cats, y: users, marker: { color: colors, opacity: 0.5 } },
        ], { ...DARK_LAYOUT, height: 300, barmode: 'group', showlegend: true, legend: { font: { color: '#888' } } }, { responsive: true });
    }

    async function loadTimeline() {
        const d = await fetchJSON('/dashboard/activity-timeline');
        if (!d.timeline?.length) { document.getElementById('chart-timeline').textContent = 'No data'; return; }
        const times = d.timeline.map(t => t.minute);
        const events = d.timeline.map(t => t.events);
        Plotly.newPlot('chart-timeline', [{
            type: 'scatter', mode: 'lines+markers', x: times, y: events,
            line: { color: '#60a5fa', width: 2 }, marker: { size: 4 },
            fill: 'tozeroy', fillcolor: 'rgba(96,165,250,0.1)',
        }], { ...DARK_LAYOUT, height: 300 }, { responsive: true });
    }

    function loadAll() {
        loadStats();
        loadTopProducts();
        loadEventBreakdown();
        loadCategories();
        loadTimeline();
    }

    loadAll();
    setInterval(loadAll, 30000);
    </script>
</body>
</html>
"""
