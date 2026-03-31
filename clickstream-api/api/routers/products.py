"""Product-related endpoints: viewers and engagement stats."""

from fastapi import APIRouter, Query
from services.delta_reader import read_delta_table

router = APIRouter()


@router.get("/{product_id}/viewers")
def get_product_viewers(product_id: int, limit: int = Query(default=20, le=100)):
    """Get users who viewed this product, ranked by view count."""
    df = read_delta_table("clickstream")
    if df is None or df.empty:
        return {"product_id": product_id, "viewers": [], "message": "No clickstream data available yet."}

    product_df = df[(df["product_id"] == product_id) & (df["event_type"] == "VIEW")]
    if product_df.empty:
        return {"product_id": product_id, "viewers": []}

    viewer_counts = (
        product_df.groupby("customer_id")
        .size()
        .reset_index(name="view_count")
        .sort_values("view_count", ascending=False)
        .head(limit)
    )

    # Join with customers for names
    customers_df = read_delta_table("customers")
    if customers_df is not None and not customers_df.empty:
        viewer_counts = viewer_counts.merge(
            customers_df[["customer_id", "first_name", "last_name"]],
            on="customer_id",
            how="left",
        )

    viewers = viewer_counts.to_dict(orient="records")
    return {"product_id": product_id, "total_viewers": len(viewers), "viewers": viewers}


@router.get("/{product_id}/engagement")
def get_product_engagement(product_id: int):
    """Get engagement breakdown for a product (views, cart adds, wishlists)."""
    df = read_delta_table("clickstream")
    if df is None or df.empty:
        return {"product_id": product_id, "engagement": {}, "message": "No clickstream data available yet."}

    product_df = df[df["product_id"] == product_id]
    if product_df.empty:
        return {"product_id": product_id, "engagement": {}}

    breakdown = product_df["event_type"].value_counts().to_dict()
    return {
        "product_id": product_id,
        "total_events": int(product_df.shape[0]),
        "unique_users": int(product_df["customer_id"].nunique()),
        "engagement": {k: int(v) for k, v in breakdown.items()},
    }
