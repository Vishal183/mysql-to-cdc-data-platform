"""User-related endpoints: interests and clickstream history."""

from fastapi import APIRouter, Query
from services.delta_reader import read_delta_table

router = APIRouter()


@router.get("/{user_id}/interests")
def get_user_interests(user_id: int):
    """Get top interest categories for a user, sorted by score."""
    df = read_delta_table("user_category_interests")
    if df is None or df.empty:
        return {"user_id": user_id, "interests": [], "message": "No interest data available yet. Run the interests batch job."}

    user_df = df[df["customer_id"] == user_id].sort_values("score", ascending=False)
    interests = user_df[["category", "score"]].to_dict(orient="records")
    return {"user_id": user_id, "interests": interests}


@router.get("/{user_id}/clickstream")
def get_user_clickstream(user_id: int, limit: int = Query(default=50, le=200)):
    """Get recent clickstream events for a user."""
    df = read_delta_table("clickstream")
    if df is None or df.empty:
        return {"user_id": user_id, "events": [], "message": "No clickstream data available yet."}

    user_df = (
        df[df["customer_id"] == user_id]
        .sort_values("created_at", ascending=False)
        .head(limit)
    )

    # Join with products for product names
    products_df = read_delta_table("products")
    if products_df is not None and not products_df.empty:
        user_df = user_df.merge(
            products_df[["product_id", "name", "category"]],
            on="product_id",
            how="left",
        )

    events = user_df.fillna("").to_dict(orient="records")
    return {"user_id": user_id, "count": len(events), "events": events}
