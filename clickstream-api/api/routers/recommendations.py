"""Recommendation endpoint: content-based filtering from user interests."""

from fastapi import APIRouter, Query
from services.delta_reader import read_delta_table

router = APIRouter()


@router.get("/{user_id}")
def get_recommendations(user_id: int, limit: int = Query(default=10, le=50)):
    """Recommend products the user hasn't interacted with, from their top-interest categories.

    Algorithm:
    1. Get user's top interest categories (from batch-computed scores)
    2. Get products the user has already interacted with (from clickstream)
    3. Find products in top categories that the user hasn't seen
    4. Rank by category interest score
    """
    # Load interest scores
    cat_interests = read_delta_table("user_category_interests")
    if cat_interests is None or cat_interests.empty:
        return {"user_id": user_id, "recommendations": [],
                "message": "No interest data available yet. Run the interests batch job."}

    user_interests = (
        cat_interests[cat_interests["customer_id"] == user_id]
        .sort_values("score", ascending=False)
    )
    if user_interests.empty:
        return {"user_id": user_id, "recommendations": [],
                "message": "No interest data for this user."}

    top_categories = user_interests["category"].tolist()
    category_scores = dict(zip(user_interests["category"], user_interests["score"]))

    # Get products user already interacted with
    clickstream = read_delta_table("clickstream")
    seen_product_ids = set()
    if clickstream is not None and not clickstream.empty:
        user_clicks = clickstream[clickstream["customer_id"] == user_id]
        seen_product_ids = set(user_clicks["product_id"].dropna().astype(int).tolist())

    # Get all products in top categories
    products = read_delta_table("products")
    if products is None or products.empty:
        return {"user_id": user_id, "recommendations": [],
                "message": "No product data available."}

    candidates = products[products["category"].isin(top_categories)].copy()

    # Exclude already-seen products
    candidates = candidates[~candidates["product_id"].isin(seen_product_ids)]

    if candidates.empty:
        return {"user_id": user_id, "recommendations": [],
                "message": "User has seen all products in their interest categories."}

    # Score by category interest
    candidates["interest_score"] = candidates["category"].map(category_scores)
    candidates = candidates.sort_values("interest_score", ascending=False).head(limit)

    recommendations = candidates[
        ["product_id", "name", "category", "price", "interest_score"]
    ].to_dict(orient="records")

    return {
        "user_id": user_id,
        "top_categories": top_categories[:3],
        "count": len(recommendations),
        "recommendations": recommendations,
    }
