"""FastAPI query service for clickstream data and product recommendations."""

from fastapi import FastAPI
from routers import users, products, recommendations, dashboard

app = FastAPI(
    title="Clickstream & Recommendations API",
    description="Query clickstream data, user interests, and product recommendations from Delta Lake.",
    version="1.0.0",
)

app.include_router(users.router, prefix="/users", tags=["Users"])
app.include_router(products.router, prefix="/products", tags=["Products"])
app.include_router(recommendations.router, prefix="/recommendations", tags=["Recommendations"])
app.include_router(dashboard.router, prefix="/dashboard", tags=["Dashboard"])


@app.get("/health")
def health():
    return {"status": "ok"}
