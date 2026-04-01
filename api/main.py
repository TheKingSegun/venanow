"""
api/main.py

VenaNow FastAPI application.
All routes are prefixed with /api.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.routes import statements, dashboard, recommendations, chat, health

app = FastAPI(
    title="VenaNow Financial Intelligence API",
    description="Backend API for Nigerian personal finance tracking and AI-powered insights.",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

# CORS — allow frontend dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173", "https://venanow.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(statements.router,      prefix="/api/statements",      tags=["Statements"])
app.include_router(dashboard.router,       prefix="/api/dashboard",       tags=["Dashboard"])
app.include_router(recommendations.router, prefix="/api/recommendations", tags=["Recommendations"])
app.include_router(health.router,          prefix="/api/health-score",    tags=["Health Score"])
app.include_router(chat.router,            prefix="/api/chat",            tags=["AI Assistant"])


@app.get("/api/ping")
def ping():
    return {"status": "ok", "service": "VenaNow API v1.0"}


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"error": str(exc), "detail": "An unexpected error occurred."},
    )
