"""
api/main.py — VenaNow FastAPI application.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from api.routes import statements, dashboard, recommendations, chat, health, manual_entries, jobs

app = FastAPI(title="VenaNow API", version="1.0.0", docs_url="/api/docs", redoc_url="/api/redoc")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=False, allow_methods=["*"], allow_headers=["*"])

app.include_router(statements.router,      prefix="/api/statements",      tags=["Statements"])
app.include_router(dashboard.router,       prefix="/api/dashboard",       tags=["Dashboard"])
app.include_router(recommendations.router, prefix="/api/recommendations", tags=["Recommendations"])
app.include_router(health.router,          prefix="/api/health-score",    tags=["Health Score"])
app.include_router(chat.router,            prefix="/api/chat",            tags=["AI Assistant"])
app.include_router(manual_entries.router,  prefix="/api/manual",          tags=["Manual Entries"])
app.include_router(jobs.router,            prefix="/api/jobs",            tags=["Jobs"])

@app.get("/api/ping")
def ping():
    return {"status": "ok", "service": "VenaNow API v1.0"}

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return JSONResponse(status_code=500, content={"error": str(exc), "detail": "An unexpected error occurred."})
