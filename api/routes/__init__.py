"""
api/routes/dashboard.py — Dashboard summary endpoint
api/routes/recommendations.py — Recommendations endpoint
api/routes/health.py — Health score endpoint
api/routes/chat.py — AI financial assistant endpoint
"""

# ── dashboard.py ──────────────────────────────────────────────────────────────
from fastapi import APIRouter

router_dashboard = APIRouter()

@router_dashboard.get("/{user_id}")
async def get_dashboard(user_id: str, month: str = None):
    """
    Returns all data needed to render the dashboard for a user.
    In production: queries financial_health_snapshots + transactions.
    """
    return {
        "user_id": user_id,
        "month": month or "latest",
        "note": "Wire to DB — query monthly_summary view + category_spend view",
    }


# ── recommendations.py ────────────────────────────────────────────────────────
router_recommendations = APIRouter()

@router_recommendations.get("/{user_id}")
async def get_recommendations(user_id: str):
    """Return stored recommendations for a user."""
    return {
        "user_id": user_id,
        "note": "Wire to DB — query recommendations table ordered by priority",
    }

@router_recommendations.delete("/{user_id}/{recommendation_id}")
async def dismiss_recommendation(user_id: str, recommendation_id: str):
    """Dismiss a recommendation (sets is_dismissed=True)."""
    return {"dismissed": recommendation_id}


# ── health.py ─────────────────────────────────────────────────────────────────
router_health = APIRouter()

@router_health.get("/{user_id}")
async def get_health_score(user_id: str):
    """Return latest health score snapshot for a user."""
    return {
        "user_id": user_id,
        "note": "Wire to DB — query financial_health_snapshots latest row",
    }


# ── chat.py ───────────────────────────────────────────────────────────────────
import os
from typing import Optional
from pydantic import BaseModel

router_chat = APIRouter()


class ChatRequest(BaseModel):
    user_id: str
    message: str
    context: Optional[dict] = None  # Pass dashboard data for richer answers


FINANCIAL_SYSTEM_PROMPT = """
You are a smart, friendly Nigerian personal finance assistant called VenaBot.
You speak plain English with occasional Pidgin flair when appropriate.
You have access to the user's financial data for the current period.

Rules:
- Always give specific, actionable advice
- Quote amounts in Naira (₦) when relevant
- Reference the user's actual data when answering
- Be encouraging, not judgmental
- Keep answers under 120 words unless a detailed breakdown is requested
- Never give generic advice if you have data to be specific

User financial context will be provided in the system message.
""".strip()


@router_chat.post("")
async def chat(req: ChatRequest):
    """
    AI financial assistant endpoint.
    Uses Anthropic Claude API when key is available, falls back to rule-based responses.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")

    if api_key:
        return await _llm_chat(req, api_key)
    else:
        return _rule_based_chat(req)


async def _llm_chat(req: ChatRequest, api_key: str) -> dict:
    """Call Anthropic Claude API for intelligent financial Q&A."""
    import httpx

    context_str = ""
    if req.context:
        ctx = req.context
        summary = ctx.get("summary", {})
        context_str = f"""
User's current month data:
- Total Income: ₦{summary.get('total_income', 0):,.0f}
- Total Expenses: ₦{summary.get('total_expenses', 0):,.0f}
- Net Savings: ₦{summary.get('net_savings', 0):,.0f}
- Savings Rate: {summary.get('savings_rate_pct', 0):.1f}%
- Top expense categories: {ctx.get('category_spend', {})}
""".strip()

    messages = [{"role": "user", "content": req.message}]
    system = FINANCIAL_SYSTEM_PROMPT
    if context_str:
        system += f"\n\nUser Financial Data:\n{context_str}"

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 300,
                "system": system,
                "messages": messages,
            },
        )
        data = response.json()
        reply = data["content"][0]["text"] if data.get("content") else "I couldn't process that. Try again."

    return {"reply": reply, "source": "llm"}


def _rule_based_chat(req: ChatRequest) -> dict:
    """Fallback rule-based chat when no API key is configured."""
    msg = req.message.lower()
    ctx = req.context or {}
    summary = ctx.get("summary", {})

    income   = summary.get("total_income", 0)
    expenses = summary.get("total_expenses", 0)
    savings  = summary.get("net_savings", 0)
    sav_rate = summary.get("savings_rate_pct", 0)

    if any(w in msg for w in ["overspend", "spend too much", "where did my money go"]):
        reply = (
            f"Your top expense categories this month drove most of the spending. "
            f"With {sav_rate:.1f}% savings rate, there's room to tighten food and subscription costs. "
            f"Cutting food by 15% alone could save ₦{expenses * 0.15 * 0.23:,.0f}."
        )
    elif any(w in msg for w in ["save", "how to save", "save more"]):
        gap = income * 0.30 - savings
        reply = (
            f"To hit a 30% savings rate, you'd save ₦{income * 0.30:,.0f}/month. "
            f"You're currently at ₦{savings:,.0f}. "
            f"Close the ₦{max(0, gap):,.0f} gap by cutting subscriptions and reducing eating out."
        )
    elif any(w in msg for w in ["health", "score", "how am i doing"]):
        reply = (
            f"You're saving {sav_rate:.1f}% of your income. "
            f"Your emergency fund and subscription costs are the areas pulling your health score down. "
            f"Prioritize building 6 months of emergency savings."
        )
    elif any(w in msg for w in ["income", "earn", "salary"]):
        reply = f"Your total income this period was ₦{income:,.0f}. Consider diversifying income sources to improve stability."
    else:
        reply = (
            f"This month: income ₦{income:,.0f}, expenses ₦{expenses:,.0f}, savings ₦{savings:,.0f} ({sav_rate:.1f}%). "
            f"Ask me about overspending, how to save more, or your health score for specific advice."
        )

    return {"reply": reply, "source": "rule"}


# ── Wire routers to main app ──────────────────────────────────────────────────
# These are imported in api/main.py as individual modules.
# Keeping them all here for conciseness; in production split into separate files.

dashboard       = type("Module", (), {"router": router_dashboard})()
recommendations = type("Module", (), {"router": router_recommendations})()
health          = type("Module", (), {"router": router_health})()
chat            = type("Module", (), {"router": router_chat})()
