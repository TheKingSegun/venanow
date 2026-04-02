"""
api/routes/manual_entries.py

Manual transaction entry endpoint.
Allows users to add cash, mobile money, and other off-bank transactions.
Stores in-memory per user session (wire to DB for persistence).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Optional, List
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

# In-memory store per user — replace with DB in production
_manual_store: dict[str, list[dict]] = {}

VALID_CATEGORIES = [
    "food", "transport", "rent", "utilities", "subscriptions",
    "transfers", "business", "miscellaneous", "income", "freelance", "investment"
]

VALID_CHANNELS = ["Cash", "Mobile Money", "USSD", "POS", "Bank Transfer", "Other"]


class ManualEntryRequest(BaseModel):
    user_id: str
    tx_date: str           # ISO format: 2026-03-15
    description: str
    amount: float
    tx_type: str           # 'debit' or 'credit'
    category: str
    channel: str = "Cash"
    notes: Optional[str] = None


class ManualEntryResponse(BaseModel):
    id: str
    user_id: str
    tx_date: str
    description: str
    amount: float
    tx_type: str
    category: str
    channel: str
    notes: Optional[str]
    source: str = "manual"
    created_at: str


@router.post("", response_model=ManualEntryResponse)
async def add_manual_entry(entry: ManualEntryRequest):
    """Add a manual transaction (cash, mobile money, etc.)"""

    # Validate
    if entry.tx_type not in ("debit", "credit"):
        from fastapi import HTTPException
        raise HTTPException(400, "tx_type must be 'debit' or 'credit'")
    if entry.category not in VALID_CATEGORIES:
        from fastapi import HTTPException
        raise HTTPException(400, f"Invalid category. Choose from: {VALID_CATEGORIES}")
    if entry.amount <= 0:
        from fastapi import HTTPException
        raise HTTPException(400, "Amount must be greater than 0")

    record = {
        "id":          str(uuid.uuid4()),
        "user_id":     entry.user_id,
        "tx_date":     entry.tx_date,
        "description": entry.description.strip(),
        "amount":      round(entry.amount, 2),
        "tx_type":     entry.tx_type,
        "category":    entry.category,
        "channel":     entry.channel,
        "notes":       entry.notes,
        "source":      "manual",
        "balance":     None,
        "fingerprint": str(uuid.uuid4()),
        "created_at":  datetime.utcnow().isoformat(),
    }

    if entry.user_id not in _manual_store:
        _manual_store[entry.user_id] = []
    _manual_store[entry.user_id].append(record)

    return record


@router.get("/{user_id}", response_model=List[ManualEntryResponse])
async def get_manual_entries(user_id: str):
    """Get all manual entries for a user."""
    return _manual_store.get(user_id, [])


@router.delete("/{user_id}/{entry_id}")
async def delete_manual_entry(user_id: str, entry_id: str):
    """Delete a specific manual entry."""
    entries = _manual_store.get(user_id, [])
    original_len = len(entries)
    _manual_store[user_id] = [e for e in entries if e["id"] != entry_id]
    deleted = original_len - len(_manual_store[user_id])
    return {"deleted": deleted, "id": entry_id}
