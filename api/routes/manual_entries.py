# api/routes/manual_entries.py
# Handles manual transaction entries (cash, mobile money, etc.)
# that are not captured in bank statements.

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import date
import uuid

router = APIRouter()

# ---------------------------------------------------------------------------
# In-memory store (Phase 1)
# Replace this dict with Supabase DB calls in Phase 2.
# Key: user_id → list of entry dicts
# ---------------------------------------------------------------------------
_store: dict[str, list[dict]] = {}


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class ManualEntryCreate(BaseModel):
    user_id: str
    tx_date: date
    description: str
    amount: float               # positive = credit, negative = debit
    tx_type: str                # "credit" or "debit"
    category: Optional[str] = "miscellaneous"
    channel: Optional[str] = "cash"
    notes: Optional[str] = ""


class ManualEntryResponse(BaseModel):
    entry_id: str
    user_id: str
    tx_date: date
    description: str
    amount: float
    tx_type: str
    category: str
    channel: str
    notes: str


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("", response_model=ManualEntryResponse, status_code=201)
async def add_manual_entry(entry: ManualEntryCreate):
    """Add a manual transaction entry for a user."""
    entry_id = str(uuid.uuid4())
    record = {
        "entry_id": entry_id,
        "user_id": entry.user_id,
        "tx_date": entry.tx_date.isoformat(),
        "description": entry.description,
        "amount": entry.amount,
        "tx_type": entry.tx_type,
        "category": entry.category or "miscellaneous",
        "channel": entry.channel or "cash",
        "notes": entry.notes or "",
    }
    _store.setdefault(entry.user_id, []).append(record)
    return record


@router.get("/{user_id}", response_model=list[ManualEntryResponse])
async def get_manual_entries(user_id: str):
    """Get all manual entries for a user."""
    return _store.get(user_id, [])


@router.delete("/{user_id}/{entry_id}", status_code=204)
async def delete_manual_entry(user_id: str, entry_id: str):
    """Delete a specific manual entry."""
    entries = _store.get(user_id, [])
    original_len = len(entries)
    _store[user_id] = [e for e in entries if e["entry_id"] != entry_id]
    if len(_store[user_id]) == original_len:
        raise HTTPException(status_code=404, detail="Entry not found")


@router.put("/{user_id}/{entry_id}", response_model=ManualEntryResponse)
async def update_manual_entry(user_id: str, entry_id: str, updates: ManualEntryCreate):
    """Update a manual entry (full replace)."""
    entries = _store.get(user_id, [])
    for i, e in enumerate(entries):
        if e["entry_id"] == entry_id:
            updated = {
                "entry_id": entry_id,
                "user_id": user_id,
                "tx_date": updates.tx_date.isoformat(),
                "description": updates.description,
                "amount": updates.amount,
                "tx_type": updates.tx_type,
                "category": updates.category or "miscellaneous",
                "channel": updates.channel or "cash",
                "notes": updates.notes or "",
            }
            entries[i] = updated
            return updated
    raise HTTPException(status_code=404, detail="Entry not found")
