"""
api/auth.py

Supabase JWT verification for FastAPI.
Use as a dependency on any protected route:

    from api.auth import get_current_user
    @router.get("/protected")
    async def protected(user=Depends(get_current_user)):
        return {"user_id": user["sub"]}
"""

import os
import jwt
from fastapi import HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET")  # set on Render
bearer = HTTPBearer(auto_error=False)


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Security(bearer),
) -> dict:
    """
    Verify Supabase JWT and return the decoded payload.
    Raises 401 if token is missing or invalid.
    """
    if not credentials:
        raise HTTPException(status_code=401, detail="Not authenticated.")

    token = credentials.credentials
    try:
        payload = jwt.decode(
            token,
            SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
            audience="authenticated",
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Session expired. Please log in again.")
    except jwt.InvalidTokenError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {e}")
