from fastapi import Depends, HTTPException, Header
from typing import Optional
from app.core.config import settings

async def verify_api_key(api_key: Optional[str] = Header(None, alias="X-API-Key")):
    if not api_key or api_key != settings.API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key"
        )
    return api_key