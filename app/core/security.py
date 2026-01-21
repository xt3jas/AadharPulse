from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
from .config import get_settings

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

async def verify_api_key(api_key: str = Security(API_KEY_HEADER)) -> str:
    
    settings = get_settings()
    expected_key = getattr(settings, "API_KEY", None)
    
    if not expected_key:
        return "anonymous"
    
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API Key in X-API-Key header"
        )
    
    if api_key != expected_key:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid API Key"
        )
    
    return api_key
