from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.routes import router as api_router
from .core.config import ensure_data_directories, get_settings

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    
    settings = get_settings()
    ensure_data_directories()
    print(f"🚀 {settings.APP_NAME} v{settings.APP_VERSION} starting...")
    print(f"📂 Data directory: {settings.DATA_DIR}")
    print(f"🌐 API prefix: {settings.API_PREFIX}")
    
    yield
    
    print(f"👋 {settings.APP_NAME} shutting down...")

def create_app() -> FastAPI:
    
    settings = get_settings()
    
    app = FastAPI(
        title=settings.APP_NAME,
        description=,
        version=settings.APP_VERSION,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan
    )
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    app.include_router(
        api_router,
        prefix=settings.API_PREFIX,
        tags=["AadharPulse API"]
    )
    
    @app.get("/", tags=["Root"])
    async def root():
        
        return {
            "name": settings.APP_NAME,
            "version": settings.APP_VERSION,
            "docs": "/docs",
            "health": f"{settings.API_PREFIX}/health"
        }
    
    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    
    settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG
    )
