from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import os
from dotenv import load_dotenv

# Carica le variabili d'ambiente
load_dotenv()

from app.core.config import settings
from app.api.v1.api import api_router
from app.monitoring.middleware import MetricsMiddleware
from app.monitoring.logging import configure_logging

def create_app() -> FastAPI:
    configure_logging()
    
    app = FastAPI(
        title="Football Serie A Prediction System",
        description="Professional-grade football prediction system with hybrid data architecture",
        version="1.0.0",
        openapi_url=f"{settings.API_V1_STR}/openapi.json",
    )

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.BACKEND_CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Monitoring middleware
    app.add_middleware(MetricsMiddleware)

    # Include API routes
    app.include_router(api_router, prefix=settings.API_V1_STR)

    # Serve frontend
    templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "frontend", "templates"))
    
    @app.get("/", response_class=HTMLResponse)
    async def serve_homepage(request: Request):
        return templates.TemplateResponse("index.html", {"request": request})
    
    @app.get("/dashboard", response_class=HTMLResponse)
    async def serve_dashboard(request: Request):
        return templates.TemplateResponse("index.html", {"request": request})

    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)