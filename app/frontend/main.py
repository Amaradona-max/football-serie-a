from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import os

app = FastAPI(title="Serie A Frontend", version="1.0.0")

# Configure templates
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

# Mount static files (if any)
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")

@app.get("/", response_class=HTMLResponse)
async def serve_homepage(request: Request):
    """Serve the main dashboard homepage"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    """Serve the dashboard (alias for homepage)"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health", response_class=HTMLResponse)
async def health_check(request: Request):
    """Health check endpoint for frontend"""
    return HTMLResponse(content="<h1>Frontend Server Healthy</h1>", status_code=200)