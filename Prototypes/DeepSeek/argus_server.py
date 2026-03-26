"""
argus_server.py - Dashboard + Background Worker
"""

import asyncio
import logging
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import uvicorn

from argus_agent import ArgusAgent
from argus_cache import ArgusCache
from config import DASHBOARD_HOST, DASHBOARD_PORT

log = logging.getLogger(__name__)

# Global instances
cache = ArgusCache()
agent = ArgusAgent(enable_llm=False)
background_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown"""
    global background_task
    
    # Startup
    log.info("Starting ARGUS Dashboard...")
    background_task = asyncio.create_task(agent.run())
    yield
    
    # Shutdown
    log.info("Shutting down ARGUS Dashboard...")
    if background_task:
        background_task.cancel()
        try:
            await background_task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="ARGUS Narrative Intelligence",
    description="Real-time narrative detection and token launch analysis",
    version="2.0.0",
    lifespan=lifespan
)

# Setup templates
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard page"""
    findings = cache.get_latest_findings(limit=40)
    narratives = cache.get_latest_narratives(limit=20)
    stats = cache.get_stats()
    health = await agent.health_check()
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "findings": findings,
        "narratives": narratives,
        "stats": stats,
        "health": health,
        "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    })


@app.get("/api/findings")
async def api_findings(limit: int = 50):
    """API endpoint for findings"""
    return {
        "findings": cache.get_latest_findings(limit),
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/api/narratives")
async def api_narratives(limit: int = 20):
    """API endpoint for narratives"""
    return {
        "narratives": cache.get_latest_narratives(limit),
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/api/top-narratives")
async def api_top_narratives(hours: int = 24, limit: int = 10):
    """API endpoint for top narratives by velocity"""
    return {
        "narratives": cache.get_top_narratives(hours, limit),
        "hours": hours,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/api/stats")
async def api_stats():
    """API endpoint for cache statistics"""
    return cache.get_stats()


@app.get("/api/health")
async def api_health():
    """Health check endpoint"""
    health = await agent.health_check()
    return health


@app.get("/api/cycle")
async def trigger_cycle():
    """Manually trigger an analysis cycle"""
    try:
        await agent._cycle()
        return {"status": "success", "message": "Cycle triggered"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    log.info(f"🚀 Starting ARGUS Dashboard on http://{DASHBOARD_HOST}:{DASHBOARD_PORT}")
    uvicorn.run(app, host=DASHBOARD_HOST, port=DASHBOARD_PORT)