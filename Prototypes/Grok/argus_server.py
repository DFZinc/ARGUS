"""
argus_server.py - Dashboard + Background Worker
"""

import asyncio
import logging
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import uvicorn

from argus_agent import ArgusAgent   # your main agent
from argus_cache import ArgusCache

log = logging.getLogger(__name__)

app = FastAPI(title="ARGUS Narrative Intelligence")
templates = Jinja2Templates(directory="templates")

cache = ArgusCache()
agent = ArgusAgent()

# Background task reference
background_task = None

async def background_worker():
    """Runs the ARGUS agent in the background"""
    log.info("🛠️ Starting ARGUS background worker...")
    await agent.run()   # This will run the infinite loop

@app.on_event("startup")
async def startup_event():
    global background_task
    background_task = asyncio.create_task(background_worker())

@app.on_event("shutdown")
async def shutdown_event():
    if background_task:
        background_task.cancel()

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    findings = cache.get_latest_findings(limit=40)
    narratives = cache.get_latest_narratives(limit=20)

    return templates.TemplateResponse("index.html", {
        "request": request,
        "findings": findings,
        "narratives": narratives,
        "last_updated": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    })

@app.get("/api/findings")
async def api_findings():
    return {"findings": cache.get_latest_findings(50)}

@app.get("/api/narratives")
async def api_narratives():
    return {"narratives": cache.get_latest_narratives(20)}

if __name__ == "__main__":
    log.info("🚀 Starting ARGUS Dashboard + Background Worker on http://127.0.0.1:8002")
    uvicorn.run(app, host="0.0.0.0", port=8002)