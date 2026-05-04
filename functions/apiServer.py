import threading
import logging
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from library.app import WANT_API_PORT

logger = logging.getLogger(__name__)

app = FastAPI(title="torbox-media-center Want API")


class WantRequest(BaseModel):
    tmdb_id: int
    media_type: str
    title: str | None = None
    year: int | None = None
    seasons: list[int] | None = None


@app.post("/want", status_code=201)
def post_want(req: WantRequest):
    from functions.wantFunctions import addWanted

    success, detail = addWanted(
        tmdb_id=req.tmdb_id,
        media_type=req.media_type,
        title=req.title,
        year=req.year,
        priority=10,
        seasons_needed=req.seasons,
    )
    if success:
        return {"status": "queued", "tmdb_id": req.tmdb_id}
    return JSONResponse(status_code=409, content={"status": "skipped", "detail": detail})


@app.get("/want")
def get_want():
    from functions.wantFunctions import getAllWanted

    return {"items": getAllWanted()}


@app.delete("/want/{tmdb_id}", status_code=204)
def delete_want(tmdb_id: int):
    from functions.wantFunctions import removeWanted

    removeWanted(tmdb_id)


@app.post("/want/discover", status_code=202)
def trigger_discover(background_tasks: BackgroundTasks):
    from functions.discoveryFunctions import runDiscovery

    background_tasks.add_task(runDiscovery)
    return {"status": "discovery triggered"}


@app.post("/want/acquire", status_code=202)
def trigger_acquire(background_tasks: BackgroundTasks):
    from functions.acquisitionFunctions import runAcquisition

    background_tasks.add_task(runAcquisition)
    return {"status": "acquisition triggered"}


def startApiServer():
    import uvicorn

    def _run():
        uvicorn.run(app, host="0.0.0.0", port=WANT_API_PORT, log_level="warning")

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    logger.info(f"Want API running on port {WANT_API_PORT}")
