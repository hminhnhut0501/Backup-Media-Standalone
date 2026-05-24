import os
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from database import get_db_connection
import mod_backup

app = FastAPI(title="Backup Media Standalone")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

conn = get_db_connection()
mod_backup.init_db(conn)
app.include_router(mod_backup.router, prefix="/api/backup", tags=["backup"])


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.get("/api/drip/targets")
async def drip_targets():
    # Keep compatibility with backup UI datalist loader.
    return []


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8010")), reload=True)
