from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import shutil
import os
import uuid
from unified_parser import process_file

rack_app = FastAPI()
templates = Jinja2Templates(directory="templates")

UPLOAD_DIR = "uploads"
OUTPUT_DIR = "output_combined"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

@rack_app.get("/", response_class=HTMLResponse)
async def upload_page(request: Request):
    return templates.TemplateResponse("upload.html", {"request": request})

@rack_app.post("/upload/", response_class=HTMLResponse)
async def handle_upload(request: Request, file: UploadFile = File(...)):
    extension = os.path.splitext(file.filename)[1]
    if extension not in [".csv", ".xlsx"]:
        return templates.TemplateResponse("upload.html", {
            "request": request,
            "error": "Only CSV and XLSX files are supported."
        })

    temp_filename = f"{uuid.uuid4()}{extension}"
    temp_filepath = os.path.join(UPLOAD_DIR, temp_filename)
    with open(temp_filepath, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    result = process_file(temp_filepath, OUTPUT_DIR)
    if not result:
        return templates.TemplateResponse("upload.html", {
            "request": request,
            "error": "Failed to parse and process file."
        })

    sheet_type, device_count, rack_count = result
    conn_path = os.path.join(OUTPUT_DIR, f"{sheet_type}_connections.txt")
    topo_path = os.path.join(OUTPUT_DIR, f"{sheet_type}_rack_topology.txt")

    with open(conn_path) as f1, open(topo_path) as f2:
        connections = f1.read()
        topology = f2.read()

    return templates.TemplateResponse("result.html", {
        "request": request,
        "sheet": sheet_type,
        "devices": device_count,
        "racks": rack_count,
        "connections": connections,
        "topology": topology
    })
