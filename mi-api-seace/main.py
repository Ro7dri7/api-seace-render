from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from scraper import run_scraper
import uvicorn
import os

app = FastAPI(title="API Scraper SEACE")

# Modelo de datos que esperamos recibir de n8n
class ScrapeRequest(BaseModel):
    fecha_inicio: str  # Formato dd/mm/yyyy
    fecha_fin: str     # Formato dd/mm/yyyy
    max_resultados: int = 10
    incluir_cubso: bool = False

@app.get("/")
def read_root():
    return {"status": "ok", "msg": "API SEACE activa. Usa POST /scrape"}

@app.post("/scrape")
async def scrape_endpoint(request: ScrapeRequest):
    """
    Endpoint principal.
    Recibe JSON: { "fecha_inicio": "27/10/2025", "fecha_fin": "02/11/2025", "incluir_cubso": true }
    """
    # Validaciones básicas de formato de fecha
    try:
        if len(request.fecha_inicio) != 10 or len(request.fecha_fin) != 10:
            raise ValueError("Formato de fecha incorrecto")
    except:
        raise HTTPException(status_code=400, detail="Las fechas deben ser dd/mm/yyyy")

    try:
        print(f"Solicitud recibida: {request}")
        data = await run_scraper(
            request.fecha_inicio,
            request.fecha_fin,
            request.max_resultados,
            request.incluir_cubso
        )
        return {"cantidad": len(data), "resultados": data}
    except Exception as e:
        print(f"Error interno: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # Render asigna un puerto en la variable de entorno PORT
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)