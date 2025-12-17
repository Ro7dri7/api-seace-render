from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
from scraper import run_scraper
import uvicorn
import os
import logging

# Configuración de logs para la API
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("API")

app = FastAPI(title="API Scraper SEACE")

class ScrapeRequest(BaseModel):
    fecha_inicio: str  # Formato dd/mm/yyyy
    fecha_fin: str     # Formato dd/mm/yyyy
    max_resultados: Optional[int] = None  # Opcional: si no se envía, busca todo

@app.get("/")
def read_root():
    return {"status": "ok", "msg": "API SEACE activa. Usa POST /scrape"}

@app.post("/scrape")
async def scrape_endpoint(request: ScrapeRequest):
    # Validación básica del formato de fecha
    if (
            len(request.fecha_inicio) != 10 or
            len(request.fecha_fin) != 10 or
            request.fecha_inicio[2] != '/' or
            request.fecha_inicio[5] != '/' or
            request.fecha_fin[2] != '/' or
            request.fecha_fin[5] != '/'
    ):
        raise HTTPException(status_code=400, detail="Las fechas deben tener formato dd/mm/yyyy")

    try:
        logger.info(f"📩 Solicitud recibida: {request}")

        # Llamada al scraper con los 3 argumentos correctos
        data = await run_scraper(
            request.fecha_inicio,
            request.fecha_fin,
            request.max_resultados
        )

        return {"cantidad": len(data), "resultados": data}

    except Exception as e:
        logger.error(f"❌ Error interno en scraper: {e}")
        raise HTTPException(status_code=500, detail=f"Error al procesar la solicitud: {str(e)}")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)