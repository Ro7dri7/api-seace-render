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
    max_resultados: int = 2000  # ✅ Límite de seguridad alto (no es un tope real)
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
    # Validación básica del formato de fecha (dd/mm/yyyy)
    if (
            len(request.fecha_inicio) != 10
            or len(request.fecha_fin) != 10
            or request.fecha_inicio[2] != '/'
            or request.fecha_inicio[5] != '/'
            or request.fecha_fin[2] != '/'
            or request.fecha_fin[5] != '/'
    ):
        raise HTTPException(status_code=400, detail="Las fechas deben tener formato dd/mm/yyyy")

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
        print(f"Error interno en scraper: {e}")
        raise HTTPException(status_code=500, detail="Error al procesar la solicitud")

if __name__ == "__main__":
    # Render asigna el puerto en la variable de entorno PORT
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)