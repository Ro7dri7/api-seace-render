import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re
import logging

# Configuración de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SEACE_Scraper")

SEACE_URL = "https://prod6.seace.gob.pe/buscador-publico/contrataciones"

def parse_fecha_regex(texto_completo: str):
    """
    Busca cualquier patrón de fecha y hora (dd/mm/yyyy HH:mm) dentro del texto.
    Es inmune a cambios en el HTML, solo necesita que el texto sea visible.
    """
    # Regex: busca dd/mm/yyyy seguido de hora hh:mm
    match = re.search(r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})", texto_completo)
    if match:
        try:
            fecha_str = match.group(1)
            # Normalizar espacios múltiples
            fecha_str = re.sub(r"\s+", " ", fecha_str)
            return datetime.strptime(fecha_str, "%d/%m/%Y %H:%M")
        except ValueError:
            return None
    return None

def extraer_tipo(desc: str) -> str:
    if not isinstance(desc, str): return "Otro"
    d = desc.lower()
    if d.startswith("bien"): return "Bien"
    elif d.startswith("servicio"): return "Servicio"
    elif d.startswith("obra"): return "Obra"
    elif "consultor" in d: return "Consultoría"
    else: return "Otro"

async def get_cubso(page, url):
    if url == "No disponible": return "No disponible"
    try:
        await page.goto(url, timeout=15000) # Timeout corto para no demorar
        await page.wait_for_selector("body", timeout=10000)
        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")
        match = re.search(r"\b\d{13,16}\b", soup.get_text())
        return match.group() if match else "No encontrado"
    except Exception:
        return "Error"

async def run_scraper(fecha_inicio_str: str, fecha_fin_str: str, max_items: int, include_cubso: bool):
    items_data = []

    # 1. Preparar fechas
    try:
        f_inicio = datetime.strptime(fecha_inicio_str, "%d/%m/%Y")
        f_fin = datetime.strptime(fecha_fin_str, "%d/%m/%Y")
        # Ajustamos fin al final del día
        f_fin = f_fin.replace(hour=23, minute=59, second=59)
    except ValueError:
        logger.error("Formato de fecha incorrecto. Use dd/mm/yyyy")
        return []

    logger.info(f"🔎 OBJETIVO: Buscar todas las licitaciones entre {f_inicio} y {f_fin}")

    async with async_playwright() as p:
        # Lanzamiento optimizado para servidor (headless)
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-gpu", "--single-process"]
        )
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        page = await context.new_page()

        try:
            logger.info("🌍 Navegando a SEACE...")
            await page.goto(SEACE_URL, timeout=60000)

            # Esperar carga inicial
            try:
                await page.wait_for_selector('div[class*="bg-fondo-section"]', timeout=30000)
            except:
                logger.warning("⚠️ Alerta: Carga lenta detectada.")

            # Intentar poner 100 resultados por página para ir más rápido
            try:
                await page.get_by_role("combobox").click()
                await page.get_by_text("100").click()
                await asyncio.sleep(3)
            except:
                pass # Si falla, seguimos con 10

            page_count = 1
            max_paginas = 300 # Límite de seguridad alto
            stop_scraping = False

            # Si el usuario no mandó límite, asumimos infinito (solo manda el rango de fechas)
            limit_count = max_items if max_items is not None and max_items > 0 else 999999

            while page_count <= max_paginas and not stop_scraping:
                if len(items_data) >= limit_count:
                    logger.info("✅ Límite de cantidad alcanzado.")
                    break

                logger.info(f"📄 Procesando PÁGINA {page_count} | Llevamos: {len(items_data)}")

                # Recoger tarjetas
                cards = await page.query_selector_all('div[class*="bg-fondo-section"]')
                if not cards:
                    logger.info("🛑 No hay más tarjetas en la lista.")
                    break

                items_added_this_page = 0

                for card in cards:
                    if len(items_data) >= limit_count:
                        break

                    try:
                        # Extraemos TODO el texto visible de la tarjeta
                        text_content = await card.inner_text()

                        # Buscamos la fecha ahí dentro
                        fecha_obj = parse_fecha_regex(text_content)

                        if not fecha_obj:
                            # Si no hay fecha legible, saltamos por seguridad
                            continue

                        # === LÓGICA CRÍTICA DE FECHAS ===

                        if fecha_obj > f_fin:
                            # Fecha del item (ej: 16/12) > Fecha fin (12/12) -> Es muy nuevo
                            # NO paramos, solo saltamos este item y seguimos buscando
                            continue

                        if fecha_obj < f_inicio:
                            # Fecha del item (ej: 04/12) < Fecha inicio (05/12) -> Es muy viejo
                            # AQUÍ PARAMOS TODO EL PROCESO
                            logger.info(f"🛑 Se encontró fecha antigua ({fecha_obj}). Fin de la búsqueda.")
                            stop_scraping = True
                            break

                            # Si llegamos aquí, la fecha está DENTRO del rango
                        # Extraemos los datos restantes
                        lines = [l.strip() for l in text_content.split('\n') if l.strip()]

                        # Extracción heurística simple
                        codigo = lines[0] if lines else "S/D"
                        entidad = lines[1] if len(lines) > 1 else "S/D"
                        desc = lines[2] if len(lines) > 2 else "S/D"

                        # Obtener enlace
                        html = await card.inner_html()
                        soup = BeautifulSoup(html, "html.parser")
                        link_elem = soup.select_one("a[href*='/buscador-publico/contrataciones/']")
                        enlace = urljoin(SEACE_URL, link_elem["href"]) if link_elem else "No disponible"

                        items_data.append({
                            "codigo": codigo,
                            "entidad": entidad,
                            "descripcion": desc,
                            "tipo": extraer_tipo(desc),
                            "fecha_publicacion": fecha_obj.strftime("%d/%m/%Y %H:%M"),
                            "enlace": enlace,
                            "cubso": None
                        })
                        items_added_this_page += 1

                    except Exception as e:
                        continue

                logger.info(f"   --> Agregados en esta página: {items_added_this_page}")

                if stop_scraping:
                    break

                # Ir a página siguiente
                next_btn = await page.query_selector("button[aria-label*='iguiente']:not([disabled])")
                if next_btn:
                    await next_btn.click()
                    await asyncio.sleep(1.5) # Pausa ligera
                    await page.wait_for_timeout(500) # Estabilidad
                    page_count += 1
                else:
                    logger.info("🚫 No hay botón siguiente.")
                    break

            # Extracción de CUBSO (Opcional)
            if include_cubso and items_data:
                logger.info(f"🛠 Extrayendo CUBSO para {len(items_data)} registros...")
                for item in items_data:
                    if item["enlace"] != "No disponible":
                        item["cubso"] = await get_cubso(page, item["enlace"])
                    else:
                        item["cubso"] = "N/A"

        except Exception as e:
            logger.error(f"Error crítico: {e}")
            raise
        finally:
            await browser.close()

    logger.info(f"✅ FIN DEL SCRAPING. Total recolectado: {len(items_data)}")
    return items_data