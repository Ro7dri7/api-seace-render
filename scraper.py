import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SEACE_Scraper")

# ✅ URL CORREGIDA: sin espacios al final
SEACE_URL = "https://prod6.seace.gob.pe/buscador-publico/contrataciones"

def parse_fecha_seace(fecha_str: str):
    try:
        fecha_str = fecha_str.replace("Fecha de publicación:", "").strip()
        return datetime.strptime(fecha_str, "%d/%m/%Y %H:%M:%S")
    except (ValueError, AttributeError):
        return None

def fecha_en_rango(fecha_str: str, fecha_inicio: str, fecha_fin: str) -> bool:
    fecha = parse_fecha_seace(fecha_str)
    if not fecha:
        return False
    inicio = datetime.strptime(fecha_inicio, "%d/%m/%Y")
    fin = datetime.strptime(fecha_fin, "%d/%m/%Y")
    return inicio.date() <= fecha.date() <= fin.date()

def extraer_tipo(desc: str) -> str:
    if not isinstance(desc, str):
        return "Otro"
    d = desc.lower()
    if d.startswith("bien:"):
        return "Bien"
    elif d.startswith("servicio:"):
        return "Servicio"
    elif d.startswith("obra:"):
        return "Obra"
    elif "consultor" in d:
        return "Consultoría"
    else:
        return "Otro"

async def get_cubso(page, url):
    if url == "No disponible":
        return "No disponible"
    try:
        await page.goto(url, timeout=30000)
        await page.wait_for_selector("body", timeout=15000)
        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")

        for cell in soup.find_all("td", class_=re.compile(r".*codCubso.*", re.IGNORECASE)):
            txt = cell.get_text(strip=True)
            if txt.isdigit() and 13 <= len(txt) <= 16:
                return txt

        match = re.search(r"\b\d{13,16}\b", soup.get_text())
        return match.group() if match else "No encontrado"
    except Exception as e:
        return "Error"

async def run_scraper(fecha_inicio: str, fecha_fin: str, max_items: int, include_cubso: bool):
    items_data = []
    logger.info(f"Iniciando scraping: {fecha_inicio} - {fecha_fin}, max: {max_items}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--single-process"  # Crítico para Render Free
            ]
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            # Navegamos a SEACE
            logger.info("Navegando a SEACE...")
            await page.goto(SEACE_URL, timeout=60000)

            # Esperamos a que el body esté presente
            await page.wait_for_selector("body", timeout=30000)

            # Simulamos interacción humana
            await page.evaluate("window.scrollBy(0, 500)")
            await asyncio.sleep(2)  # Tiempo realista

            # Intentar cambiar a 100 resultados
            try:
                select_button = await page.query_selector("button[aria-haspopup='listbox']")
                if select_button:
                    await select_button.click()
                    opt = await page.query_selector("mat-option:has-text('100')")
                    if opt:
                        await opt.click()
                        await asyncio.sleep(2)
            except Exception as e:
                logger.warning(f"No se pudo cambiar a 100 resultados: {e}")

            page_count = 1
            max_paginas = 200  # Límite alto, pero seguro

            # Determinar límite efectivo
            max_items_effective = max_items if max_items is not None else float('inf')

            while page_count <= max_paginas and len(items_data) < max_items_effective:
                logger.info(f"Procesando página {page_count} | Recopilados: {len(items_data)}")

                cards = await page.query_selector_all('div[class*="bg-fondo-section"]')
                if not cards:
                    logger.info("No se encontraron más tarjetas. Finalizando.")
                    break

                en_rango_en_pagina = False
                for card in cards:
                    try:
                        html = await card.inner_html()
                        soup = BeautifulSoup(html, "html.parser")
                        p_tags = soup.select("p")

                        fecha_raw = "No disponible"
                        for p in p_tags:
                            if "Fecha de publicación:" in p.get_text():
                                fecha_raw = p.get_text(strip=True)
                                break

                        if fecha_en_rango(fecha_raw, fecha_inicio, fecha_fin):
                            en_rango_en_pagina = True
                            codigo = p_tags[0].get_text(strip=True) if len(p_tags) > 0 else "N/A"
                            entidad = p_tags[1].get_text(strip=True) if len(p_tags) > 1 else "N/A"
                            desc = p_tags[2].get_text(strip=True) if len(p_tags) > 2 else "N/A"

                            link_elem = soup.select_one("a[href*='/buscador-publico/contrataciones/']")
                            enlace = urljoin(SEACE_URL, link_elem["href"]) if link_elem else "No disponible"

                            items_data.append({
                                "codigo": codigo,
                                "entidad": entidad,
                                "descripcion": desc,
                                "tipo": extraer_tipo(desc),
                                "fecha_publicacion": fecha_raw.replace("Fecha de publicación:", "").strip(),
                                "enlace": enlace,
                                "cubso": None
                            })

                            if len(items_data) >= max_items_effective:
                                break

                    except Exception as e:
                        continue

                # Si no hay licitaciones en rango en toda la página, detenemos
                if not en_rango_en_pagina:
                    logger.info("No más licitaciones en el rango de fechas. Deteniendo búsqueda.")
                    break

                # Siguiente página
                next_btn = await page.query_selector("button[aria-label*='iguiente']:not([disabled])")
                if not next_btn:
                    break

                await next_btn.click()
                await page.wait_for_selector('div[class*="bg-fondo-section"]', timeout=45000)
                await asyncio.sleep(2)
                page_count += 1

            # Extraer CUBSO si se solicita
            if include_cubso and items_data:
                logger.info(f"Extrayendo CUBSO para {len(items_data)} licitaciones...")
                for item in items_data:
                    if item["enlace"] != "No disponible":
                        item["cubso"] = await get_cubso(page, item["enlace"])
                    else:
                        item["cubso"] = "No enlace"

        except Exception as e:
            logger.error(f"Error crítico en el scraper: {e}")
            raise
        finally:
            await browser.close()

    logger.info(f"Scraping finalizado. Total obtenido: {len(items_data)}")
    return items_data