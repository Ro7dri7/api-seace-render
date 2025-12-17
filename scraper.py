import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SEACE_Scraper")

SEACE_URL = "https://prod6.seace.gob.pe/buscador-publico/contrataciones"

def parse_fecha_seace(fecha_str: str):
    # Limpieza más robusta usando regex para quitar texto
    try:
        clean_str = re.sub(r"[^\d/:\s]", "", fecha_str).strip()
        return datetime.strptime(clean_str, "%d/%m/%Y %H:%M:%S")
    except (ValueError, AttributeError) as e:
        return None

def extraer_tipo(desc: str) -> str:
    if not isinstance(desc, str): return "Otro"
    d = desc.lower()
    if d.startswith("bien:"): return "Bien"
    elif d.startswith("servicio:"): return "Servicio"
    elif d.startswith("obra:"): return "Obra"
    elif "consultor" in d: return "Consultoría"
    else: return "Otro"

async def get_cubso(page, url):
    if url == "No disponible": return "No disponible"
    try:
        await page.goto(url, timeout=30000)
        await page.wait_for_selector("body", timeout=15000)
        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")

        # Búsqueda optimizada de CUBSO
        match = re.search(r"\b\d{13,16}\b", soup.get_text())
        return match.group() if match else "No encontrado"
    except Exception:
        return "Error"

async def run_scraper(fecha_inicio_str: str, fecha_fin_str: str, max_items: int, include_cubso: bool):
    items_data = []

    # Convertimos las fechas límite a objetos datetime para comparar
    f_inicio = datetime.strptime(fecha_inicio_str, "%d/%m/%Y")
    f_fin = datetime.strptime(fecha_fin_str, "%d/%m/%Y")

    # Ajustamos fin para incluir todo el día final (23:59:59)
    f_fin = f_fin.replace(hour=23, minute=59, second=59)

    logger.info(f"🔎 Buscando entre: {f_inicio} y {f_fin}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-gpu", "--single-process"]
        )
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        page = await context.new_page()

        try:
            logger.info("Navegando a SEACE...")
            await page.goto(SEACE_URL, timeout=60000)

            # 🔥 IMPORTANTE: Esperar a que cargue la lista, no solo el body
            try:
                await page.wait_for_selector('div[class*="bg-fondo-section"]', timeout=30000)
            except:
                logger.warning("No se detectaron tarjetas iniciales. El sitio podría estar lento.")

            # Intentar cambiar a 100 resultados (opcional pero recomendado)
            try:
                await page.get_by_role("button", name="10").click() # Busca el selector de paginación
                await page.get_by_text("100").click()
                await asyncio.sleep(3) # Esperar recarga de tabla
            except:
                pass # Si falla, seguimos con 10 por página

            page_count = 1
            max_paginas = 200
            stop_scraping = False
            max_items_effective = max_items if max_items is not None else float('inf')

            while page_count <= max_paginas and len(items_data) < max_items_effective and not stop_scraping:
                logger.info(f"📄 Procesando página {page_count} | Recopilados: {len(items_data)}")

                # Re-capturamos las tarjetas en cada vuelta
                cards = await page.query_selector_all('div[class*="bg-fondo-section"]')

                if not cards:
                    logger.info("Fin de la lista (no hay más tarjetas).")
                    break

                items_in_page_processed = 0

                for card in cards:
                    if len(items_data) >= max_items_effective:
                        break

                    try:
                        html = await card.inner_html()
                        soup = BeautifulSoup(html, "html.parser")
                        p_tags = soup.select("p")

                        # Extraer fecha
                        fecha_raw = "No disponible"
                        for p in p_tags:
                            if "Fecha de publicación:" in p.get_text():
                                fecha_raw = p.get_text(strip=True)
                                break

                        fecha_obj = parse_fecha_seace(fecha_raw)

                        if not fecha_obj:
                            continue # Si no hay fecha, saltamos

                        # === 🧠 LÓGICA CORREGIDA ===

                        # 1. Si la fecha es MAYOR que el fin, es muy nueva. Seguimos buscando (Next Page)
                        if fecha_obj > f_fin:
                            continue

                            # 2. Si la fecha es MENOR que el inicio, es muy antigua. PARAMOS TODO.
                        if fecha_obj < f_inicio:
                            logger.info(f"🛑 Fecha encontrada ({fecha_obj}) es anterior al inicio solicitado. Deteniendo.")
                            stop_scraping = True
                            break # Rompe el for de cartas

                        # 3. Si llegamos aquí, está DENTRO del rango
                        items_in_page_processed += 1

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
                            "fecha_publicacion": fecha_obj.strftime("%d/%m/%Y %H:%M:%S"),
                            "enlace": enlace,
                            "cubso": None
                        })

                    except Exception as e:
                        continue

                if stop_scraping:
                    break

                # Paginación
                next_btn = await page.query_selector("button[aria-label*='iguiente']:not([disabled])")
                if next_btn:
                    await next_btn.click()
                    # Esperar a que el contenido cambie o cargue
                    await asyncio.sleep(2)
                    await page.wait_for_selector('div[class*="bg-fondo-section"]', state="attached")
                    page_count += 1
                else:
                    logger.info("No hay botón 'Siguiente'. Fin.")
                    break

            # Extracción de CUBSO (fuera del bucle principal para no ralentizar la búsqueda)
            if include_cubso and items_data:
                logger.info("Extrayendo CUBSO...")
                for item in items_data:
                    if item["enlace"] != "No disponible":
                        item["cubso"] = await get_cubso(page, item["enlace"])
                    else:
                        item["cubso"] = "No enlace"

        except Exception as e:
            logger.error(f"Error crítico: {e}")
            raise
        finally:
            await browser.close()

    logger.info(f"✅ Finalizado. Total: {len(items_data)}")
    return items_data