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

ESTADOS_IGNORAR = [
    "VIGENTE", "EN EVALUACION", "EN EVALUACIÓN",
    "ADJUDICADO", "DESIERTO", "CANCELADO",
    "CONCLUIDO", "NULO", "SUSPENDIDO"
]

DEPARTAMENTOS_PERU = [
    "AMAZONAS", "ANCASH", "APURIMAC", "AREQUIPA", "AYACUCHO", "CAJAMARCA", "CALLAO",
    "CUSCO", "HUANCAVELICA", "HUANUCO", "ICA", "JUNIN", "LA LIBERTAD",
    "LAMBAYEQUE", "LIMA", "LORETO", "MADRE DE DIOS", "MOQUEGUA", "PASCO",
    "PIURA", "PUNO", "SAN MARTIN", "TACNA", "TUMBES", "UCAYALI"
]

def limpiar_texto(texto: str) -> str:
    if not texto: return ""
    return re.sub(r'\s+', ' ', texto).strip()

def parse_fecha_regex(texto_completo: str):
    match = re.search(r"Publicaci[oó]n.*?(\d{2}/\d{2}/\d{4})", texto_completo, re.IGNORECASE)
    if match:
        return datetime.strptime(match.group(1), "%d/%m/%Y")

    match_gen = re.search(r"(\d{2}/\d{2}/\d{4})", texto_completo)
    if match_gen:
        try:
            return datetime.strptime(match_gen.group(1), "%d/%m/%Y")
        except:
            return None
    return None

def extraer_tipo_exacto(desc: str) -> str:
    if not isinstance(desc, str): return "Otro"
    d_upper = desc.upper().strip()

    if d_upper.startswith("BIEN") or "BIEN:" in d_upper: return "Bien"
    if d_upper.startswith("SERVICIO") or "SERVICIO:" in d_upper: return "Servicio"
    if d_upper.startswith("OBRA") or "OBRA:" in d_upper: return "Obra"
    if "CONSULTOR" in d_upper: return "Consultoría"

    keywords_obra = ["MEJORAMIENTO", "CREACION", "REHABILITACION", "CONSTRUCCION", "INSTALACION", "EJECUCION DE OBRA"]
    if any(k in d_upper for k in keywords_obra): return "Obra"

    keywords_servicio = ["MANTENIMIENTO", "ALQUILER", "SEGURIDAD", "VIGILANCIA", "LIMPIEZA", "TRANSPORTE", "SEGURO", "LOCACION", "CONFECCION", "SERVICIO"]
    if any(k in d_upper for k in keywords_servicio): return "Servicio"

    keywords_bien = ["ADQUISICION", "COMPRA", "SUMINISTRO", "CAMIONETA", "COMBUSTIBLE", "EQUIPO", "MATERIAL", "INSUMO"]
    if any(k in d_upper for k in keywords_bien): return "Bien"

    return "Otro"

def inferir_region(entidad: str, texto_tarjeta: str) -> str:
    texto_busqueda = (entidad + " " + texto_tarjeta).upper()
    match_ubi = re.search(r"UBICACI[ÓO]N[:\s]+([^:\n]+)", texto_busqueda)
    if match_ubi:
        posible_ubi = match_ubi.group(1)
        for d in DEPARTAMENTOS_PERU:
            if d in posible_ubi:
                return posible_ubi.strip()
    for d in DEPARTAMENTOS_PERU:
        if d in texto_busqueda:
            return d
    return "NO IDENTIFICADO"

async def run_scraper(fecha_inicio_str: str, fecha_fin_str: str, max_items: int, user_phone: str = None):
    items_data = []

    try:
        f_inicio = datetime.strptime(fecha_inicio_str, "%d/%m/%Y")
        f_fin = datetime.strptime(fecha_fin_str, "%d/%m/%Y")
        f_fin = f_fin.replace(hour=23, minute=59, second=59)
    except ValueError:
        logger.error("Formato de fecha incorrecto. Use dd/mm/yyyy")
        return []

    logger.info(f"🔎 OBJETIVO: {f_inicio.date()} a {f_fin.date()} | Usuario: {user_phone}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-gpu", "--single-process"]
        )
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()

        try:
            logger.info("🌍 Navegando a SEACE...")
            await page.goto(SEACE_URL, timeout=60000)

            try:
                await page.wait_for_selector('div[class*="bg-fondo-section"]', timeout=30000)
            except:
                logger.warning("⚠ Alerta: Carga lenta detectada.")

            try:
                await page.wait_for_timeout(2000)
                await page.get_by_role("combobox").click()
                await page.get_by_text("100", exact=True).click()
                logger.info("✅ Cambiado a 100 resultados por página.")
                await asyncio.sleep(5)
            except Exception as e:
                logger.warning(f"No se pudo cambiar a 100 resultados: {e}")

            page_count = 1
            max_paginas = 300
            stop_scraping = False
            limit_count = max_items if max_items is not None and max_items > 0 else 999999

            while page_count <= max_paginas and not stop_scraping:
                if len(items_data) >= limit_count:
                    break

                logger.info(f"📄 Procesando PÁGINA {page_count} | Llevamos: {len(items_data)}")

                cards = await page.query_selector_all('div[class*="bg-fondo-section"]')
                if not cards:
                    break

                items_added_this_page = 0

                for card in cards:
                    if len(items_data) >= limit_count:
                        break

                    try:
                        text_content = await card.inner_text()

                        fecha_obj = parse_fecha_regex(text_content)
                        if not fecha_obj: continue

                        if fecha_obj > f_fin: continue
                        if fecha_obj < f_inicio:
                            logger.info(f"🛑 Fecha límite alcanzada ({fecha_obj.date()}). Fin.")
                            stop_scraping = True
                            break

                        raw_lines = [l.strip() for l in text_content.split('\n') if l.strip()]
                        clean_lines = [line for line in raw_lines if line.upper() not in ESTADOS_IGNORAR]

                        nomenclatura = clean_lines[0] if len(clean_lines) > 0 else "S/D"
                        entidad = clean_lines[1] if len(clean_lines) > 1 else "S/D"
                        descripcion = clean_lines[2] if len(clean_lines) > 2 else "S/D"

                        for line in clean_lines:
                            if re.match(r"^(Bien|Servicio|Obra|Consultor)", line, re.IGNORECASE):
                                descripcion = line
                                break

                        region = inferir_region(entidad, text_content)
                        tipo_objeto = extraer_tipo_exacto(descripcion)

                        html = await card.inner_html()
                        soup = BeautifulSoup(html, "html.parser")
                        link_elem = soup.select_one("a[href*='/buscador-publico/contrataciones/']")
                        enlace = urljoin(SEACE_URL, link_elem["href"]) if link_elem else None

                        if not enlace: continue

                        items_data.append({
                            "nomenclatura": nomenclatura,
                            "entidad_solicitante": entidad,
                            "descripcion": descripcion,
                            "objeto": tipo_objeto,
                            "region": region,
                            # --- CORRECCIÓN AQUÍ: Formato SQL YYYY-MM-DD ---
                            "fecha_publicacion": fecha_obj.strftime("%Y-%m-%d"),
                            "moneda": "SOLES",
                            "descripcion_item": descripcion,
                            "url": enlace,
                            "user_phone": user_phone
                        })
                        items_added_this_page += 1

                    except Exception:
                        continue

                logger.info(f"   --> Agregados: {items_added_this_page}")

                if stop_scraping:
                    break

                next_btn = await page.query_selector("button[aria-label*='iguiente']:not([disabled])")
                if next_btn:
                    await next_btn.click()
                    await asyncio.sleep(2)
                    page_count += 1
                else:
                    break

        except Exception as e:
            logger.error(f"Error crítico: {e}")
            raise
        finally:
            await browser.close()

    logger.info(f"✅ FIN. Total recolectado: {len(items_data)}")
    return items_data