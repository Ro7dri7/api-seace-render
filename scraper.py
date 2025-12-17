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

DEPARTAMENTOS_PERU = [
    "AMAZONAS", "ANCASH", "APURIMAC", "AREQUIPA", "AYACUCHO", "CAJAMARCA", "CALLAO",
    "CUSCO", "HUANCAVELICA", "HUANUCO", "ICA", "JUNIN", "LA LIBERTAD",
    "LAMBAYEQUE", "LIMA", "LORETO", "MADRE DE DIOS", "MOQUEGUA", "PASCO",
    "PIURA", "PUNO", "SAN MARTIN", "TACNA", "TUMBES", "UCAYALI"
]

def limpiar_texto(texto: str) -> str:
    if not texto: return ""
    return re.sub(r'\s+', ' ', texto).strip()

def extraer_tipo(desc: str) -> str:
    if not isinstance(desc, str): return "Otro"
    d = desc.lower()
    if "bien" in d or d.startswith("b:"): return "Bien"
    elif "servicio" in d or d.startswith("s:"): return "Servicio"
    elif "obra" in d or d.startswith("o:"): return "Obra"
    elif "consultor" in d: return "Consultoría"
    else: return "Otro"

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

def extraer_fechas_cronograma(texto_tarjeta: str):
    """
    Extrae fecha de inicio y fin.
    MEJORA: El Regex ahora es opcional con la hora para no fallar si solo hay fecha.
    """
    fechas = {"inicio": None, "fin": None}

    # Regex busca dd/mm/yyyy, ignorando si hay hora o no
    regex_fecha = r"(\d{2}/\d{2}/\d{4})"

    match_ini = re.search(r"(?:Inicio|Desde|Presentación).*?" + regex_fecha, texto_tarjeta, re.IGNORECASE)
    if match_ini:
        fechas["inicio"] = match_ini.group(1) # Solo la fecha

    match_fin = re.search(r"(?:Fin|Hasta|Cierre|Límite).*?" + regex_fecha, texto_tarjeta, re.IGNORECASE)
    if match_fin:
        fechas["fin"] = match_fin.group(1) # Solo la fecha

    return fechas

async def run_scraper(fecha_inicio_str: str, fecha_fin_str: str, max_items: int):
    items_data = []

    try:
        f_inicio = datetime.strptime(fecha_inicio_str, "%d/%m/%Y")
        f_fin = datetime.strptime(fecha_fin_str, "%d/%m/%Y")
        f_fin = f_fin.replace(hour=23, minute=59, second=59)
    except ValueError:
        logger.error("Formato de fecha incorrecto.")
        return []

    logger.info(f"🔎 OBJETIVO: {f_inicio.date()} a {f_fin.date()}")

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
                logger.warning("Carga lenta o sin resultados iniciales.")

            try:
                # Intentar poner 100 items por página
                await page.get_by_role("combobox").click()
                await page.get_by_text("100").click()
                await asyncio.sleep(4)
            except:
                pass

            page_count = 1
            max_paginas = 20 # Reducido para evitar loops infinitos si falla la detección
            stop_scraping = False
            limit_count = max_items if max_items and max_items > 0 else 9999

            while page_count <= max_paginas and not stop_scraping:
                if len(items_data) >= limit_count:
                    break

                logger.info(f"📄 PÁGINA {page_count} | Capturados: {len(items_data)}")

                cards = await page.query_selector_all('div[class*="bg-fondo-section"]')
                if not cards:
                    logger.warning("No se encontraron tarjetas en esta página.")
                    break

                items_en_pagina = 0

                for card in cards:
                    if len(items_data) >= limit_count:
                        break

                    try:
                        text_content = await card.inner_text()
                        html_content = await card.inner_html()
                        soup = BeautifulSoup(html_content, "html.parser")
                        p_tags = soup.select("p")

                        # --- 1. Detectar Fecha Publicación ---
                        # Regex mejorado para capturar fecha aunque el formato varíe
                        match_pub = re.search(r"(\d{2}/\d{2}/\d{4})", text_content)

                        if not match_pub:
                            # logger.warning("Saltando: No se encontró fecha de publicación")
                            continue

                        fecha_pub_str = match_pub.group(1)
                        # Añadimos hora 00:00 por defecto para comparar
                        fecha_obj = datetime.strptime(fecha_pub_str, "%d/%m/%Y")

                        # --- 2. Validar Rango ---
                        # Nota: SEACE ordena desc (más nuevo primero).
                        if fecha_obj > f_fin:
                            continue # Es más nuevo que lo que queremos, sigue buscando

                        if fecha_obj < f_inicio:
                            logger.info(f"🛑 Encontrada fecha antigua ({fecha_obj.date()}). Deteniendo.")
                            stop_scraping = True
                            break

                        # --- 3. Extraer Datos ---
                        nomenclatura = limpiar_texto(p_tags[0].get_text()) if len(p_tags) > 0 else "S/D"
                        entidad = limpiar_texto(p_tags[1].get_text()) if len(p_tags) > 1 else "S/D"
                        descripcion = limpiar_texto(p_tags[2].get_text()) if len(p_tags) > 2 else "S/D"

                        fechas_crono = extraer_fechas_cronograma(text_content)
                        region = inferir_region(entidad, text_content)

                        link_elem = soup.select_one("a[href*='/buscador-publico/contrataciones/']")
                        enlace = urljoin(SEACE_URL, link_elem["href"]) if link_elem else None

                        # =================================================================
                        # 4. FILTRO DE CALIDAD (RELAJADO)
                        # =================================================================

                        # SOLO rechazamos si no hay URL.
                        # Aceptamos items sin cronograma exacto para no perder datos.
                        if not enlace:
                            continue

                        # Si quieres depurar, descomenta esto:
                        # logger.info(f"Capturado: {nomenclatura} - {fecha_pub_str}")

                        items_data.append({
                            "nomenclatura": nomenclatura,
                            "entidad_solicitante": entidad,
                            "descripcion": descripcion,
                            "objeto": extraer_tipo(descripcion),
                            "region": region,
                            "fecha_publicacion": fecha_pub_str,
                            "fecha_inicio": fechas_crono["inicio"] if fechas_crono["inicio"] else "Ver Link",
                            "fecha_fin": fechas_crono["fin"] if fechas_crono["fin"] else "Ver Link",
                            "moneda": "SOLES",
                            "valor_referencial": "---",
                            "descripcion_item": descripcion,
                            "url": enlace
                        })
                        items_en_pagina += 1

                    except Exception as e:
                        logger.error(f"Error parseando tarjeta: {e}")
                        continue

                logger.info(f"   --> Agregados en esta pag: {items_en_pagina}")

                if stop_scraping:
                    break

                # Paginación
                next_btn = await page.query_selector("button[aria-label*='iguiente']:not([disabled])")
                if next_btn:
                    await next_btn.click()
                    await asyncio.sleep(3) # Espera un poco más por seguridad
                    page_count += 1
                else:
                    logger.info("No hay botón siguiente o está deshabilitado.")
                    break

        except Exception as e:
            logger.error(f"Error crítico: {e}")
            raise
        finally:
            await browser.close()

    logger.info(f"✅ FIN. Total recolectado: {len(items_data)}")
    return items_data