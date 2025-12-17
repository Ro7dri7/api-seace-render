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

# --- LISTA DE DEPARTAMENTOS PARA DETECCIÓN INTELIGENTE ---
DEPARTAMENTOS_PERU = [
    "AMAZONAS", "ANCASH", "APURIMAC", "AREQUIPA", "AYACUCHO", "CAJAMARCA", "CALLAO",
    "CUSCO", "HUANCAVELICA", "HUANUCO", "ICA", "JUNIN", "LA LIBERTAD",
    "LAMBAYEQUE", "LIMA", "LORETO", "MADRE DE DIOS", "MOQUEGUA", "PASCO",
    "PIURA", "PUNO", "SAN MARTIN", "TACNA", "TUMBES", "UCAYALI"
]

def limpiar_texto(texto: str) -> str:
    """Elimina espacios extra y saltos de línea."""
    if not texto: return ""
    return re.sub(r'\s+', ' ', texto).strip()

def extraer_tipo(desc: str) -> str:
    """Deduce el Objeto (Bien, Servicio, Obra) basado en la descripción."""
    if not isinstance(desc, str): return "Otro"
    d = desc.lower()
    if "bien" in d or d.startswith("b:"): return "Bien"
    elif "servicio" in d or d.startswith("s:"): return "Servicio"
    elif "obra" in d or d.startswith("o:"): return "Obra"
    elif "consultor" in d: return "Consultoría"
    else: return "Otro"

def inferir_region(entidad: str, texto_tarjeta: str) -> str:
    """
    Intenta extraer la ubicación del texto de la tarjeta.
    Si no la encuentra, la deduce del nombre de la Entidad.
    """
    texto_busqueda = (entidad + " " + texto_tarjeta).upper()

    # 1. Buscar etiqueta explícita "Ubicación:"
    match_ubi = re.search(r"UBICACI[ÓO]N[:\s]+([^:\n]+)", texto_busqueda)
    if match_ubi:
        posible_ubi = match_ubi.group(1)
        for d in DEPARTAMENTOS_PERU:
            if d in posible_ubi:
                return posible_ubi.strip()

    # 2. Si falla, buscar el nombre del departamento en la Entidad
    for d in DEPARTAMENTOS_PERU:
        if d in texto_busqueda:
            return d

    return "NO IDENTIFICADO"

def extraer_fechas_cronograma(texto_tarjeta: str):
    """
    Extrae fecha de inicio y fin de presentación de ofertas usando Regex.
    Retorna None si no encuentra alguna para poder filtrar después.
    """
    fechas = {"inicio": None, "fin": None}

    # Patrón para fechas dd/mm/yyyy HH:mm
    regex_fecha = r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})"

    # Buscar Fecha Inicio (Presentación)
    match_ini = re.search(r"(?:Inicio|Desde|Presentación).*?" + regex_fecha, texto_tarjeta, re.IGNORECASE)
    if match_ini:
        fechas["inicio"] = match_ini.group(1)

    # Buscar Fecha Fin (Cierre)
    match_fin = re.search(r"(?:Fin|Hasta|Cierre|Límite).*?" + regex_fecha, texto_tarjeta, re.IGNORECASE)
    if match_fin:
        fechas["fin"] = match_fin.group(1)

    return fechas

async def run_scraper(fecha_inicio_str: str, fecha_fin_str: str, max_items: int):
    items_data = []

    # 1. Preparar fechas (Conversión)
    try:
        f_inicio = datetime.strptime(fecha_inicio_str, "%d/%m/%Y")
        f_fin = datetime.strptime(fecha_fin_str, "%d/%m/%Y")
        f_fin = f_fin.replace(hour=23, minute=59, second=59)
    except ValueError:
        logger.error("Formato de fecha incorrecto.")
        return []

    logger.info(f"🔎 OBJETIVO: Buscar licitaciones entre {f_inicio} y {f_fin}")

    async with async_playwright() as p:
        # Lanzamiento optimizado para servidor (headless)
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-gpu", "--single-process"]
        )
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()

        try:
            logger.info("🌍 Navegando a SEACE...")
            await page.goto(SEACE_URL, timeout=60000)

            # Esperar a que cargue la lista inicial
            try:
                await page.wait_for_selector('div[class*="bg-fondo-section"]', timeout=30000)
            except:
                logger.warning("Carga lenta, intentando continuar...")

            # Intentar cambiar a 100 resultados para acelerar la paginación
            try:
                await page.get_by_role("combobox").click()
                await page.get_by_text("100").click()
                await asyncio.sleep(3)
            except:
                pass

            page_count = 1
            max_paginas = 300
            stop_scraping = False
            # Si no hay límite, ponemos uno muy alto
            limit_count = max_items if max_items is not None and max_items > 0 else 999999

            while page_count <= max_paginas and not stop_scraping:
                if len(items_data) >= limit_count:
                    break

                logger.info(f"📄 Procesando PÁGINA {page_count} | Recolectados (Válidos): {len(items_data)}")

                cards = await page.query_selector_all('div[class*="bg-fondo-section"]')
                if not cards:
                    break

                for card in cards:
                    if len(items_data) >= limit_count:
                        break

                    try:
                        text_content = await card.inner_text()
                        html_content = await card.inner_html()
                        soup = BeautifulSoup(html_content, "html.parser")
                        p_tags = soup.select("p")

                        # --- 1. Detectar Fecha de Publicación ---
                        match_pub = re.search(r"Publicaci[oó]n.*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})", text_content, re.IGNORECASE)
                        if not match_pub:
                            match_pub = re.search(r"(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2})", text_content)

                        if not match_pub:
                            continue # Sin fecha base, no sirve

                        fecha_pub_str = match_pub.group(1)
                        fecha_obj = datetime.strptime(fecha_pub_str, "%d/%m/%Y %H:%M")

                        # --- 2. Validar Rango de Fechas ---
                        if fecha_obj > f_fin:
                            continue # Muy nuevo
                        if fecha_obj < f_inicio:
                            stop_scraping = True
                            break

                        # --- 3. Extraer Datos Básicos ---
                        nomenclatura = limpiar_texto(p_tags[0].get_text()) if len(p_tags) > 0 else "S/D"
                        entidad = limpiar_texto(p_tags[1].get_text()) if len(p_tags) > 1 else "S/D"
                        descripcion = limpiar_texto(p_tags[2].get_text()) if len(p_tags) > 2 else "S/D"

                        fechas_crono = extraer_fechas_cronograma(text_content)
                        region = inferir_region(entidad, text_content)

                        link_elem = soup.select_one("a[href*='/buscador-publico/contrataciones/']")
                        enlace = urljoin(SEACE_URL, link_elem["href"]) if link_elem else None

                        # =================================================================
                        # 4. FILTRO DE CALIDAD (Omitir valores nulos)
                        # =================================================================

                        # Si no tiene cronograma completo (inicio y fin), descartar
                        if not fechas_crono["inicio"] or not fechas_crono["fin"]:
                            continue

                        # Si no tiene URL, descartar
                        if not enlace:
                            continue

                        # Si la nomenclatura o entidad no se leyeron bien
                        if nomenclatura == "S/D" or entidad == "S/D":
                            continue
                        # =================================================================

                        items_data.append({
                            "nomenclatura": nomenclatura,
                            "entidad_solicitante": entidad,
                            "descripcion": descripcion,
                            "objeto": extraer_tipo(descripcion),
                            "region": region,
                            "fecha_publicacion": fecha_pub_str,
                            "fecha_inicio": fechas_crono["inicio"],
                            "fecha_fin": fechas_crono["fin"],
                            "moneda": "SOLES",
                            "valor_referencial": "---",
                            "descripcion_item": descripcion,
                            "url": enlace
                        })

                    except Exception as e:
                        continue

                if stop_scraping:
                    break

                # Paginación
                next_btn = await page.query_selector("button[aria-label*='iguiente']:not([disabled])")
                if next_btn:
                    await next_btn.click()
                    await asyncio.sleep(1.5)
                    await page.wait_for_timeout(500)
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