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
    Intenta extraer fecha de inicio y fin, pero NO es obligatorio que existan.
    """
    fechas = {"inicio": None, "fin": None}

    # Regex busca dd/mm/yyyy
    regex_fecha = r"(\d{2}/\d{2}/\d{4})"

    match_ini = re.search(r"(?:Inicio|Desde|Presentación).*?" + regex_fecha, texto_tarjeta, re.IGNORECASE)
    if match_ini:
        fechas["inicio"] = match_ini.group(1)

    match_fin = re.search(r"(?:Fin|Hasta|Cierre|Límite).*?" + regex_fecha, texto_tarjeta, re.IGNORECASE)
    if match_fin:
        fechas["fin"] = match_fin.group(1)

    return fechas

async def run_scraper(fecha_inicio_str: str, fecha_fin_str: str, max_items: int):
    items_data = []

    try:
        f_inicio = datetime.strptime(fecha_inicio_str, "%d/%m/%Y")
        f_fin = datetime.strptime(fecha_fin_str, "%d/%m/%Y")
        # Ajustamos fin del día para cubrir todo el rango
        f_fin = f_fin.replace(hour=23, minute=59, second=59)
    except ValueError:
        logger.error("Formato de fecha incorrecto.")
        return []

    logger.info(f"🔎 OBJETIVO: {f_inicio.date()} a {f_fin.date()}")

    async with async_playwright() as p:
        # Lanzamos browser
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-gpu", "--single-process"]
        )
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()

        try:
            logger.info("🌍 Navegando a SEACE...")
            await page.goto(SEACE_URL, timeout=60000)

            # Esperar carga inicial
            try:
                await page.wait_for_selector('div[class*="bg-fondo-section"]', timeout=30000)
            except:
                logger.warning("No cargaron tarjetas inicialmente.")

            # Intentar cambiar a 100 items por página
            try:
                await page.get_by_role("combobox").click()
                await page.get_by_text("100").click()
                # Aumentamos espera a 5s para asegurar que el DOM se repinte
                await asyncio.sleep(5)
            except:
                logger.warning("No se pudo cambiar a 100 resultados, usando valor por defecto.")

            page_count = 1
            max_paginas = 50
            stop_scraping = False
            # Si max_items es None, ponemos un límite alto
            limit_count = max_items if (max_items and max_items > 0) else 500

            while page_count <= max_paginas and not stop_scraping:
                if len(items_data) >= limit_count:
                    break

                # Re-check de selectores por si el DOM cambió
                cards = await page.query_selector_all('div[class*="bg-fondo-section"]')
                if not cards:
                    logger.info("No se encontraron más tarjetas.")
                    break

                logger.info(f"📄 PÁGINA {page_count} | Encontradas {len(cards)} tarjetas | Total acumulado: {len(items_data)}")

                items_agregados_pag = 0

                for card in cards:
                    if len(items_data) >= limit_count:
                        break

                    try:
                        text_content = await card.inner_text()
                        html_content = await card.inner_html()
                        soup = BeautifulSoup(html_content, "html.parser")
                        p_tags = soup.select("p")

                        # --- 1. Detectar Fecha de Publicación ---
                        # Buscamos explícitamente la palabra "Publicación" para mayor precisión
                        match_pub = re.search(r"Publicaci[oó]n.*?(\d{2}/\d{2}/\d{4})", text_content, re.IGNORECASE)

                        # Fallback: Si no dice "Publicación", buscamos la primera fecha que aparezca
                        if not match_pub:
                            match_pub = re.search(r"(\d{2}/\d{2}/\d{4})", text_content)

                        if not match_pub:
                            # Sin fecha no podemos validar, saltamos
                            continue

                        fecha_pub_str = match_pub.group(1)
                        fecha_obj = datetime.strptime(fecha_pub_str, "%d/%m/%Y")

                        # --- 2. Validar Rango de Fechas ---
                        # SEACE ordena del más nuevo al más antiguo.

                        if fecha_obj > f_fin:
                            # Es una fecha futura (más nueva que nuestro rango), seguimos buscando
                            continue

                        if fecha_obj < f_inicio:
                            # Encontramos una fecha más antigua que el inicio.
                            # DETENEMOS TODO EL SCRAPING (asumiendo orden cronológico)
                            logger.info(f"🛑 Fecha encontrada ({fecha_pub_str}) es anterior al inicio. Deteniendo.")
                            stop_scraping = True
                            break

                        # --- 3. Extraer Datos ---
                        # Usamos "S/D" temporalmente pero NO filtramos por ello
                        nomenclatura = limpiar_texto(p_tags[0].get_text()) if len(p_tags) > 0 else "S/D"
                        entidad = limpiar_texto(p_tags[1].get_text()) if len(p_tags) > 1 else "S/D"
                        descripcion = limpiar_texto(p_tags[2].get_text()) if len(p_tags) > 2 else "S/D"

                        fechas_crono = extraer_fechas_cronograma(text_content)
                        region = inferir_region(entidad, text_content)

                        link_elem = soup.select_one("a[href*='/buscador-publico/contrataciones/']")
                        enlace = urljoin(SEACE_URL, link_elem["href"]) if link_elem else None

                        # --- 4. FILTRO DE CALIDAD MÍNIMO ---
                        # Solo descartamos si NO HAY URL, porque sin url no sirve.
                        if not enlace:
                            continue

                        # NOTA: Ya NO descartamos si faltan fechas de inicio/fin
                        # NOTA: Ya NO descartamos si nomenclatura es S/D

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
                        items_agregados_pag += 1

                    except Exception as e:
                        # Si falla una tarjeta, intentamos con la siguiente
                        continue

                if stop_scraping:
                    break

                # Paginación
                next_btn = await page.query_selector("button[aria-label*='iguiente']:not([disabled])")
                if next_btn:
                    await next_btn.click()
                    # Espera necesaria para que carguen los nuevos items
                    await asyncio.sleep(3)
                    page_count += 1
                else:
                    break

        except Exception as e:
            logger.error(f"Error crítico en navegación: {e}")
            raise
        finally:
            await browser.close()

    logger.info(f"✅ FIN. Total recolectado: {len(items_data)}")
    return items_data