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

# --- LISTA DE DEPARTAMENTOS ---
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
    """Busca fecha de publicación (dd/mm/yyyy)."""
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

def extraer_tipo(desc: str) -> str:
    """
    Clasifica el objeto basándose en palabras clave si no hay prefijo explícito.
    """
    if not isinstance(desc, str): return "Otro"

    d = desc.upper() # Convertimos a mayúsculas para comparar mejor

    # 1. Prefijos explícitos (Prioridad Alta)
    if d.startswith("BIEN") or "BIEN:" in d: return "Bien"
    if d.startswith("SERVICIO") or "SERVICIO:" in d: return "Servicio"
    if d.startswith("OBRA") or "OBRA:" in d: return "Obra"
    if "CONSULTOR" in d: return "Consultoría"

    # 2. Palabras clave de OBRAS
    # "Mejoramiento", "Creación", "Rehabilitación", "Construcción", "Instalación"
    keywords_obra = ["MEJORAMIENTO", "CREACION", "REHABILITACION", "CONSTRUCCION", "INSTALACION", "RENOVACION", "SALDO DE OBRA"]
    if any(k in d for k in keywords_obra):
        return "Obra"

    # 3. Palabras clave de SERVICIOS
    # "Contratación de servicio", "Mantenimiento", "Alquiler", "Consultoría" (si no cayó antes), "Seguro", "Vigilancia"
    keywords_servicio = ["MANTENIMIENTO", "ALQUILER", "SEGURIDAD", "VIGILANCIA", "LIMPIEZA", "TRANSPORTE", "SEGURO", "LOCACION", "CONFECCION", "SERVICIO"]
    if any(k in d for k in keywords_servicio):
        return "Servicio"

    # 4. Palabras clave de BIENES
    # "Adquisición", "Compra", "Suministro"
    keywords_bien = ["ADQUISICION", "COMPRA", "SUMINISTRO", "CAMIONETA", "COMBUSTIBLE", "EQUIPO", "MATERIAL", "INSUMO"]
    if any(k in d for k in keywords_bien):
        return "Bien"

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

def extraer_fechas_cronograma(texto_tarjeta: str):
    fechas = {"inicio": None, "fin": None}
    regex_fecha = r"(\d{2}/\d{2}/\d{4})"

    match_ini = re.search(r"(?:Inicio|Desde|Presentación).*?" + regex_fecha, texto_tarjeta, re.IGNORECASE)
    if match_ini: fechas["inicio"] = match_ini.group(1)

    match_fin = re.search(r"(?:Fin|Hasta|Cierre|Límite).*?" + regex_fecha, texto_tarjeta, re.IGNORECASE)
    if match_fin: fechas["fin"] = match_fin.group(1)

    return fechas

async def run_scraper(fecha_inicio_str: str, fecha_fin_str: str, max_items: int):
    items_data = []

    try:
        f_inicio = datetime.strptime(fecha_inicio_str, "%d/%m/%Y")
        f_fin = datetime.strptime(fecha_fin_str, "%d/%m/%Y")
        f_fin = f_fin.replace(hour=23, minute=59, second=59)
    except ValueError:
        logger.error("Formato de fecha incorrecto. Use dd/mm/yyyy")
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

                        lines = [l.strip() for l in text_content.split('\n') if l.strip()]

                        nomenclatura = lines[0] if lines else "S/D"
                        entidad = lines[1] if len(lines) > 1 else "S/D"
                        descripcion = lines[2] if len(lines) > 2 else "S/D"

                        region = inferir_region(entidad, text_content)
                        fechas_crono = extraer_fechas_cronograma(text_content)

                        # --- NUEVA CLASIFICACIÓN ---
                        tipo_objeto = extraer_tipo(descripcion)

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
                            "fecha_publicacion": fecha_obj.strftime("%d/%m/%Y"),
                            "fecha_inicio": fechas_crono["inicio"] if fechas_crono["inicio"] else "Ver Link",
                            "fecha_fin": fechas_crono["fin"] if fechas_crono["fin"] else "Ver Link",
                            "moneda": "SOLES",
                            "valor_referencial": "---",
                            "descripcion_item": descripcion,
                            "url": enlace
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