import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re

# ✅ Corregido: sin espacios al final
SEACE_URL = "https://prod6.seace.gob.pe/buscador-publico/contrataciones"

def parse_fecha_seace(fecha_str: str):
    try:
        fecha_str = fecha_str.replace("Fecha de publicación:", "").strip()
        return datetime.strptime(fecha_str, "%d/%m/%Y %H:%M:%S")
    except:
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
    """Extrae el CUBSO de una URL específica"""
    if url == "No disponible":
        return "No disponible"
    try:
        await page.goto(url, timeout=20000)
        # ✅ Espera a que la página cargue contenido
        await page.wait_for_selector("body", timeout=10000)
        content = await page.content()
        soup = BeautifulSoup(content, "html.parser")

        # Estrategia 1: Buscar celda específica
        for cell in soup.find_all("td", class_=re.compile(r".*codCubso.*", re.I)):
            txt = cell.get_text(strip=True)
            if txt.isdigit() and 13 <= len(txt) <= 16:
                return txt

        # Estrategia 2: Regex general
        match = re.search(r"\b\d{13,16}\b", soup.get_text())
        if match:
            return match.group()

        return "No encontrado"
    except Exception as e:
        print(f"Error extrayendo CUBSO: {e}")
        return "Error"

async def run_scraper(fecha_inicio: str, fecha_fin: str, max_items: int, include_cubso: bool):
    items_data = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-gpu", "--disable-dev-shm-usage"]
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080}
        )
        page = await context.new_page()

        try:
            print(f"Navegando a SEACE: {fecha_inicio} - {fecha_fin}")
            await page.goto(SEACE_URL, timeout=60000)
            # ✅ Espera a que aparezcan los primeros resultados
            await page.wait_for_selector("div.bg-fondo-section.rounded-md.p-5.ng-star-inserted", timeout=30000)

            # Intentar cambiar a 100 resultados por página
            try:
                select = await page.query_selector("mat-select[aria-labelledby*='mat-paginator-page-size-label']")
                if select:
                    await select.click()
                    opt = await page.query_selector("mat-option:has-text('100')")
                    if opt:
                        await opt.click()
                    # Esperar a que la página se actualice
                    await page.wait_for_selector("div.bg-fondo-section.rounded-md.p-5.ng-star-inserted", timeout=15000)
            except Exception as e:
                print(f"No se pudo cambiar a 100 resultados: {e}")

            page_count = 1
            max_paginas = 10

            while page_count <= max_paginas and len(items_data) < max_items:
                print(f"Procesando página {page_count}...")
                cards = await page.query_selector_all("div.bg-fondo-section.rounded-md.p-5.ng-star-inserted")
                if not cards:
                    print("No se encontraron más tarjetas.")
                    break

                for card in cards:
                    if len(items_data) >= max_items:
                        break
                    try:
                        html = await card.inner_html()
                        soup = BeautifulSoup(html, "html.parser")
                        p_tags = soup.select("p")

                        fecha_raw = "No disponible"
                        for p in p_tags:
                            if "Fecha de publicación:" in p.get_text():
                                fecha_raw = p.get_text(strip=True)
                                break

                        if not fecha_en_rango(fecha_raw, fecha_inicio, fecha_fin):
                            continue

                        codigo = p_tags[0].get_text(strip=True) if len(p_tags) > 0 else "N/A"
                        entidad = p_tags[1].get_text(strip=True) if len(p_tags) > 1 else "N/A"
                        desc = p_tags[2].get_text(strip=True) if len(p_tags) > 2 else "N/A"

                        link_elem = soup.select_one("a[href*='/buscador-publico/contrataciones/']")
                        enlace = urljoin(SEACE_URL, link_elem["href"]) if link_elem else "No disponible"

                        item = {
                            "codigo": codigo,
                            "entidad": entidad,
                            "descripcion": desc,
                            "tipo": extraer_tipo(desc),
                            "fecha_publicacion": fecha_raw.replace("Fecha de publicación:", "").strip(),
                            "enlace": enlace,
                            "cubso": None
                        }
                        items_data.append(item)

                    except Exception as e:
                        continue

                # Verificar si hay página siguiente
                next_btn = await page.query_selector("button.mat-mdc-paginator-navigation-next:not([disabled])")
                if not next_btn:
                    print("No hay más páginas.")
                    break

                await next_btn.click()
                # ✅ Esperar a que carguen los nuevos resultados
                await page.wait_for_selector("div.bg-fondo-section.rounded-md.p-5.ng-star-inserted", timeout=20000)
                page_count += 1

            # Extraer CUBSO si se solicita
            if include_cubso and items_data:
                print(f"Extrayendo CUBSO para {len(items_data)} licitaciones...")
                for item in items_data:
                    if item["enlace"] != "No disponible":
                        item["cubso"] = await get_cubso(page, item["enlace"])
                    else:
                        item["cubso"] = "No enlace"

        finally:
            await browser.close()

    return items_data