# Usamos la imagen oficial de Microsoft Playwright con Python ya instalado
FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

# Directorio de trabajo
WORKDIR /app

# Copiamos requerimientos
COPY requirements.txt .

# Instalamos dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Instalamos los navegadores de Playwright explícitamente (por seguridad)
RUN playwright install chromium
RUN playwright install-deps

# Copiamos el resto del código
COPY . .

# Exponemos el puerto (aunque Render lo ignora, es buena práctica)
EXPOSE 8000

# Comando para iniciar la aplicación
CMD ["python", "main.py"]