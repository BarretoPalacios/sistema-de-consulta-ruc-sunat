FROM python:3.11-slim

# Ya no instalamos curl porque no descargaremos la BD aquí
WORKDIR /app

# 1. Instalamos dependencias (se mantiene igual)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copiamos el código fuente 
# Asegúrate de que tu .dockerignore ignore el archivo .db local si lo tienes,
# para que no se suba a la imagen por error.
COPY . .

# 3. Comando para arrancar
CMD ["python", "main.py"]