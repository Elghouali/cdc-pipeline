# ─────────────────────────────────────────────────────────────
#  Dashboard CDC — image legere basee sur Python 3.11
# ─────────────────────────────────────────────────────────────
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=5000

WORKDIR /app

# Dependances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Code applicatif + frontend statique
COPY app.py .
COPY static ./static

EXPOSE 5000

# Gunicorn en prod : 2 workers + threads pour les requetes Trino simultanees
CMD ["gunicorn", "-w", "2", "--threads", "4", "-b", "0.0.0.0:5000", "app:app"]
