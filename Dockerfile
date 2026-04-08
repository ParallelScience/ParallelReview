FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY review_browse/ review_browse/
COPY main.py wsgi.py ./
COPY scripts/ scripts/

RUN pip install --no-cache-dir -e .

RUN useradd -m reviewer
USER reviewer

ENV GUNICORN_CMD="gunicorn --bind :8080 --workers 5 --threads 10 --timeout 0 --preload wsgi:app"
EXPOSE 8080
CMD exec $GUNICORN_CMD
