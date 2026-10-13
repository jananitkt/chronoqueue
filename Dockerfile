FROM python:3.13-alpine

WORKDIR /app

# Zero external dependencies — copies clean Python package
COPY chronoqueue/ /app/chronoqueue/

# Create a data directory for persistent SQLite storage
RUN mkdir -p /app/data

ENV PYTHONUNBUFFERED=1
EXPOSE 9324

# Volume mount point for data persistence across restarts/rebuilds
VOLUME ["/app/data"]

CMD ["python", "-m", "chronoqueue", "server", "--port", "9324", "--db", "/app/data/chronoqueue.db"]
