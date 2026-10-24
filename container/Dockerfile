# Non-root Ubuntu 24.04 image of the edge node. Build and run from the repo root:
#   docker build -f container/Dockerfile -t songdo-edge .
#   docker run --rm -p 8000:8000 songdo-edge            # dashboard: http://127.0.0.1:8000
# (or: docker compose -f container/compose.yaml up --build)
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-yaml \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m edge
WORKDIR /app
COPY --chown=edge:edge . /app
# Inside a container the API must listen on all interfaces, or the published port cannot reach it.
RUN sed -i 's/api_host: 127.0.0.1/api_host: 0.0.0.0/' config/node.yaml \
    && mkdir -p data/backups data/run && chown -R edge:edge data
USER edge
ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/status', timeout=2)"
CMD ["python3", "-m", "edge.supervisor", "--config", "config/node.yaml"]
