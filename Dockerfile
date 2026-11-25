FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/app/data

WORKDIR /app/project

COPY pyproject.toml ./
RUN python -c "import tomllib; print(*tomllib.load(open('pyproject.toml', 'rb'))['project']['dependencies'], sep=chr(10))" > /tmp/requirements.txt \
    && python -m pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

COPY src/ ./src/
RUN python -m pip install --no-cache-dir --no-deps . \
    && python -c "import sqlite3; sqlite3.connect(':memory:').execute('CREATE VIRTUAL TABLE test_fts USING fts5(content)')"

COPY --chown=10001:10001 models.example.yaml ./models.yaml
COPY --chown=10001:10001 prompts/persona.example.yaml ./prompts/persona.yaml
COPY --chown=10001:10001 prompts/style_guide.md ./prompts/style_guide.md

RUN groupadd --gid 10001 companion \
    && useradd --uid 10001 --gid companion --no-create-home companion \
    && install -d -m 0700 -o companion -g companion /app/data

USER companion
ENTRYPOINT ["python", "-m", "aibottelegram"]
CMD ["run"]
