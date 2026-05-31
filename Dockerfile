FROM python:3.11-slim

# uv (single static binary) from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

# 1) Dependencies first — cached unless pyproject.toml / uv.lock change.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# 2) App code + migrations, then install the project itself.
COPY app ./app
COPY migrations ./migrations
RUN uv sync --frozen --no-dev

EXPOSE 8000
CMD ["uv", "run", "--no-dev", "python", "-m", "app.main"]
