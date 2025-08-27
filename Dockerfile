# ───────────────────────────────
# Builder: use Python 3.11 so ABI matches runtime
# ───────────────────────────────
FROM python:3.11-slim AS builder
ARG DEBIAN_FRONTEND=noninteractive

# Build deps + headers for pybind11
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake ninja-build git curl ca-certificates pkg-config \
    python3-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy sources (pybind binding lives at ../python/olob/_bindings.cpp)
COPY cpp/ /src/cpp/
COPY python/ /src/python/
COPY CMakeLists.txt /src/CMakeLists.txt

# Configure & build inner project WITH pybind ON (ABI = cp311)
RUN cmake -S cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release \
          -DLOB_BUILD_TESTS=OFF -DLOB_PROFILING=ON -DLOB_BUILD_PYBIND=ON \
 && cmake --build build/cpp -j

# Normalize pybind filename (copy to a stable path)
RUN bash -lc 'set -euo pipefail; f=$(ls build/cpp/_lob*.so | head -n1); cp "$f" build/cpp/_lob.so'

# ───────────────────────────────
# Runtime: also Python 3.11, to match ABI
# ───────────────────────────────
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/usr/local/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    git tini ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Python deps for analytics + capture + backtests
RUN pip install --no-cache-dir \
    numpy pandas pyarrow matplotlib scikit-learn rich click fastparquet \
    aiohttp pyyaml

WORKDIR /app

# Copy analytics package
COPY python/ /app/python/
ENV PYTHONPATH=/app/python

# Copy C++ artifacts
COPY --from=builder /src/build/cpp/tools/replay_tool /usr/local/bin/replay_tool
COPY --from=builder /src/build/cpp/_lob.so /app/python/olob/_lob.so

# Provide `lob` CLI entrypoint  ✅ NOTE: no backslashes in "$@"
RUN printf '#!/usr/bin/env bash\nexec python -m olob.cli "$@"\n' > /usr/local/bin/lob \
 && chmod +x /usr/local/bin/lob

VOLUME ["/data", "/out"]
ENTRYPOINT ["/usr/bin/tini","--","/usr/local/bin/lob"]
