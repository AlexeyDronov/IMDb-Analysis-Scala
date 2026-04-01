FROM python:3.8

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Java 11.0.30
RUN ARCH=$(uname -m) &&\
    case "$ARCH" in \
        "amd64" | "x86_64") TARGET_ARCH="x64" ;;\
        "aarch64") TARGET_ARCH="aarch64" ;;\
        "ppc64le") TARGET_ARCH="ppc64le" ;;\
        "s390x") TARGET_ARCH="s390x" ;;\ 
        *) echo "Unsupported arch: $ARCH" && exit 1 ;;\
    esac && \
    mkdir -p /opt/temurin-11 && \
    curl -L -o /tmp/temurin11.tar.gz "https://api.adoptium.net/v3/binary/latest/11/ga/linux/${TARGET_ARCH}/jdk/hotspot/normal/eclipse" && \
    tar -xzf /tmp/temurin11.tar.gz  -C /opt/temurin-11/ --strip-components=1 && \
    rm -rf /tmp/temurin11.tar.gz

ENV JAVA_HOME=/opt/temurin-11
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Install SBT
RUN mkdir -p /opt/sbt &&\
    curl -fSsL https://github.com/sbt/sbt/releases/download/v1.9.6/sbt-1.9.6.tgz | tar xfz - -C /opt && \
    ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt


WORKDIR /app

# Create venv and install PySpark
COPY python/requirements.txt .
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir -r requirements.txt

COPY benchmark.sh entrypoint.sh ./
RUN chmod +x benchmark.sh entrypoint.sh

CMD ["bash", "./entrypoint.sh"]