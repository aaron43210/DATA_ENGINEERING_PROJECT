FROM python:3.11-slim

WORKDIR /app

# Pick up Debian security fixes and patched packaging tools on every build.
RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/* \
 && pip install --no-cache-dir --upgrade pip setuptools wheel

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Amazon RDS CA bundle, for POSTGRES_SSLMODE=verify-full
ADD --chmod=644 https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem /etc/ssl/rds-global-bundle.pem

RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin app
COPY --chown=root:root . .
USER 10001

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
