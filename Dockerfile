FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN adduser --disabled-password --gecos '' appuser && chown -R appuser /app
USER appuser

EXPOSE 8000

# Note: client IP is derived in-app from X-Forwarded-For via TRUSTED_PROXY_HOPS,
# so we do NOT pass --forwarded-allow-ips=* (which would let any peer spoof
# request.client.host). --proxy-headers stays for correct scheme detection.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]
