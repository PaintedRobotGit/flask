import multiprocessing

# Bind to the port provided by Railway
import os
port = os.getenv("PORT", "8000")
bind = f"0.0.0.0:{port}"

# Workers and threads
# Use a few workers with multiple threads to handle blocking I/O to external APIs
workers = int(os.getenv("WEB_CONCURRENCY", str(max(1, multiprocessing.cpu_count()))))
threads = int(os.getenv("GUNICORN_THREADS", "4"))

# Timeouts
# Increase to allow upstream API calls + tool use. Railway proxy often disconnects ~55s on free tier,
# but for paid tiers/custom domains longer is fine. Set generously.
timeout = int(os.getenv("GUNICORN_TIMEOUT", "600"))            # seconds
graceful_timeout = int(os.getenv("GUNICORN_GRACEFUL_TIMEOUT", "60"))
keepalive = int(os.getenv("GUNICORN_KEEPALIVE", "5"))

# Worker class: sync is OK with threads for requests
worker_class = os.getenv("GUNICORN_WORKER_CLASS", "gthread")

# Logging
accesslog = "-"
errorlog = "-"
loglevel = os.getenv("GUNICORN_LOG_LEVEL", "info")


