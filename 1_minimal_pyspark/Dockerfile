FROM bitnami/spark:latest

# Optional: install Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy your Spark job
COPY app /opt/app
WORKDIR /opt/app

CMD ["spark-submit", "--master", "local[*]", "job.py"]
