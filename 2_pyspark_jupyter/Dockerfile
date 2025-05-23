FROM bitnami/spark:latest
WORKDIR /app

# Step 2: Copy requirements.txt
COPY ./requirements.txt /app

USER root
# Step 3: Install the specified packages
RUN pip install -r requirements.txt


COPY ./entrypoint.sh /app

EXPOSE 8888

ENTRYPOINT /app/entrypoint.sh