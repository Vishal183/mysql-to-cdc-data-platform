FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless procps curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

COPY requirements.txt /opt/consumer/requirements.txt
RUN pip install --no-cache-dir -r /opt/consumer/requirements.txt

COPY *.py /opt/consumer/
COPY entrypoint.sh /opt/consumer/entrypoint.sh
RUN chmod +x /opt/consumer/entrypoint.sh

WORKDIR /opt/consumer
ENV PYTHONUNBUFFERED=1

CMD ["./entrypoint.sh"]
