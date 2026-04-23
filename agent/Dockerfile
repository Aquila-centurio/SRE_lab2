FROM postgres:17

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    iptables \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --break-system-packages -r requirements.txt

COPY agent.py .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

EXPOSE 5432 8080

ENTRYPOINT ["/app/entrypoint.sh"]