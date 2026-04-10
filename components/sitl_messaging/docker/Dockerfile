FROM sitl-module-messaging:latest

WORKDIR /app

COPY . .

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "-m", "components.sitl_messaging"]
