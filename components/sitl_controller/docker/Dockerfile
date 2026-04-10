FROM sitl-module-controller:latest

WORKDIR /app

COPY . .

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "-m", "components.sitl_controller"]
