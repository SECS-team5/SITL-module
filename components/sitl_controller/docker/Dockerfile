FROM python:3.13-slim

WORKDIR /app

COPY systems/SITL-module/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --timeout 60 --retries 3 -r requirements.txt

COPY broker/ ./broker/          
COPY sdk/ ./sdk/                
COPY systems/SITL-module/ ./    

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "-m", "components.sitl_controller"]
