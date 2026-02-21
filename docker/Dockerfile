FROM python:3.12-slim

WORKDIR /app

# Зависимости
COPY docker/requirements.txt ./requirements.txt     
RUN python -m pip install --no-cache-dir -U pip \
    && python -m pip install --no-cache-dir -r requirements.txt

# Код + данные
COPY . .                      

CMD ["python", "run_demo.py"]               