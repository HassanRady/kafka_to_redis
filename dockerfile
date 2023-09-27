FROM python:3.10-slim-buster

WORKDIR /src

COPY main.py .
COPY config.py .
COPY logger.py .
COPY requirements.txt .


RUN pip install -r requirements.txt