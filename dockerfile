FROM python:3.10-slim-buster

WORKDIR /src

COPY . .

RUN pip install -r requirements.txt