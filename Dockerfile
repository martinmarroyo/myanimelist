FROM python:3.10-slim

WORKDIR /myanimelist

RUN mkdir -p ingest database transform
COPY ./ingest ingest
COPY ./database database
COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["tail", "-f", "/dev/null"]