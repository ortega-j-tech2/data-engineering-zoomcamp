FROM python:3.9

#RUN apt-get install wget
#RUN apt-get install curl

RUN pip install pandas sqlalchemy psycopg2 fastparquet

WORKDIR /app
COPY ingest_data_jon.py ingest_data_jon.py

ENTRYPOINT [ "python","ingest_data_jon.py" ]

