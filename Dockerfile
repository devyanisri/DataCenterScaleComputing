FROM python:3.8

WORKDIR /app

COPY pipeline.py pipeline_c.py
COPY shelter_data.csv shelter_data.csv

RUN pip install pandas sqlalchemy psycopg2

ENTRYPOINT [ "python","pipeline.py" ]