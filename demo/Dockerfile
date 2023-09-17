FROM python:3.8

WORKDIR /app

COPY pipeline.py pipeline_c.py

RUN pip install pandas
RUN python pipeline_c.py https://shelterdata.s3.amazonaws.com/shelterdata1000.csv target.csv

ENTRYPOINT [ "bash" ]