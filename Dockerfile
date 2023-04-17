FROM python:3.10-slim-bullseye
RUN apt update
RUN apt install curl mlocate default-jdk -y
RUN pip install --no-cache-dir pandas matplotlib koalas confluent-kafka pyarrow pyspark==3.3.2 pytest delta-spark
RUN mkdir ~/spark_training/
WORKDIR /root/spark_training
