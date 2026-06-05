FROM apache/spark:3.5.3-python3

USER root
WORKDIR /app
ENV HOME=/tmp
ENV SPARK_LOCAL_DIRS=/tmp/spark-local
ENV SPARK_SUBMIT_OPTS="-Duser.home=/tmp"
ENV JAVA_TOOL_OPTIONS="-Duser.home=/tmp"

RUN pip install --no-cache-dir matplotlib==3.7.5 pandas==2.0.3

COPY src /app/src

RUN mkdir -p /outputs

ENTRYPOINT ["/opt/spark/bin/spark-submit", "--driver-memory", "4g", "--conf", "spark.driver.maxResultSize=1g", "/app/src/collision_detection.py"]
