FROM astrocrpublic.azurecr.io/runtime:3.0-10

USER root

# Install OpenJDK-17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER astro

RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake \
    snowflake-connector-python \
    snowflake-sqlalchemy \
    pandas \
    sqlalchemy \
    pyspark \
    python-dotenv \
    pyarrow \
    apache-airflow-providers-dbt-cloud
