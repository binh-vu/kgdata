FROM ubuntu:20.04

RUN apt update
RUN apt install -y software-properties-common
RUN apt install -y wget openjdk-8-jdk build-essential

RUN wget https://mirror.olnevhost.net/pub/apache/spark/spark-3.0.3/spark-3.0.3-bin-hadoop2.7.tgz && \
    tar -xzf spark-3.0.3-bin-hadoop2.7.tgz && rm spark-3.0.3-bin-hadoop2.7.tgz

RUN wget https://repo.anaconda.com/archive/Anaconda3-2021.05-Linux-x86_64.sh && \
    bash Anaconda3-2021.05-Linux-x86_64.sh -b && \
    rm Anaconda3-2021.05-Linux-x86_64.sh

ENV PATH=/root/anaconda3/bin:$PATH
ENV SPARK_HOME=/spark-3.0.3-bin-hadoop2.7

RUN pip install kgdata

RUN cd $SPARK_HOME/sbin && \
    echo 'set -e' >> /start_spark.sh && \
    echo '$SPARK_HOME/sbin/start-master.sh' >> /start_spark.sh && \
    echo '$SPARK_HOME/sbin/start-slave.sh spark://$(hostname):7077' >> /start_spark.sh && \
    echo 'PID=`cat /tmp/spark--org.apache.spark.deploy.worker.Worker-1.pid`' >> /start_spark.sh && \
    echo 'tail --pid=$PID -f /dev/null' >> /start_spark.sh