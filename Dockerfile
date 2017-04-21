
FROM ubuntu:16.04

MAINTAINER Tomasz Swider version: 0.1

RUN apt-get update
RUN apt-get install sudo

RUN echo "deb https://artifacts.elastic.co/packages/5.x/apt stable main" | sudo tee -a /etc/apt/sources.list.d/elastic-5.x.list

RUN apt-get install -y \
    apache2 \
    git     \
    apt-transport-https \
    curl \
    net-tools \
    vim \
    default-jre default-jdk \
    python3-pip

ADD resources/apache/apache-config.conf /etc/apache2/sites-enabled/000-default.conf

RUN apt-get clean && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /elasticsearch/data && mkdir /elasticsearch/logs
RUN cd /elasticsearch && curl -L -O https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.3.0.tar.gz && tar -xvf elasticsearch-5.3.0.tar.gz
ADD resources/elasticsearch/elasticsearch.yml /elasticsearch/elasticsearch-5.3.0/config/elasticsearch.yml

RUN groupadd esgroup && useradd -ms /bin/bash esuser
RUN usermod -a -G esgroup esuser
RUN chown esuser.esgroup /elasticsearch -R

RUN cd /var && git clone https://github.com/immunochomik/kibana3.git
ADD resources/kibana3/config.js /var/kibana3/src/config.js

ENV APACHE_RUN_USER www-data
ENV APACHE_RUN_GROUP www-data
ENV APACHE_LOG_DIR /var/log/apache2

# setup kibanan 5
RUN apt-get update && apt-get install -y \
    wget \
    htop

RUN wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
RUN apt-get install -y apt-transport-https
RUN echo "deb https://artifacts.elastic.co/packages/5.x/apt stable main" | tee -a /etc/apt/sources.list.d/elastic-5.x.list
RUN apt-get update && apt-get install -y --allow-unauthenticated kibana
ADD resources/kibana5/kibana.yml /etc/kibana/kibana.yml



ADD startscript /scripts/startscript
RUN chmod +x /scripts/startscript

# Set up watcherd
COPY watcher /watcher
RUN mkdir /inserts
RUN pip3 install -r /watcher/requirements.txt
RUN chmod +x /watcher/watcher.py

EXPOSE 80
EXPOSE 9200
EXPOSE 5601

CMD /scripts/startscript