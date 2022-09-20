# Based on docker-influxdb-grafana by Phil Hawthorne <me@philhawthorne.com>
FROM debian:bullseye-slim
LABEL maintainer="Piotr Piatkowski <pp@idea7.pl>"

ENV DEBIAN_FRONTEND noninteractive
ENV LANG C.UTF-8

# Default versions
ENV INFLUXDB_VERSION=1.8.10
ENV GRAFANA_VERSION=9.1.5

# Grafana database type
ENV GF_DATABASE_TYPE=sqlite3

# Fix bad proxy issue
COPY system/99fixbadproxy /etc/apt/apt.conf.d/99fixbadproxy

WORKDIR /root

# Base dependencies
RUN rm /var/lib/apt/lists/* -vf \
    && apt-get -y update \
    && apt-get -y dist-upgrade \
    && apt-get -y install \
        apt-utils \
        atop \
        ca-certificates \
        curl \
        git \
        htop \
        libfontconfig \
        nano \
        net-tools \
        python3-pip \
        supervisor \
        wget \
        gnupg \
    && curl -sL https://deb.nodesource.com/setup_14.x | bash - \
    && apt-get install -y nodejs

# Install grafana and influx
RUN ARCH= && dpkgArch="$(dpkg --print-architecture)" && \
    case "${dpkgArch##*-}" in \
      amd64) ARCH='amd64';; \
      arm64) ARCH='arm64';; \
      armhf) ARCH='armhf';; \
      armel) ARCH='armel';; \
      *)     echo "Unsupported architecture: ${dpkgArch}"; exit 1;; \
    esac \
    && wget --no-verbose https://dl.influxdata.com/influxdb/releases/influxdb_${INFLUXDB_VERSION}_${ARCH}.deb \
    && dpkg -i influxdb_${INFLUXDB_VERSION}_${ARCH}.deb \
    && rm influxdb_${INFLUXDB_VERSION}_${ARCH}.deb \
    && wget --no-verbose https://dl.grafana.com/oss/release/grafana_${GRAFANA_VERSION}_${ARCH}.deb \
    && dpkg -i grafana_${GRAFANA_VERSION}_${ARCH}.deb \
    && rm grafana_${GRAFANA_VERSION}_${ARCH}.deb

# Install modules for collector
RUN pip3 install influxdb

# Cleanup
RUN mkdir -p /var/log/supervisor \
    && rm -rf .profile \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Configure Supervisord and base env
COPY supervisord/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY bash/profile .profile

# Configure InfluxDB
COPY influxdb/influxdb.conf /etc/influxdb/influxdb.conf

# Configure Grafana
COPY grafana/grafana.ini /etc/grafana/grafana.ini

COPY collector/collector.py /usr/sbin/collector.py

EXPOSE 3003

COPY run.sh /run.sh
RUN ["chmod", "+x", "/run.sh"]
CMD ["/run.sh"]
