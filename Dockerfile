FROM debian:jessie

ENV LANG C.UTF-8

# ==================== Java

##########
# Packages

RUN apt-get --assume-yes update && apt-get --assume-yes install \
  # Makes /etc/ssl/certs/java/cacerts available for the java8 install uses less stringent cacerts
  default-jre-headless \
  # to fetch jce_policy
  curl \
  # to unzip jce policy
  unzip && \
  rm -rf /var/lib/apt/lists/*

########

# Install Oracle Java 8. See https://github.com/William-Yeh/docker-java8
# add webupd8 repository
RUN \
    echo "===> add webupd8 repository..."  && \
    echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee /etc/apt/sources.list.d/webupd8team-java.list  && \
    echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee -a /etc/apt/sources.list.d/webupd8team-java.list  && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EEA14886  && \
    apt-get update  && \
    \
    \
    echo "===> install Java"  && \
    echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections  && \
    echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections  && \
    DEBIAN_FRONTEND=noninteractive  apt-get --assume-yes install oracle-java8-installer oracle-java8-set-default  && \
    \
    \
    echo "===> clean up..."  && \
    rm -rf /var/cache/oracle-jdk8-installer  && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle


# ==================== Lein

ENV LEIN_ROOT true

ENV LEIN_VERSION 2.7.1

RUN wget -q -O /usr/bin/lein \
    https://raw.githubusercontent.com/technomancy/leiningen/${LEIN_VERSION}/bin/lein \
    && chmod +x /usr/bin/lein \
    && lein upgrade

COPY . /app

WORKDIR /app

RUN lein deps
RUN lein uberjar

ENTRYPOINT ["./scripts/entrypoint.sh"]
