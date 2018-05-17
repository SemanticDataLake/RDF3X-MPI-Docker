FROM ubuntu:16.04

USER root

RUN apt-get update && apt install -y build-essential autotools-dev autoconf automake unzip wget net-tools libtool flex bison gperf gawk m4 libssl-dev libreadline-dev openssl crudini git nano software-properties-common

RUN add-apt-repository ppa:ubuntu-toolchain-r/test &&  apt-get update && apt-get install -y gcc-4.7

RUN mkdir /data

WORKDIR /data
RUN mkdir -p /opt/openmpi/3.1.0
RUN wget https://download.open-mpi.org/release/open-mpi/v3.1/openmpi-3.1.0.tar.gz
RUN tar -xvf openmpi-3.1.0.tar.gz &&  cd openmpi-3.1.0 && mkdir build && cd build && ../configure --enable-mpi-cxx --prefix=/opt/openmpi/3.1.0 && make all install

ENV PATH=/opt/openmpi/3.1.0/bin:/gh-rdf3x-mpi/bin:$PATH

ADD ./gh-rdf3x  /gh-rdf3x-mpi


RUN cd /gh-rdf3x-mpi && make

CMD ["tail", "-f", "/dev/null"]
