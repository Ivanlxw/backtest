FROM python:3.8-buster
RUN  apt-get update \
  && apt-get install -y wget tar build-essential
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
RUN tar -xf ta-lib-0.4.0-src.tar.gz
RUN cd ./ta-lib/ && ./configure  --prefix=/usr
RUN cd ta-lib/ && make
RUN cd ta-lib/ && make install
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
RUN mkdir code/
COPY . code/
WORKDIR /code
RUN pip install -e .
RUN python loop.py