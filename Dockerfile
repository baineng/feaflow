FROM python:3.7

ARG EXTRA

WORKDIR /usr/src/feaflow

COPY . .

RUN bin/install.sh $EXTRA