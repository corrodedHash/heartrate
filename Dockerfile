FROM python:3.13-alpine

WORKDIR /workspace

RUN apk update && apk add --no-cache inotify-tools

ADD requirements.txt .
RUN pip install -r requirements.txt

ADD main.py .

ADD --chmod=755 init.sh /init.sh

VOLUME /watch
ENV WATCHPATH=/watch

ENTRYPOINT ["/bin/sh", "/init.sh"]