ARG BUILD_FROM=ghcr.io/hassio-addons/base:17.2.5
FROM ${BUILD_FROM}

ENV LANG=C.UTF-8

RUN apk add --no-cache python3 py3-pip

COPY requirements.txt /
RUN pip3 install --no-cache-dir --break-system-packages -r /requirements.txt

COPY run.sh /
COPY bridge.py /
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
