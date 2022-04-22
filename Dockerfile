FROM hub.mfwdev.com/paas/golang:latest
ENV TZ='Asia/Shanghai'
ADD ./config /usr/bin/config
ADD ./mfwregistry-adapter /usr/bin/
WORKDIR /usr/bin/
ENTRYPOINT ["/usr/bin/mfwregistry-adapter"]