FROM hub.mfwdev.com/paas/golang:1.17.4
ENV TZ='Asia/Shanghai'
ADD ./config /usr/bin/config
ADD ./mfwregistry-adapter /usr/bin/
WORKDIR /usr/bin/
ENTRYPOINT ["/usr/bin/mfwregistry-adapter"]