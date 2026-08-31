FROM hub.mfwdev.com/paas/golang:latest
ENV TZ='Asia/Shanghai'
ADD ./config /usr/bin/config
ADD ./spotter /usr/bin/
WORKDIR /usr/bin/
ENTRYPOINT ["/usr/bin/spotter"]