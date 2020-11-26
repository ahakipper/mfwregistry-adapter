FROM hub.mfwdev.com/paas/centos:7.5.1804
ADD ./config /usr/bin/config
ADD ./mfwregistry-k8sadapter /usr/bin/
WORKDIR /usr/bin/
ENTRYPOINT ["/usr/bin/mfwregistry-k8sadapter"]