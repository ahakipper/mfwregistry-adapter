package notice

import (
    "github.com/ahakipper/spotter/pkg/log"
    "github.com/ahakipper/spotter/pkg/notice/appcenternotice"
    "net"
)

var Noticer *appcenternotice.Noticer

func InitNoticeClient(env string) {
    Noticer = appcenternotice.NewNoticer()
    Noticer = Noticer.WithAppCode("spotter-mtech").
        WithKey("KZ60vWUzdM65ibQCGn03sPF9c1trlIfA").WithEnv(env)
}

func Notice(title, content string) {
    messageLevel := appcenternotice.MESSAGE_LEVEL_EMERGENCY
    messageType := appcenternotice.MESSAGE_TYPE_TEXT
    go func() {
        err := Noticer.SendNotice(title, content, messageLevel, messageType)
        if err != nil {
            log.Logger.Errorf("noticer send notice error:%s", err)
        }
    }()
}

// get the current node IP
func GetLocalIP() (ip string, err error) {
    addrs, err := net.InterfaceAddrs()
    if err != nil {
        return
    }
    for _, addr := range addrs {
        ipAddr, ok := addr.(*net.IPNet)
        if !ok {
            continue
        }
        if ipAddr.IP.IsLoopback() {
            continue
        }
        if !ipAddr.IP.IsGlobalUnicast() {
            continue
        }
        return ipAddr.IP.String(), nil
    }
    return
}
