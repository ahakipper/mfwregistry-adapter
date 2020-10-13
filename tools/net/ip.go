package net

import "net"

func GetIPAddress() string {
    interfaces, err := net.Interfaces()
    if err != nil {
        return ""
    }

    for _, i := range interfaces {
        addrs, err := i.Addrs()
        if err != nil {
            continue
        }

        for _, addr := range addrs {
            switch v := addr.(type) {
            case *net.IPNet:
                if v.IP.To4() != nil && v.IP.String() != "127.0.0.1" && v.IP.String() != "" {
                    return v.IP.String()
                }
            }
        }
    }
    return ""
}
