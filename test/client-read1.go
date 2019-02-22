package main

import (
	"fmt"
	"math/rand"
	"net"
	"time"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		fmt.Println("-------->dail err: ", err)
		return
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	go func(conn net.Conn) {
		for {
			str := fmt.Sprintf("get:%s", GetRandomString(10))
			_, err := conn.Write([]byte(str))
			if err != nil {
				fmt.Println("------->write err: ", err)
				break
			} else {
				//fmt.Println("-------->write len: ", n, " write string: ", str)
			}

			//time.Sleep(2 * time.Second)
			time.Sleep(time.Duration(r.Intn(10000)) * time.Nanosecond)
		}
	}(conn)

	go func(conn net.Conn) {
		for {
			body := make([]byte, 500)
			_, err := conn.Read(body)
			if err != nil {
				fmt.Println("------->read err: ", err)
				break
			} else {
				//fmt.Println("--------> read string: ", string(body))
			}
		}
	}(conn)

	select {}
}
