package main

import (
	"fmt"
	"net"
	"time"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:9000")
	if err != nil {
		fmt.Println("-------->dail err: ", err)
		return
	}

	c := make(chan string, 100000)
	go func(conn net.Conn) {
		for {
			key := fmt.Sprintf("%s", GetRandomString(10))
			str := fmt.Sprintf("set:%s:%s", key, GetRandomString(10))
			_, err := conn.Write([]byte(str))
			if err != nil {
				fmt.Println("------->write err: ", err)
				break
			} else {
				c <- key
				//fmt.Println("--------> write string: ", str)
			}
			time.Sleep(5 * time.Second)
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
				fmt.Println("--------> read string: ", string(body))
			}
		}
	}(conn)

	for {
		select {
		case key := <-c:
			key = fmt.Sprintf("get:%s", key)
			_, err := conn.Write([]byte(key))
			if err != nil {
				fmt.Println("------->write err: ", err)
				break
			} else {
				//fmt.Println("-------->write len: ", n, " write string: ", key)
			}
		}
	}
}
