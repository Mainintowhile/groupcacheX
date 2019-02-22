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

	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	go func(conn net.Conn) {
		for {
			//key := fmt.Sprintf("%s", utils.GetRandomString(10))
			str := fmt.Sprintf("get:%s", GetRandomString(10))
			//str := fmt.Sprintf("get:%s", utils.GetRandomString(1000))
			_, err := conn.Write([]byte(str))
			if err != nil {
				fmt.Println("------->write err: ", err)
				break
			} else {
				//fmt.Println("--------> write string: ", str)
			}

			time.Sleep(400 * time.Millisecond)
			//time.Sleep(20 * time.Millisecond)
			//time.Sleep(200 * time.Nanosecond)
			//time.Sleep(time.Duration(r.Intn(10000)) * time.Nanosecond)
		}
	}(conn)

	select {}
}
