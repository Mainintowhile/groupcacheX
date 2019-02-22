package main

import (
	"errors"
	"fmt"
	"groupcacheX"
	"net"
	"net/http"
	"strings"
	"log"
	"encoding/json"
	"math/rand"
	"time"
)

func main() {
	type cacheData struct {
		Value string
	}


	cache := groupcache.NewGroup(&groupcache.Opts{
		Name:           "test",
		CacheBytes:     2 << 20,
		ExpireDuration: 0,
		Logger:         log.Println,
		Getter: groupcache.GetterFunc(func(key string, dest groupcache.Sink) error {
			//log.Println("----------->cacheData cache nil find db:", key)
			if strings.Contains(key, "fromnet") {
				return errors.New("fake of cant find such key:" + key)
			}
			data := new(cacheData)
			data.Value = fmt.Sprintf("cachevalue%s", key)

			b, err := json.Marshal(data)
			if err != nil {
				log.Println(err)
				return err
			}

			return dest.SetString(string(b))
		}),
	})

	cache1 := groupcache.NewGroup(&groupcache.Opts{
		Name:            "test1",
		CacheBytes:      2 << 20,
		ExpireDuration:  0,
		Logger:          log.Println,
		DisableHttpPeer: true,
		Getter: groupcache.GetterFunc(func(key string, dest groupcache.Sink) error {
			//log.Println("----------->cacheData cache nil find db:", key)
			if strings.Contains(key, "fromnet") {
				return errors.New("fake of cant find such key:" + key)
			}
			data := new(cacheData)
			data.Value = fmt.Sprintf("cachevalue%s", key)

			b, err := json.Marshal(data)
			if err != nil {
				log.Println(err)
				return err
			}

			return dest.SetString(string(b))
		}),
	})

	go func() {
		ln, err := net.Listen("tcp", "127.0.0.1:9000")
		if err != nil {
			log.Println("-------->init tcp server err: ,", err)
		}

		defer ln.Close()

		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Println("------------>accept err: ,", err)
				break
			} else {
				go func() {
					for {
						readbody := make([]byte, 14)
						n, err := conn.Read(readbody)
						if err != nil {
							log.Println("-------->read err: ", err)
							break
						} else {
							//set:key:value   write
							//get:key   get
							//stats       stats
							//count       count
							//list       list count
							_d := strings.Replace(string(readbody[:n]), "\n", "", -1)
							d := strings.Split(_d, ":")
							if len(d) <= 0 {

							}
							//log.Println(fmt.Sprintf("----receive:%s-", d))
							switch d[0] {
							case "set":
								_v := new(cacheData)
								_v.Value = fmt.Sprintf("cachevalue%s", d[2])
								//log.Println("---b", _v)
								b, err := json.Marshal(_v)
								if err != nil {
									log.Println(err)
									continue
								}
								c:= string(b)

								//log.Println("---b", b)
								err = cache.Set(d[1], &c)
								if err != nil {
									log.Println(err)
								} else {
									//log.Println(fmt.Sprintf("set key----:%s,value: %s", d[1], b))
								}
							case "get":
								var _s []byte
								err := cache.Get(d[1], groupcache.AllocatingByteSliceSink(&_s))
								if err != nil {
									log.Println(err)
									continue
								}
								//v := new(cacheData)
								//err = json.Unmarshal(_s, &v)
								//log.Println(fmt.Sprintf("get key----:%s,value: %s", d[1], v))
								//_, err = conn.Write(_s)
								//if err != nil {
								//	log.Println(err)
								//}
							case "get1":
								var _s []byte
								err := cache1.Get(d[1], groupcache.AllocatingByteSliceSink(&_s))
								if err != nil {
									log.Println(err)
									continue
								}
							case "stats":
								stats := cache.MachinesStats()
								b, err := json.Marshal(stats)
								if err != nil {
									log.Println(err)
									continue
								}
								_, err = conn.Write([]byte(b))
								if err != nil {
									log.Println(err)
								}
							case "list":
								cache.List()
							default:
								continue
							}
						}
					}
				}()
			}
		}
	}()

	addr, p := groupcache.NewHTTPPool([]string{"192.168.2.161:9111", "192.168.2.161:9112", "192.168.2.161:9113"})
	err := http.ListenAndServe(addr, p)
	if err != nil {
		log.Fatal("listen 1 error:", err)
	}
}


func GetRandomString(size int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < size; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}
