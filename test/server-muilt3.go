package main

import (
	"errors"
	"fmt"
	"groupcacheX"
	"log"
	"encoding/json"
	"net/http"
	"strings"
)

func main() {
	type cacheData struct {
		Value string
	}


	groupcache.NewGroup(&groupcache.Opts{
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

			return dest.SetString(b)
		}),
	})

	groupcache.NewGroup(&groupcache.Opts{
		Name:            "test1",
		CacheBytes:      2 << 20,
		ExpireDuration:  0,
		DisableHttpPeer: true,
		Logger:          log.Println,
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

	addr, p := groupcache.NewHTTPPool([]string{"192.168.0.242:9113", "192.168.0.242:9112", "192.168.0.242:9111"})
	err := http.ListenAndServe(addr, p)
	if err != nil {
		log.Fatal("listen 3 error:", err)
	}
}
