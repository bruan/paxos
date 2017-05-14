package main

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
)

type paxosCfg struct {
	XMLName   xml.Name      `xml:"root"`
	NodeAddr  listenAddrCfg `xml:"listen"`
	NodeAddrs nodeAddrCfgs  `xml:"node_list"`
}

type nodeAddrCfgs struct {
	Addr []nodeAddrCfg `xml:"node"`
}

type nodeAddrCfg struct {
	Addr string `xml:"addr,attr"`
	ID   int    `xml:"id,attr"`
}

type listenAddrCfg struct {
	Addr   string `xml:"addr,attr"`
	Client string `xml:"http,attr"`
	ID     int    `xml:"id,attr"`
}

func main() {
	var paxosCfg paxosCfg
	file, err := os.Open("./etc/paxos_conf.xml")
	if err != nil {
		log.Printf("can't open paxos_conf.xml: %v\n", err)
		return
	}
	data, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("read paxos_conf.xml error: %v\n", err)
		return
	}

	err = xml.Unmarshal(data, &paxosCfg)
	if err != nil {
		log.Printf("unmarshal paxos_conf.xml error: %v\n", err)
		return
	}
	file.Close()

	nodeAddrs := make(map[int]string)
	for i := 0; i < len(paxosCfg.NodeAddrs.Addr); i++ {
		nodeAddr := paxosCfg.NodeAddrs.Addr[i]
		nodeAddrs[nodeAddr.ID] = nodeAddr.Addr
	}
	kvService := NewKVService(paxosCfg.NodeAddr.ID, paxosCfg.NodeAddr.Addr, nodeAddrs, 1)

	http.HandleFunc("/GET_LOCAL", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			http.Error(w, "The method is not allowed.", http.StatusMethodNotAllowed)
			return
		}

		req.ParseForm()

		key := req.FormValue("key")
		value, version := kvService.GetLocal(key)
		w.Write([]byte(fmt.Sprintf("[GET_LOCAL] key: %s value: %s version: %d", key, value, version)))
	})

	http.HandleFunc("/GET_GLOBAL", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			http.Error(w, "The method is not allowed.", http.StatusMethodNotAllowed)
			return
		}

		req.ParseForm()

		key := req.FormValue("key")
		value, version := kvService.GetGlobal(key)
		w.Write([]byte(fmt.Sprintf("[GET_GLOBAL] key: %s value: %s version: %d", key, value, version)))
	})

	http.HandleFunc("/SET", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			http.Error(w, "The method is not allowed.", http.StatusMethodNotAllowed)
			return
		}

		req.ParseForm()

		key := req.FormValue("key")
		value := req.FormValue("value")
		versionBuf := req.FormValue("version")
		version, err := strconv.Atoi(versionBuf)
		if err != nil {
			http.Error(w, "The arg is not allowed.", http.StatusNotFound)
			return
		}
		_version := int32(version)
		value, _version = kvService.Set(key, value, _version)
		w.Write([]byte(fmt.Sprintf("[SET] key: %s value: %s version: %d", key, value, _version)))
	})

	http.HandleFunc("/DEL", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			http.Error(w, "The method is not allowed.", http.StatusMethodNotAllowed)
			return
		}

		req.ParseForm()

		key := req.FormValue("key")
		version, err := strconv.Atoi(req.FormValue("version"))
		if err != nil {
			http.Error(w, "The arg is not allowed.", http.StatusNotFound)
			return
		}

		value, _version := kvService.Del(key, int32(version))
		w.Write([]byte(fmt.Sprintf("[DEL] key: %s value: %s version: %d", key, value, _version)))
	})

	err = http.ListenAndServe(paxosCfg.NodeAddr.Client, nil)
	if err != nil {
		fmt.Printf("ListenAndServe error: %s %s", err, paxosCfg.NodeAddr.Client)
	}
}
