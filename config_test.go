package rocketmq_test

import (
	"encoding/json"
	"log"
	"os"
)

type data struct {
	Endpoint     string   `json:"endpoint"`
	AccessKey    string   `json:"accessKey"`
	SecretKey    string   `json:"secretKey"`
	NameSpace    string   `json:"nameSpace"`
	ConsumeGroup string   `json:"consumeGroup"`
	Topic        []string `json:"topic"`
}

var config = &data{}

func init() {
	bytes, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatal(err)
		return
	}
	if err = json.Unmarshal(bytes, config); err != nil {
		log.Fatal(err)
		return
	}
}
