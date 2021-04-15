package config

import (
	"log"
	"testing"

	"github.com/go-kratos/kratos/v2/config"
	"gopkg.in/yaml.v2"
)

func TestConfig_Load(t *testing.T) {
	c := config.New(
		config.WithSource(
			NewSource("http://xxxx.com",
				"xx",
				Group("test"),
				DataID("test")),
		),
		config.WithDecoder(func(kv *config.KeyValue, v map[string]interface{}) error {
			return yaml.Unmarshal(kv.Value, v)
		}),
	)
	if err := c.Load(); err != nil {
		panic(err)
	}

	// struct
	var v struct {
		Serivce struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"service"`
	}
	if err := c.Scan(&v); err != nil {
		panic(err)
	}

	log.Printf("config: %+v", v)
	// key/value
	name, err := c.Value("service.name").String()
	if err != nil {
		panic(err)
	}
	log.Printf("service: %s", name)
}
