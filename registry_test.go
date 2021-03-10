package nacos

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/go-kratos/kratos/v2/registry"
)

func TestRegistry_Register(t *testing.T) {
	serviceName := "config_test"
	_registry, err := NewRegistry("http://xxx",
		"xxx",
		Group("test123"))

	if err != nil {
		log.Println(err)
		return
	}

	err = _registry.Register(context.Background(), &registry.ServiceInstance{
		ID:        serviceName,
		Name:      serviceName,
		Version:   "v1.0",
		Endpoints: []string{"rpc://0.0.0.0:8000"},
	})
	if err != nil {
		log.Println(err)
		return
	}

	instances, err := _registry.Fetch(context.Background(), serviceName)
	if err != nil {
		log.Println(err)
		return
	}
	for _, v := range instances {
		fmt.Println(v)
	}
	err = _registry.Deregister(context.Background(), &registry.ServiceInstance{
		ID:   serviceName,
		Name: serviceName,
	})
	if err != nil {
		log.Println(err)
		return
	}
}
