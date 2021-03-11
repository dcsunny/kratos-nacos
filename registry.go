package nacos

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/common/constant"

	"github.com/nacos-group/nacos-sdk-go/model"

	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/vo"

	"github.com/go-kratos/kratos/v2/registry"
)

type Registry struct {
	opts   options
	client naming_client.INamingClient
}

func NewRegistry(endpoint string, namespaceID string, opts ...Option) (*Registry, error) {
	_options := options{}
	for _, o := range opts {
		o(&_options)
	}
	r := &Registry{
		opts: _options,
	}
	r.opts.endpoint = endpoint
	r.opts.namespaceID = namespaceID
	err := r.init()
	if err != nil {
		return nil, err
	}
	return r, err
}

func (r *Registry) init() error {
	raw, err := url.Parse(r.opts.endpoint)
	if err != nil {
		return err
	}
	addr := raw.Hostname()
	port, _ := strconv.ParseUint(raw.Port(), 10, 16)
	port = getPort(raw.Scheme, port)
	sc := []constant.ServerConfig{
		{
			IpAddr: addr,
			Port:   port,
		},
	}
	cc := constant.ClientConfig{
		NamespaceId:         r.opts.namespaceID, //namespace id
		TimeoutMs:           5000,
		NotLoadCacheAtStart: true,
		RotateTime:          "1h",
		MaxAge:              3,
		LogLevel:            "warn",
	}

	client, err := clients.CreateNamingClient(map[string]interface{}{
		"serverConfigs": sc,
		"clientConfig":  cc,
	})
	if err != nil {
		return err
	}
	r.client = client
	return nil
}

func (r *Registry) Register(ctx context.Context, service *registry.ServiceInstance) error {
	var addr string
	var port uint64
	var scheme string
	for _, endpoint := range service.Endpoints {
		raw, err := url.Parse(endpoint)
		if err != nil {
			return err
		}
		addr = raw.Hostname()
		port, _ = strconv.ParseUint(raw.Port(), 10, 16)
		scheme = raw.Scheme
		port = getPort(scheme, port)
	}
	params := vo.RegisterInstanceParam{
		Ip:          addr,
		Port:        port,
		Weight:      1,
		Enable:      true,
		Healthy:     false,
		Metadata:    service.Metadata,
		ServiceName: service.Name,
		GroupName:   r.opts.group,
	}
	if params.Metadata == nil {
		params.Metadata = make(map[string]string)
	}
	params.Metadata["scheme"] = scheme
	params.Metadata["id"] = service.ID
	params.Metadata["name"] = service.Name
	_, err := r.client.RegisterInstance(params)
	return err
}

func (r *Registry) Deregister(ctx context.Context, service *registry.ServiceInstance) error {
	var addr string
	var port uint64
	var scheme string
	for _, endpoint := range service.Endpoints {
		raw, err := url.Parse(endpoint)
		if err != nil {
			return err
		}
		addr = raw.Hostname()
		port, _ = strconv.ParseUint(raw.Port(), 10, 16)
		scheme = raw.Scheme
		port = getPort(scheme, port)
	}
	_, err := r.client.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          addr,
		Port:        port,
		ServiceName: service.Name,
		GroupName:   r.opts.group,
	})

	return err
}

func (r *Registry) Fetch(ctx context.Context, serviceName string) ([]*registry.ServiceInstance, error) {
	s, err := r.client.GetService(vo.GetServiceParam{
		ServiceName: serviceName,
		GroupName:   r.opts.group,
	})
	if err != nil {
		return nil, err
	}
	instances := make([]*registry.ServiceInstance, len(s.Hosts))
	for k, v := range s.Hosts {
		instances[k] = &registry.ServiceInstance{
			ID:   v.Metadata["id"],
			Name: v.ServiceName,
			Endpoints: []string{
				fmt.Sprintf("%s://%s:%d", v.Metadata["scheme"], v.Ip, v.Port),
			},
		}
	}
	return instances, nil
}

func (r *Registry) Watch(ctx context.Context, serviceName string) (registry.Watcher, error) {
	watcher := newRegistryWatcher(r.opts.group, serviceName, r.client.Unsubscribe)
	err := r.client.Subscribe(&vo.SubscribeParam{
		ServiceName: serviceName,
		GroupName:   r.opts.group,
		SubscribeCallback: func(services []model.SubscribeService, err error) {
			watcher.services <- services
		},
	})
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return watcher, nil
}

func getPort(scheme string, port uint64) uint64 {
	if port != 0 {
		return port
	}
	if scheme == "http" {
		return 80
	} else if scheme == "https" {
		return 443
	}
	return port
}

type RegistryWatcher struct {
	services    chan []model.SubscribeService
	unsubscribe unsubscribeFunc
	serviceName string
	group       string

	context.Context
	cancel context.CancelFunc
}

type unsubscribeFunc func(param *vo.SubscribeParam) error

func newRegistryWatcher(group string, serviceName string, unsubscribe unsubscribeFunc) *RegistryWatcher {
	w := &RegistryWatcher{
		group:       group,
		serviceName: serviceName,
		unsubscribe: unsubscribe,
		services:    make(chan []model.SubscribeService, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Context = ctx
	w.cancel = cancel
	return w
}

func (w *RegistryWatcher) Next() ([]*registry.ServiceInstance, error) {
	select {
	case <-w.Context.Done():
		return nil, nil
	case services := <-w.services:
		instances := make([]*registry.ServiceInstance, 0)
		for _, v := range services {
			ins := &registry.ServiceInstance{
				ID:       v.Metadata["id"],
				Name:     v.ServiceName,
				Metadata: v.Metadata,
				Endpoints: []string{
					fmt.Sprintf("%s://%s:%d", v.Metadata["scheme"], v.Ip, v.Port),
				},
			}
			instances = append(instances, ins)
		}
		return instances, nil
	}
}

func (w *RegistryWatcher) Close() error {
	w.cancel()
	err := w.unsubscribe(&vo.SubscribeParam{
		ServiceName: w.serviceName,
		GroupName:   w.group,
	})
	return err
}
