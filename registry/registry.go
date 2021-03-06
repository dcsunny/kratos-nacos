package registry

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	"github.com/dcsunny/kratos-nacos/util"

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

func New(endpoint string, namespaceID string, opts ...Option) (*Registry, error) {
	_options := options{
		weight: 100,
	}
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
	port = util.GetPort(raw.Scheme, port)
	sc := []constant.ServerConfig{
		{
			IpAddr: addr,
			Port:   port,
		},
	}
	cc := constant.ClientConfig{
		NamespaceId:         r.opts.namespaceID, //namespace id
		TimeoutMs:           r.opts.timeoutMs,
		NotLoadCacheAtStart: true,
		LogLevel:            r.opts.logLevel,
	}

	if cc.LogLevel == "" {
		cc.LogLevel = "warn"
	}

	if cc.TimeoutMs == 0 {
		cc.TimeoutMs = 5000
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
		if raw.Scheme == "http" {
			continue
		}
		addr = raw.Hostname()
		port, _ = strconv.ParseUint(raw.Port(), 10, 16)
		scheme = raw.Scheme
		port = util.GetPort(scheme, port)
	}

	params := vo.RegisterInstanceParam{
		Ip:          addr,
		Port:        port,
		Weight:      r.opts.weight,
		Enable:      true,
		Healthy:     true,
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
	params.Metadata["version"] = service.Version
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
		if raw.Scheme == "http" {
			continue
		}
		addr = raw.Hostname()
		port, _ = strconv.ParseUint(raw.Port(), 10, 16)
		scheme = raw.Scheme
		port = util.GetPort(scheme, port)
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

func (r *Registry) GetService(ctx context.Context, serviceName string) ([]*registry.ServiceInstance, error) {
	return r.Fetch(ctx, serviceName)
}

func (r *Registry) Watch(ctx context.Context, serviceName string) (registry.Watcher, error) {
	watcher := newRegistryWatcher(ctx, r.opts.group, serviceName, r.client.Unsubscribe)
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
	ins, err := r.GetService(ctx, serviceName)
	if err != nil {
		return nil, err
	}
	services := make([]model.SubscribeService, 0)
	for _, v := range ins {
		if len(v.Endpoints) == 0 {
			return nil, errors.New("endpoints is empty")
		}
		link, err := url.Parse(v.Endpoints[0])
		if err != nil {
			return nil, errors.New(fmt.Sprintf("endpoints is err,%s", v.Endpoints[0]))
		}
		if v.Metadata == nil {
			v.Metadata = make(map[string]string)
		}
		v.Metadata["scheme"] = link.Scheme
		v.Metadata["id"] = v.ID
		v.Metadata["name"] = v.Name
		v.Metadata["version"] = v.Version
		port, _ := strconv.Atoi(link.Port())
		services = append(services, model.SubscribeService{
			Enable:      true,
			InstanceId:  v.ID,
			Ip:          link.Hostname(),
			Metadata:    v.Metadata,
			Port:        uint64(port),
			ServiceName: v.Name,
			Valid:       true,
			Weight:      r.opts.weight,
		})
	}
	watcher.services <- services
	return watcher, nil
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

func newRegistryWatcher(ctx context.Context, group string, serviceName string, unsubscribe unsubscribeFunc) *RegistryWatcher {
	w := &RegistryWatcher{
		group:       group,
		serviceName: serviceName,
		unsubscribe: unsubscribe,
		services:    make(chan []model.SubscribeService, 100),
	}
	ctx, cancel := context.WithCancel(ctx)
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
	err := w.unsubscribe(&vo.SubscribeParam{
		ServiceName: w.serviceName,
		GroupName:   w.group,
	})
	w.cancel()
	return err
}

func (w *RegistryWatcher) Stop() error {
	return w.Close()
}
