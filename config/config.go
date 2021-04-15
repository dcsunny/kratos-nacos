package config

import (
	"context"
	"net"
	"net/url"
	"strconv"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
)

type Option func(*options)

type options struct {
	endpoint string

	namespaceID string

	group  string
	dataID string

	timeoutMs uint64
	logLevel  string

	logDir   string
	cacheDir string

	//网关需要用的
	cluster string
	weight  float64
}

func Group(group string) Option {
	return func(o *options) {
		o.group = group
	}
}

func DataID(dataID string) Option {
	return func(o *options) {
		o.dataID = dataID
	}
}

func LogDir(logDir string) Option {
	return func(o *options) {
		o.logDir = logDir
	}
}

func CacheDir(cacheDir string) Option {
	return func(o *options) {
		o.cacheDir = cacheDir
	}
}

func LogLevel(logLevel string) Option {
	return func(o *options) {
		o.logLevel = logLevel
	}
}

func TimeoutMs(timeoutMs uint64) Option {
	return func(o *options) {
		o.timeoutMs = timeoutMs
	}
}

type Config struct {
	opts   options
	client config_client.IConfigClient
}

func NewSource(endpoint string, namespaceID string, opts ...Option) config.Source {
	_options := options{}
	for _, o := range opts {
		o(&_options)
	}
	c := &Config{
		opts: _options,
	}
	c.opts.endpoint = endpoint
	c.opts.namespaceID = namespaceID
	return c
}

func (c *Config) init() error {
	u, err := url.Parse(c.opts.endpoint)
	if err != nil {
		return err
	}

	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return err
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return err
	}

	sc := []constant.ServerConfig{
		{
			IpAddr: host,
			Port:   uint64(p),
		},
	}
	cc := constant.ClientConfig{
		NamespaceId:         c.opts.namespaceID, //namespace id
		TimeoutMs:           c.opts.timeoutMs,
		NotLoadCacheAtStart: true,
		LogDir:              c.opts.logDir,
		CacheDir:            c.opts.cacheDir,
		LogLevel:            c.opts.logLevel,
	}

	if cc.LogLevel == "" {
		cc.LogLevel = LogLevelWarn
	}

	if cc.TimeoutMs == 0 {
		cc.TimeoutMs = 5000
	}

	client, err := clients.CreateConfigClient(map[string]interface{}{
		"serverConfigs": sc,
		"clientConfig":  cc,
	})
	if err != nil {
		return err
	}
	c.client = client
	return nil
}

func (c *Config) Load() ([]*config.KeyValue, error) {

	err := c.init()
	if err != nil {
		return nil, err
	}

	content, err := c.client.GetConfig(vo.ConfigParam{
		DataId: c.opts.dataID,
		Group:  c.opts.group,
	})

	if err != nil {
		return nil, err
	}

	return []*config.KeyValue{
		&config.KeyValue{
			Key:   c.opts.dataID,
			Value: []byte(content),
		},
	}, nil
}

func (c *Config) Watch() (config.Watcher, error) {
	watcher := newNacosWatcher(context.Background(), c.opts.dataID, c.opts.group, c.client.CancelListenConfig)
	err := c.client.ListenConfig(vo.ConfigParam{
		DataId: c.opts.dataID,
		Group:  c.opts.group,
		OnChange: func(namespace, group, dataId, data string) {
			if dataId == watcher.dataID && group == watcher.group {
				watcher.content <- data
			}
			return
		},
	})
	if err != nil {
		return nil, err
	}
	return watcher, nil
}

type ConfigWatcher struct {
	context.Context
	dataID             string
	group              string
	content            chan string
	cancelListenConfig cancelListenConfigFunc
	cancel             context.CancelFunc
}

type cancelListenConfigFunc func(params vo.ConfigParam) (err error)

func newNacosWatcher(ctx context.Context, dataID string, group string, cancelListenConfig cancelListenConfigFunc) *ConfigWatcher {
	w := &ConfigWatcher{
		dataID:             dataID,
		group:              group,
		cancelListenConfig: cancelListenConfig,
		content:            make(chan string, 100),
	}
	ctx, cancel := context.WithCancel(ctx)
	w.Context = ctx
	w.cancel = cancel
	return w
}

func (w *ConfigWatcher) Next() ([]*config.KeyValue, error) {
	select {
	case <-w.Context.Done():
		return nil, nil
	case content := <-w.content:
		return []*config.KeyValue{
			&config.KeyValue{
				Key:   w.dataID,
				Value: []byte(content),
			},
		}, nil
	}
}

func (w *ConfigWatcher) Close() error {
	err := w.cancelListenConfig(vo.ConfigParam{
		DataId: w.dataID,
		Group:  w.group,
	})
	w.cancel()
	return err
}

func (w *ConfigWatcher) Stop() error {
	return w.Close()
}
