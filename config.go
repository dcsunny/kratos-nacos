package nacos

import (
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

	logDir   string
	cacheDir string
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

func (c Config) init() error {
	raw, err := url.Parse(c.opts.endpoint)
	if err != nil {
		return nil
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
		NamespaceId:         c.opts.namespaceID, //namespace id
		TimeoutMs:           5000,
		NotLoadCacheAtStart: true,
		LogDir:              c.opts.logDir,
		CacheDir:            c.opts.cacheDir,
		RotateTime:          "1h",
		MaxAge:              3,
		LogLevel:            "error",
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
	watcher := newNacosWatcher(c.opts.dataID, c.opts.group)
	err := c.client.ListenConfig(vo.ConfigParam{
		DataId: c.opts.dataID,
		Group:  c.opts.group,
		OnChange: func(namespace, group, dataId, data string) {
			if dataId == watcher.dataID && group == watcher.dataID {
				watcher.content = data
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
	dataID  string
	group   string
	content string
	client  config_client.IConfigClient
}

func newNacosWatcher(dataID string, group string) *ConfigWatcher {
	return &ConfigWatcher{
		dataID: dataID,
		group:  group,
	}
}

func (n *ConfigWatcher) Next() ([]*config.KeyValue, error) {
	return []*config.KeyValue{
		&config.KeyValue{
			Key:   n.dataID,
			Value: []byte(n.content),
		},
	}, nil
}

func (n *ConfigWatcher) Close() error {
	err := n.client.CancelListenConfig(vo.ConfigParam{
		DataId: n.dataID,
		Group:  n.group,
	})
	return err
}
