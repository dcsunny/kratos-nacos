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

type Config struct {
	Group  string
	DataID string

	LogDir   string //可以不配置
	CacheDir string //可以不配置

	client config_client.IConfigClient
}

func NewSource(endpoint string, namespaceID string, group string, dataID string, logDir string, cacheDir string) config.Source {
	raw, err := url.Parse(endpoint)
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
		NamespaceId:         namespaceID, //namespace id
		TimeoutMs:           5000,
		NotLoadCacheAtStart: true,
		LogDir:              logDir,
		CacheDir:            cacheDir,
		RotateTime:          "1h",
		MaxAge:              3,
		LogLevel:            "error",
	}
	client, err := clients.CreateConfigClient(map[string]interface{}{
		"serverConfigs": sc,
		"clientConfig":  cc,
	})
	if err != nil {
		return nil
	}

	return &Config{
		Group:  group,
		DataID: dataID,
		client: client,
	}
}

func (n *Config) Load() ([]*config.KeyValue, error) {

	content, err := n.client.GetConfig(vo.ConfigParam{
		DataId: n.DataID,
		Group:  n.Group,
	})

	if err != nil {
		return nil, err
	}

	return []*config.KeyValue{
		&config.KeyValue{
			Key:   n.DataID,
			Value: []byte(content),
		},
	}, nil
}

func (n *Config) Watch() (config.Watcher, error) {
	watcher := newNacosWatcher(n.DataID, n.Group)
	err := n.client.ListenConfig(vo.ConfigParam{
		DataId: n.DataID,
		Group:  n.Group,
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
