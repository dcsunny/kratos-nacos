package registry

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"

	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/common/constant"

	"github.com/go-kratos/kratos/v2/registry"
	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/vo"
)

var (
	_ registry.Registrar = (*Registry)(nil)
	_ registry.Discovery = (*Registry)(nil)
)

type options struct {
	prefix    string
	weight    float64
	cluster   string
	group     string
	logDir    string
	cacheDir  string
	timeoutMs uint64
	logLevel  string
}

// Option is nacos option.
type Option func(o *options)

// WithPrefix with prefix path.
func WithPrefix(prefix string) Option {
	return func(o *options) { o.prefix = prefix }
}

// WithWeight with weight option.
func WithWeight(weight float64) Option {
	return func(o *options) { o.weight = weight }
}

// WithCluster with cluster option.
func WithCluster(cluster string) Option {
	return func(o *options) { o.cluster = cluster }
}

// WithGroup with group option.
func WithGroup(group string) Option {
	return func(o *options) { o.group = group }
}

// WithLogLevel
func WithLogLevel(logLevel string) Option {
	return func(o *options) { o.logLevel = logLevel }
}

// WithLogDir
func WithLogDir(logDir string) Option {
	return func(o *options) { o.logDir = logDir }
}

// WithCacheDir
func WithCacheDir(cacheDir string) Option {
	return func(o *options) { o.cacheDir = cacheDir }
}

// WithTimeoutMs
func WithTimeoutMs(timeoutMs uint64) Option {
	return func(o *options) { o.timeoutMs = timeoutMs }
}

// Registry is nacos registry.
type Registry struct {
	opts options
	cli  naming_client.INamingClient
}

// New new a nacos registry.
func New(endpoint, namespaceID string, opts ...Option) (r *Registry) {
	options := options{
		prefix:    "/microservices",
		cluster:   "DEFAULT",
		group:     "DEFAULT_GROUP",
		weight:    100,
		timeoutMs: 5000,
		logLevel:  "warn",
	}
	for _, option := range opts {
		option(&options)
	}
	cli := initClient(endpoint, namespaceID, options)
	if cli == nil {
		return nil
	}
	return &Registry{
		opts: options,
		cli:  cli,
	}
}

func initClient(endpoint, namespaceID string, o options) naming_client.INamingClient {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil
	}

	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		return nil
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return nil
	}

	sc := []constant.ServerConfig{
		{
			IpAddr: host,
			Port:   uint64(p),
		},
	}
	cc := constant.ClientConfig{
		NamespaceId:         namespaceID, //namespace id
		TimeoutMs:           o.timeoutMs,
		NotLoadCacheAtStart: true,
		LogDir:              o.logDir,
		CacheDir:            o.cacheDir,
		LogLevel:            o.logLevel,
	}

	client, err := clients.CreateNamingClient(map[string]interface{}{
		"serverConfigs": sc,
		"clientConfig":  cc,
	})
	if err != nil {
		return nil
	}
	return client
}

// Register the registration.
func (r *Registry) Register(ctx context.Context, si *registry.ServiceInstance) error {
	for _, endpoint := range si.Endpoints {
		u, err := url.Parse(endpoint)
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
		if si.Metadata == nil {
			si.Metadata = make(map[string]string)
		}
		si.Metadata["kind"] = u.Scheme
		si.Metadata["version"] = si.Version
		_, e := r.cli.RegisterInstance(vo.RegisterInstanceParam{
			Ip:          host,
			Port:        uint64(p),
			ServiceName: si.Name,
			Weight:      r.opts.weight,
			Enable:      true,
			Healthy:     true,
			Ephemeral:   true,
			Metadata:    si.Metadata,
			ClusterName: r.opts.cluster,
			GroupName:   r.opts.group,
		})
		if e != nil {
			return fmt.Errorf("RegisterInstance err %v,%v", e, endpoint)
		}
	}
	return nil
}

// Deregister the registration.
func (r *Registry) Deregister(ctx context.Context, service *registry.ServiceInstance) error {
	for _, endpoint := range service.Endpoints {
		u, err := url.Parse(endpoint)
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
		if _, err = r.cli.DeregisterInstance(vo.DeregisterInstanceParam{
			Ip:          host,
			Port:        uint64(p),
			ServiceName: service.Name,
			GroupName:   r.opts.group,
			Cluster:     r.opts.cluster,
		}); err != nil {
			return err
		}
	}
	return nil
}

// Watch creates a watcher according to the service name.
func (r *Registry) Watch(ctx context.Context, serviceName string) (registry.Watcher, error) {
	return newWatcher(ctx, r.cli, serviceName, r.opts.group, []string{r.opts.cluster})
}

// GetService return the service instances in memory according to the service name.
func (r *Registry) GetService(ctx context.Context, serviceName string) ([]*registry.ServiceInstance, error) {
	res, err := r.cli.GetService(vo.GetServiceParam{
		ServiceName: serviceName,
	})
	if err != nil {
		return nil, err
	}
	var items []*registry.ServiceInstance
	for _, in := range res.Hosts {
		items = append(items, &registry.ServiceInstance{
			ID:        in.InstanceId,
			Name:      res.Name,
			Version:   in.Metadata["version"],
			Metadata:  in.Metadata,
			Endpoints: []string{fmt.Sprintf("%s://%s:%d", in.Metadata["kind"], in.Ip, in.Port)},
		})
	}
	return items, nil
}
