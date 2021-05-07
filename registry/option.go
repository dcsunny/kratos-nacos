package registry

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

	weight float64
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
