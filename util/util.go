package util

func GetPort(scheme string, port uint64) uint64 {
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
