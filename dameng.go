package dameng

import (
	"net"
	"net/url"
	"strconv"
)

// DriverName 数据库驱动、连接字符串协议名称
const DriverName = "dm"

// BuildUrl 构建达梦数据库连接字符串
//
//   - 如： dm://user:password@host:port?schema=SYSDBA[&propName2=propValue2]…
//   - 若要指定用户登录后的当前模式，请在 options 中设置 schema，缺省为用户的默认模式，如 SYSDBA
//   - 参考链接： https://eco.dameng.com/document/dm/zh-cn/pm/go-rogramming-guide.html#11.3%20%E8%BF%9E%E6%8E%A5%E4%B8%B2%E5%B1%9E%E6%80%A7%E8%AF%B4%E6%98%8E
type ConnectionConfig struct {
	user string
	password string
	host string
	port int
	props map[string][]string
}

type withOption func(config *ConnectionConfig)

func WithProp(key string, value string) withOption {
	return func(config *ConnectionConfig) {
		config.props[key] = append(config.props[key], value)
	}
}

func NewConnectionConfig(user, password, host string, port int, schema string, propsFunc ...withOption) *ConnectionConfig {
	config := &ConnectionConfig{
		user: user,
		password: password,
		host: host,
		port: port,
		props: make(map[string][]string),
	}
	for _, p := range propsFunc {
		p(config)
	}

	if schema != "" && config.props["schema"] == nil {
		config.props["schema"] = []string{schema}
	}
	if config.props["columnNameCase"] == nil {
		config.props["columnNameCase"] = []string{"lower"}
	}
	if config.props["escapeProcess"] == nil {
		config.props["escapeProcess"] = []string{"true"}
	}
	return config
}

func (c *ConnectionConfig) BuildUrl() string {
	return BuildUrl(c.user, c.password, c.host, c.port, c.props)
}

func BuildUrl(user, password, host string, port int, urlOptions map[string][]string) string {
	propQuery := url.Values{}
	for key, option := range urlOptions {
		for _, value := range option {
			propQuery.Add(key, value)
		}
	}

	dmUrl := &url.URL{
		Scheme:   DriverName,
		User:     url.UserPassword(user, password),
		Host:     net.JoinHostPort(host, strconv.Itoa(port)),
		RawQuery: propQuery.Encode(),
	}
	return dmUrl.String()
}
