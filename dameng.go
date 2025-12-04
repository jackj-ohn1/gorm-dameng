package dameng

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
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

type Options func(config *ConnectionConfig)

func WithProp(key string, value string) Options {
	return func(config *ConnectionConfig) {
		config.props[key] = append(config.props[key], value)
	}
}

func NewConnectionConfig(user, password, host string, port int, schema string, propsFunc ...Options) *ConnectionConfig {
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
	mergeMap := make(map[string][]string)
	for key, options := range c.props {
		mergeMap[key] = append(mergeMap[key], options...)
	}

	return buildUrl(c.user, c.password, c.host, c.port, mergeMap)
}

func BuildUrl(user, password, host string, port int, urlOptions ...map[string]string) string {
	mergeMap := make(map[string][]string)
	for _, option := range urlOptions {
		for key, value := range option {
			mergeMap[key] = append(mergeMap[key], value)
		}
	}

	return buildUrl(user, password, host, port, mergeMap)
}

func buildUrl(user, password, host string, port int, mergeMap map[string][]string) string {
	query := make([]string, 0, len(mergeMap))
	for key, values := range mergeMap {
		query = append(query, fmt.Sprintf("%s=%s", key, strings.Join(values, ",")))
	}

	dmUrl := &url.URL{
		Scheme:   DriverName,
		User:     url.UserPassword(user, password),
		Host:     net.JoinHostPort(host, strconv.Itoa(port)),
		RawQuery: strings.Join(query, "&"),
	}
	return dmUrl.String()
}
