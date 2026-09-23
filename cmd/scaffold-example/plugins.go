package main

import (
	"context"
	"log"

	"github.com/xingjianluo/go-service-scaffold-go/internal/config"
	"github.com/xingjianluo/go-service-scaffold-go/internal/plugin"
)

// base 提供插件名/依赖/关闭记录的公共实现，并记录生命周期事件用于演示。
type base struct {
	name   string
	deps   []string
	logger *log.Logger
	closed bool
}

func (b *base) Name() string        { return b.name }
func (b *base) DependsOn() []string { return b.deps }

func (b *base) Close(_ context.Context) error {
	b.closed = true
	b.logger.Printf("  plugin %s closed", b.name)
	return nil
}

// loggerPlugin 是最底层插件，无依赖。
type loggerPlugin struct{ *base }

func newLoggerPlugin(logger *log.Logger) *loggerPlugin {
	return &loggerPlugin{base: &base{name: "logger", logger: logger}}
}

func (p *loggerPlugin) Init(_ context.Context) error {
	p.logger.Printf("  plugin %s init", p.name)
	return nil
}

// storePlugin 依赖 logger；读取已脱敏配置（密码仅在构造客户端时经 ExposeString 取用）。
type storePlugin struct {
	*base
	password string
}

func newStorePlugin(cfg *config.Config, logger *log.Logger) *storePlugin {
	pw, _ := cfg.Get("db.password")
	return &storePlugin{
		base:     &base{name: "store", deps: []string{"logger"}, logger: logger},
		password: pw.ExposeString(), // 唯一允许读取密钥原文的位置，禁止打印
	}
}

func (p *storePlugin) Init(_ context.Context) error {
	// 真实实现里这里会用 p.password 建立数据库连接；示例只记录"已持有密钥"，不打印其值。
	p.logger.Printf("  plugin %s init (secret loaded, length=%d)", p.name, len(p.password))
	return nil
}

// serverPlugin 依赖 store（间接也依赖 logger），最后启动。
type serverPlugin struct {
	*base
	host string
	port int
}

func newServerPlugin(cfg *config.Config, logger *log.Logger) *serverPlugin {
	host, _ := cfg.Get("server.host")
	port, _ := cfg.Get("server.port")
	return &serverPlugin{
		base: &base{name: "server", deps: []string{"store"}, logger: logger},
		host: host.ExposeString(),
		port: port.Int(),
	}
}

func (p *serverPlugin) Init(_ context.Context) error {
	p.logger.Printf("  plugin %s init, would listen on %s:%d", p.name, p.host, p.port)
	return nil
}

// 编译期断言：示例插件满足 plugin.Plugin 契约。
var (
	_ plugin.Plugin = (*loggerPlugin)(nil)
	_ plugin.Plugin = (*storePlugin)(nil)
	_ plugin.Plugin = (*serverPlugin)(nil)
)
