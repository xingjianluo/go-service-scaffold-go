// scaffold-example 演示脚手架的端到端用法：
//
//	分层配置（默认值 -> 可选 JSON 文件 -> 环境变量）
//	-> 插件按依赖顺序启动、失败回滚
//	-> 运行时状态原子热重载（SIGHUP）与优雅退出（SIGINT/SIGTERM）
//
// 运行示例：
//
//	APP_NAME=demo APP_DB_PASSWORD=s3cret ./scaffold-example
//	APP_CONFIG_FILE=/etc/app.json APP_NAME=demo APP_DB_PASSWORD=s3cret ./scaffold-example
//	kill -HUP <pid>   # 触发热重载
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/xingjianluo/go-service-scaffold-go/internal/config"
	"github.com/xingjianluo/go-service-scaffold-go/internal/plugin"
	"github.com/xingjianluo/go-service-scaffold-go/internal/runtime"
)

func main() {
	logger := log.New(os.Stdout, "[example] ", log.LstdFlags|log.Lmsgprefix)

	// 首次启动与每次热重载都通过同一个 builder 构建完整新状态。
	build := func(ctx context.Context) (*runtime.State, error) {
		return buildState(ctx, logger)
	}

	holder := runtime.NewHolder()

	// 初始装入：失败则进程退出，绝不以半初始化状态提供服务。
	ctx := context.Background()
	if err := holder.Reload(ctx, build); err != nil {
		var le *config.LoadError
		if errors.As(err, &le) {
			logger.Printf("启动失败：配置非法，错误码 %v\n%s", le.Codes(), le.Error())
		} else {
			logger.Printf("启动失败：%v", err)
		}
		os.Exit(1)
	}
	logger.Printf("启动完成（当前配置已脱敏，密钥不显示）")
	if err := holder.Current(func(s *runtime.State) {
		logger.Printf("%s", s.Data["config"])
	}); err != nil {
		logger.Printf("读取状态失败：%v", err)
	}

	// 信号：HUP 热重载，INT/TERM 优雅退出。
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	for sig := range sigCh {
		switch sig {
		case syscall.SIGHUP:
			logger.Printf("收到 SIGHUP，开始热重载……")
			if err := holder.Reload(ctx, build); err != nil {
				// 关键契约：重载失败时旧状态继续可用。
				logger.Printf("热重载失败（旧状态继续服务）：%v", err)
				continue
			}
			logger.Printf("热重载成功，已原子切换到新状态")
			_ = holder.Current(func(s *runtime.State) {
				logger.Printf("新状态：%s", s.Data["config"])
			})
		case syscall.SIGINT, syscall.SIGTERM:
			logger.Printf("收到 %v，开始优雅关闭……", sig)
			signal.Stop(sigCh)
			close(sigCh)
			if err := holder.Close(); err != nil {
				logger.Printf("关闭异常：%v", err)
			}
			logger.Printf("已关闭，退出")
			return
		}
	}
}

// buildState 是 Builder：读配置 -> 建插件 -> 按依赖启动，全部成功才返回状态。
// 任何一步失败都会以错误返回，runtime 保证旧状态不被破坏。
func buildState(ctx context.Context, logger *log.Logger) (*runtime.State, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}
	logger.Printf("配置加载并校验通过：%s", cfg.String()) // String() 自动对密钥脱敏

	reg := plugin.NewRegistry()
	// 重复注册会被明确拒绝，这里用 MustRegister 让编程错误在构建期 panic。
	reg.MustRegister(newLoggerPlugin(logger))
	reg.MustRegister(newStorePlugin(cfg, logger))
	reg.MustRegister(newServerPlugin(cfg, logger))

	mgr := plugin.NewManager(reg)
	if err := mgr.Start(ctx); err != nil {
		// Start 内部已完成失败回滚；这里不需要额外清理。
		return nil, fmt.Errorf("启动插件: %w", err)
	}
	logger.Printf("插件启动顺序：%s", strings.Join(mgr.Started(), " -> "))

	return &runtime.State{
		Data: map[string]any{
			"config":  cfg.String(),
			"manager": mgr,
		},
		// 旧状态被回收时，逆序关闭其中的全部插件。
		Close: func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := mgr.CloseAll(shutdownCtx); err != nil {
				logger.Printf("插件关闭出错：%v", err)
			} else {
				logger.Printf("插件已全部逆序关闭")
			}
		},
	}, nil
}

func loadConfig() (*config.Config, error) {
	spec := config.NewSpec().
		Add(config.FieldSpec{
			Key: "app.name", Kind: config.KindString, Required: true,
			Help: "应用名称，必填",
		}).
		Add(config.FieldSpec{
			Key: "server.host", Kind: config.KindString, Default: "127.0.0.1",
			Help: "监听地址",
		}).
		Add(config.FieldSpec{
			Key: "server.port", Kind: config.KindInt, Default: 8080,
			Validate: func(v config.Value) error {
				p := v.Int()
				if p < 1 || p > 65535 {
					// 注意：不要在错误信息里拼接字段值以外的敏感内容；
					// 此值非密钥，端口号可以出现在原因中。
					return fmt.Errorf("server.port must be in [1,65535], got %d", p)
				}
				return nil
			},
			Help: "监听端口，1-65535",
		}).
		Add(config.FieldSpec{
			Key: "db.password", Kind: config.KindString, Required: true, Secret: true,
			Help: "数据库口令，必填，任何输出中均脱敏",
		})

	loader := config.NewLoader(spec)
	// 可选文件层（优先级高于默认值）。
	if path := os.Getenv("APP_CONFIG_FILE"); path != "" {
		loader.AddLayer(&config.File{Path: path, Optional: true})
	}
	// 环境变量层优先级最高。
	loader.AddLayer(config.NewEnv("APP_", spec, nil))

	return loader.Load()
}
