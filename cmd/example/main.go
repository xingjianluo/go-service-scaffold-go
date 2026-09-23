// Example service built on the reusable scaffold.
//
// Run:
//
//	go run ./cmd/example -config configs/example.json
//
// Then trigger a hot reload by rewriting the file (or sending SIGHUP if wired):
//
//	echo '{"service.name":"demo","db.password":"new-secret","server.port":9090}' \
//	  | tee configs/example.json
//	# call POST /-/reload in a real server; this binary reloads once after 2s.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/xingjianluo/go-service-scaffold-go/app"
	"github.com/xingjianluo/go-service-scaffold-go/config"
	"github.com/xingjianluo/go-service-scaffold-go/plugin"
	"github.com/xingjianluo/go-service-scaffold-go/runtime"
)

func schema() config.Schema {
	return config.Schema{
		{Key: "service.name", Type: config.String, Required: true},
		{Key: "db.password", Type: config.String, Required: true, Secret: true},
		{Key: "server.port", Type: config.Int, Default: 8080, Validator: func(v interface{}) error {
			n := v.(int)
			if n < 1024 || n > 65535 {
				return fmt.Errorf("port must be within 1024..65535")
			}
			return nil
		}},
		{Key: "feature.enabled", Type: config.Bool, Default: false},
	}
}

// ---- two tiny plugins with a dependency: cache depends on database ----

type database struct{ dsn string }

func (p *database) Name() string   { return "database" }
func (p *database) Deps() []string { return nil }
func (p *database) Init(context.Context) error {
	log.Printf("plugin database initialized (password never logged)")
	return nil
}
func (p *database) Close() error { log.Print("plugin database closed"); return nil }

type cache struct{}

func (p *cache) Name() string   { return "cache" }
func (p *cache) Deps() []string { return []string{"database"} }
func (p *cache) Init(context.Context) error {
	log.Print("plugin cache initialized")
	return nil
}
func (p *cache) Close() error { log.Print("plugin cache closed"); return nil }

// buildSnapshot validates the config-driven plugin set using plugin.Manager:
// dependencies resolve order and any Init failure rolls back automatically.
func buildSnapshot(ctx context.Context, cfg *config.Config) (*runtime.Snapshot, error) {
	reg := plugin.NewRegistry()
	if err := reg.Register(&database{dsn: "user:***@db"}); err != nil {
		return nil, err
	}
	if err := reg.Register(&cache{}); err != nil {
		return nil, err
	}
	mgr := plugin.NewManager(reg)
	if err := mgr.Start(ctx); err != nil {
		return nil, err // already-started plugins are rolled back inside
	}
	ordered := make([]plugin.Plugin, 0, len(mgr.Active()))
	for _, name := range mgr.Active() {
		p, _ := reg.Get(name)
		ordered = append(ordered, p)
	}
	return &runtime.Snapshot{Config: cfg, Plugins: ordered}, nil
}

func main() {
	cfgPath := flag.String("config", "configs/example.json", "path to JSON config file")
	flag.Parse()

	application := app.New(config.NewLoader(schema(), *cfgPath, "APP"), buildSnapshot)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := application.Boot(ctx); err != nil {
		log.Fatalf("boot failed: %v", err)
	}
	snap := application.Snapshot()
	log.Printf("booted; effective configuration:\n%s", snap.Config.Describe(schema()))
	log.Printf("active plugins in init order: %v", pluginNames(snap))

	// In a real service you would call application.Reload(ctx) from an HTTP
	// handler or SIGHUP. The scaffold guarantees readers never see a partial
	// state and that failed reloads keep serving the old snapshot.
	_ = application.Reload

	<-ctx.Done()
	log.Print("shutdown signal received")
	if err := application.Shutdown(); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}

func pluginNames(s *runtime.Snapshot) []string {
	out := make([]string, 0, len(s.Plugins))
	for _, p := range s.Plugins {
		out = append(out, p.Name())
	}
	return out
}
