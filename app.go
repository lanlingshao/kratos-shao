package kratos

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport"
)

// AppInfo is application context value.
type AppInfo interface {
	ID() string
	Name() string
	Version() string
	Metadata() map[string]string
	Endpoint() []string
}

// App is an application components lifecycle manager.
type App struct {
	opts     options
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	instance *registry.ServiceInstance
}

// New create an application lifecycle manager.
func New(opts ...Option) *App {
	o := options{
		ctx:              context.Background(),
		sigs:             []os.Signal{syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT},
		registrarTimeout: 10 * time.Second,
		stopTimeout:      10 * time.Second,
	}
	if id, err := uuid.NewUUID(); err == nil {
		o.id = id.String()
	}
	for _, opt := range opts {
		opt(&o)
	}
	if o.logger != nil {
		log.SetLogger(o.logger)
	}
	ctx, cancel := context.WithCancel(o.ctx)
	return &App{
		ctx:    ctx,
		cancel: cancel,
		opts:   o,
	}
}

// ID returns app instance id.
func (a *App) ID() string { return a.opts.id }

// Name returns service name.
func (a *App) Name() string { return a.opts.name }

// Version returns app version.
func (a *App) Version() string { return a.opts.version }

// Metadata returns service metadata.
func (a *App) Metadata() map[string]string { return a.opts.metadata }

// Endpoint returns endpoints.
func (a *App) Endpoint() []string {
	if a.instance != nil {
		return a.instance.Endpoints
	}
	return nil
}

// Run executes all OnStart hooks registered with the application's Lifecycle.
func (a *App) Run() error {
	// 1、通过 kratos.Server()声明的server实例,并通过 buildInstance() 转换成 *registry.ServiceInstance struct,用于服务注册
	// 2、在buildInstance中会先监听服务的地址（在server启动之前）
	instance, err := a.buildInstance()
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.instance = instance
	a.mu.Unlock()
	sctx := NewContext(a.ctx, a)
	eg, ctx := errgroup.WithContext(sctx)
	wg := sync.WaitGroup{}

	// 执行 helloword/cmd/main.go中的 newApp()中传入的BeforeStart方法中的函数
	for _, fn := range a.opts.beforeStart {
		if err = fn(sctx); err != nil {
			return err
		}
	}
	// 遍历通过 kratos.Server() 声明的服务实例
	for _, srv := range a.opts.servers {
		server := srv
		// 执行两个goroutine, 用于处理服务启动和退出
		eg.Go(func() error {
			<-ctx.Done() // wait for stop signal // 阻塞wait for stop signal,等待调用 cancel 方法
			// 如果没有在a.opts.stopTimeout的时间内停掉server的实例，则自动调用cancel方法
			stopCtx, cancel := context.WithTimeout(NewContext(a.opts.ctx, a), a.opts.stopTimeout)
			defer cancel()
			// 协程退出后,调用server实例的停止方法，
			// httpServer调用go-kratos/kratos/v2/调用transport/http/server.go
			// grpcServer调用go-kratos/kratos/v2/transport/grpc/server.go
			return server.Stop(stopCtx)
		})
		// 注意:下面需要利用WaitGroup来保证各个server启动后才继续进行下一步
		wg.Add(1)
		eg.Go(func() error {
			// 服务启动成功后再进行下面的服务注册
			wg.Done() // here is to ensure server start has begun running before register, so defer is not needed
			// 任意一个server启动失败的时候,都会返回err.导致errgroup的ctx被取消
			return server.Start(NewContext(a.opts.ctx, a))
		})
	}
	wg.Wait()
	if a.opts.registrar != nil {
		// 服务注册，如果在registrarTimeout时间内还没有注册成功，则直接调用rcancel
		rctx, rcancel := context.WithTimeout(ctx, a.opts.registrarTimeout)
		defer rcancel()
		if err = a.opts.registrar.Register(rctx, instance); err != nil {
			return err
		}
	}
	// 执行 helloword/cmd/main.go中的 newApp()中传入的AfterStart方法中的函数
	for _, fn := range a.opts.afterStart {
		if err = fn(sctx); err != nil {
			return err
		}
	}

	// 监听进程退出信号,退出信号会发送到c中，只有a.opts.sigs里面的信号才会被监听，a.opts.sigs在New(opts ...Option) *App 中设置
	c := make(chan os.Signal, 1)
	signal.Notify(c, a.opts.sigs...)
	eg.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		case <-c:
			// 接收到退出信号，调用Stop方法关闭应用
			return a.Stop()
		}
	})
	if err = eg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	err = nil
	// 执行 helloword/cmd/main.go中的 newApp()中传入的 AfterStop 方法中的函数
	for _, fn := range a.opts.afterStop {
		err = fn(sctx)
	}
	return err
}

// Stop gracefully stops the application.
func (a *App) Stop() (err error) {
	sctx := NewContext(a.ctx, a)
	// 执行 helloword/cmd/main.go中的 newApp()中传入的 BeforeStop 方法中的函数
	for _, fn := range a.opts.beforeStop {
		err = fn(sctx)
	}

	a.mu.Lock()
	instance := a.instance
	a.mu.Unlock()
	if a.opts.registrar != nil && instance != nil {
		ctx, cancel := context.WithTimeout(NewContext(a.ctx, a), a.opts.registrarTimeout)
		defer cancel()
		if err = a.opts.registrar.Deregister(ctx, instance); err != nil {
			return err
		}
	}
	if a.cancel != nil {
		a.cancel()
	}
	return err
}

func (a *App) buildInstance() (*registry.ServiceInstance, error) {
	endpoints := make([]string, 0, len(a.opts.endpoints))
	for _, e := range a.opts.endpoints {
		endpoints = append(endpoints, e.String())
	}
	if len(endpoints) == 0 {
		for _, srv := range a.opts.servers {
			if r, ok := srv.(transport.Endpointer); ok {
				// 监听
				e, err := r.Endpoint()
				if err != nil {
					return nil, err
				}
				endpoints = append(endpoints, e.String())
			}
		}
	}
	return &registry.ServiceInstance{
		ID:        a.opts.id,
		Name:      a.opts.name,
		Version:   a.opts.version,
		Metadata:  a.opts.metadata,
		Endpoints: endpoints,
	}, nil
}

type appKey struct{}

// NewContext returns a new Context that carries value.
func NewContext(ctx context.Context, s AppInfo) context.Context {
	return context.WithValue(ctx, appKey{}, s)
}

// FromContext returns the Transport value stored in ctx, if any.
func FromContext(ctx context.Context) (s AppInfo, ok bool) {
	s, ok = ctx.Value(appKey{}).(AppInfo)
	return
}
