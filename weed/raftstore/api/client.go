package api

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type Config struct {
	Endpoints            []string
	DialTimeout          time.Duration
	DialKeepAliveTime    time.Duration
	DialKeepAliveTimeout time.Duration
}

type Client struct {
	KV
	Cluster
	Maintenance
	cfg  Config
	conn *grpc.ClientConn

	ctx    context.Context
	cancel context.CancelFunc
}

func (c *Client) Endpoints() []string {
	ret := make([]string, 0, len(c.cfg.Endpoints))
	copy(ret, c.cfg.Endpoints)
	return ret
}

func (c *Client) Close() error {
	c.cancel()
	if c.conn != nil {
		return c.conn.Close()
	}
	return c.ctx.Err()
}

func (c *Client) Dial(endpoint string) (*grpc.ClientConn, error) {
	cfg := c.cfg
	dailCtx := c.ctx
	if cfg.DialTimeout > 0 {
		var cancel context.CancelFunc
		dailCtx, cancel = context.WithTimeout(c.ctx, cfg.DialTimeout)
		defer cancel()
	}
	var options []grpc.DialOption
	options = append(options,
		grpc.WithInsecure(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                cfg.DialKeepAliveTime,
			Timeout:             cfg.DialKeepAliveTimeout,
			PermitWithoutStream: true,
		}))
	grpcConection, err := grpc.DialContext(dailCtx, endpoint, options...)
	if err != nil {
		return nil, fmt.Errorf("fail to dial: %v", err)
	}
	return grpcConection, nil
}

func NewClient(cfg *Config) (*Client, error) {
	if cfg == nil {
		cfg = &Config{}
	}
	if cfg.Endpoints == nil {
		return nil, fmt.Errorf("endpoints must not be nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		cfg:    *cfg,
		ctx:    ctx,
		cancel: cancel,
	}
	grpcConection, err := client.Dial(cfg.Endpoints[0])
	if err != nil {
		return nil, err
	}
	client.conn = grpcConection
	client.KV = NewRPCKV(client.conn, nil)
	client.Cluster = NewCluster(client.conn, nil)
	client.Maintenance = NewMaintenance(client)
	return client, nil
}
