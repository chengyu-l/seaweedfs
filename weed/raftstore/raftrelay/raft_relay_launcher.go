package raftrelay

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/pkg/types"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/rafthttp"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/raftnode/membership"
	"github.com/seaweedfs/seaweedfs/weed/raftstore/wal"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/soheilhy/cmux"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

type RaftRelayLauncher struct {
	Peers []*peerListener

	RaftRelay *RaftRelay

	cfg   LaunchConfig
	stopc chan struct{}
	errc  chan error

	closeOnce sync.Once
}

func StartRaftRelay(cfg *LaunchConfig) (*RaftRelay, <-chan struct{}, <-chan error, func(), error) {
	l, err := startRaftRelay(cfg)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	select {
	case <-l.RaftRelay.ReadyNotify(): // wait for e.Server to join the cluster
	case <-l.RaftRelay.StopNotify(): // publish aborted from 'ErrStopped'
	}
	return l.RaftRelay, l.RaftRelay.StopNotify(), l.Err(), l.Close, nil
}

func startRaftRelay(cfg *LaunchConfig) (l *RaftRelayLauncher, err error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	l = &RaftRelayLauncher{
		cfg:   *cfg,
		stopc: make(chan struct{}),
	}
	defer func() {
		if l == nil || err == nil {
			return
		}
		l.Close()
		l = nil
	}()
	if l.Peers, err = configurePeerListeners(cfg); err != nil {
		return l, err
	}
	var urlsmap types.URLsMap
	if !isMemberInitialized(cfg) {
		urlsmap, err = cfg.PeerURLsMap("nebulasfs")
		if err != nil {
			return l, fmt.Errorf("error setting up initial cluster: %v", err)
		}
	}
	relaycfg := RaftRelayConfig{
		Name:            cfg.Name,
		PeerURLs:        cfg.LPUrls,
		ClientURLs:      cfg.LCUrls,
		DataDir:         cfg.Dir,
		DedicatedWALDir: cfg.WalDir,

		SnapshotCount:          cfg.SnapshotCount,
		SnapshotCatchUpEntries: cfg.SnapshotCatchUpEntries,
		MaxSnapFiles:           cfg.MaxSnapFiles,
		MaxWALFiles:            cfg.MaxWalFiles,

		BackendBatchLimit:    cfg.BackendBatchLimit,
		BackendFreelistType:  bolt.FreelistArrayType,
		BackendBatchInterval: cfg.BackendBatchInterval,

		InitialPeerURLsMap:  urlsmap,
		NewCluster:          cfg.IsNewCluster(),
		InitialClusterToken: cfg.InitialClusterToken,

		TickMs:        cfg.TickMs,
		ElectionTicks: cfg.ElectionTicks(),

		InitialElectionTickAdvance: cfg.InitialElectionTickAdvance,

		QuotaBackendBytes: cfg.QuotaBackendBytes,
		MaxTxnOps:         cfg.MaxTxnOps,
		MaxRequestBytes:   cfg.MaxRequestBytes,

		PreVote:           cfg.PreVote,
		Logger:            cfg.logger,
		LoggerConfig:      cfg.loggerConfig,
		LoggerCore:        cfg.loggerCore,
		LoggerWriteSyncer: cfg.loggerWriteSyncer,
	}
	if err = relaycfg.Initialize(); err != nil {
		return nil, err
	}

	if l.RaftRelay, err = NewRaftRelay(relaycfg); err != nil {
		return l, err
	}

	// buffer channel so goroutines on closed connections won't wait forever
	l.errc = make(chan error, len(l.Peers))

	l.RaftRelay.Start()
	if err = l.servePeers(); err != nil {
		return l, err
	}
	return l, nil
}

// checkBindURLs returns an error if any URL uses a domain name.
func checkBindURLs(urls []url.URL) error {
	for _, url := range urls {
		if url.Scheme == "unix" || url.Scheme == "unixs" {
			continue
		}
		host, _, err := net.SplitHostPort(url.Host)
		if err != nil {
			return err
		}
		if host == "localhost" {
			// special case for local address
			// TODO: support /etc/hosts ?
			continue
		}
		if net.ParseIP(host) == nil {
			return fmt.Errorf("expected IP in URL for binding (%s)", url.String())
		}
	}
	return nil
}

type peerListener struct {
	net.Listener
	serve func() error
	close func(context.Context) error
}

func configurePeerListeners(cfg *LaunchConfig) (peers []*peerListener, err error) {
	peers = make([]*peerListener, len(cfg.LPUrls))
	defer func() {
		if err == nil {
			return
		}
		for i := range peers {
			if peers[i] != nil && peers[i].close != nil {
				cfg.logger.Warn(
					"closing peer listener",
					zap.String("address", cfg.LPUrls[i].String()),
					zap.Error(err),
				)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				peers[i].close(ctx)
				cancel()
			}
		}
	}()

	for i, u := range cfg.LPUrls {
		peers[i] = &peerListener{close: func(context.Context) error { return nil }}
		peers[i].Listener, err = rafthttp.NewListener(u, nil)
		if err != nil {
			return nil, err
		}
		// once serve, overwrite with 'http.Server.Shutdown'
		peers[i].close = func(context.Context) error {
			return peers[i].Listener.Close()
		}
	}
	return peers, nil
}

// configure peer handlers after rafthttp.Transport started
func (l *RaftRelayLauncher) servePeers() (err error) {
	ph := NewPeerHandler(l.GetLogger(), l.RaftRelay)

	for _, p := range l.Peers {
		u := p.Listener.Addr().String()
		m := cmux.New(p.Listener)
		srv := &http.Server{
			Handler:     ph,
			ReadTimeout: 5 * time.Minute,
			ErrorLog:    log.New(ioutil.Discard, "", 0), // do not log user error
		}
		go srv.Serve(m.Match(cmux.Any()))
		p.serve = func() error { return m.Serve() }
		p.close = func(ctx context.Context) error {
			// gracefully shutdown http.Server
			// close open listeners, idle connections
			// until context cancel or time-out
			l.cfg.logger.Info(
				"stopping serving peer traffic",
				zap.String("address", u),
			)
			stopHttpServers(ctx, srv)
			l.cfg.logger.Info(
				"stopped serving peer traffic",
				zap.String("address", u),
			)
			return nil
		}
	}

	// start peer servers in a goroutine
	for _, pl := range l.Peers {
		go func(listener *peerListener) {
			u := listener.Addr().String()
			l.cfg.logger.Info(
				"serving peer traffic",
				zap.String("address", u),
			)
			l.errHandler(listener.serve())
		}(pl)
	}
	return nil
}

// Close gracefully shuts down all servers/listeners.
func (l *RaftRelayLauncher) Close() {
	fields := []zap.Field{
		zap.String("name", l.cfg.Name),
		zap.String("data-dir", l.cfg.Dir),
		zap.Strings("listen-peer-urls", l.cfg.getLPURLs()),
	}
	lg := l.GetLogger()
	lg.Info("closing raft relay", fields...)
	defer func() {
		lg.Info("closed raft relay", fields...)
		lg.Sync()
	}()

	l.closeOnce.Do(func() { close(l.stopc) })

	// close rafthttp transports
	if l.RaftRelay != nil {
		l.RaftRelay.Stop()
	}

	// close all idle connections in peer handler (wait up to 1-second)
	for i := range l.Peers {
		if l.Peers[i] != nil && l.Peers[i].close != nil {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			l.Peers[i].close(ctx)
			cancel()
		}
	}
}

func parseBackendFreelistType(freelistType string) bolt.FreelistType {
	if freelistType == freelistMapType {
		return bolt.FreelistMapType
	}

	return bolt.FreelistArrayType
}

func stopHttpServers(ctx context.Context, ss *http.Server) {
	ss.Shutdown(ctx)
}

func (l *RaftRelayLauncher) errHandler(err error) {
	select {
	case <-l.stopc:
		return
	default:
	}
	select {
	case <-l.stopc:
	case l.errc <- err:
	}
}

// GetLogger returns the logger.
func (l *RaftRelayLauncher) GetLogger() *zap.Logger {
	l.cfg.loggerMu.RLock()
	log := l.cfg.logger
	l.cfg.loggerMu.RUnlock()
	return log
}

func (l *RaftRelayLauncher) Err() <-chan error { return l.errc }

func isMemberInitialized(cfg *LaunchConfig) bool {
	waldir := cfg.WalDir
	if waldir == "" {
		waldir = filepath.Join(cfg.Dir, "member", "wal")
	}
	return wal.Exist(waldir)
}

const (
	peerMembersPath         = "/members"
	peerMemberPromotePrefix = "/members/promote/"
)

// NewPeerHandler generates an http.Handler to handle peer requests.
func NewPeerHandler(lg *zap.Logger, s Service) http.Handler {
	return newPeerHandler(lg, s)
}

func newPeerHandler(lg *zap.Logger, s Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", http.NotFound)
	mux.Handle(rafthttp.RaftPrefix, s.RaftHandler())
	mux.Handle(rafthttp.RaftPrefix+"/", s.RaftHandler())
	mux.Handle(peerMembersPath, newPeerMembersHandler(lg, s.Cluster()))
	mux.Handle(peerMemberPromotePrefix, newPeerMemberPromoteHandler(lg, s))
	return mux
}

type peerMembersHandler struct {
	lg      *zap.Logger
	cluster *membership.RaftCluster
}

func newPeerMembersHandler(lg *zap.Logger, cluster *membership.RaftCluster) http.Handler {
	return &peerMembersHandler{
		lg:      lg,
		cluster: cluster,
	}
}

func (h *peerMembersHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, "GET") {
		return
	}
	w.Header().Set("X-Nebulasfs-Cluster-ID", h.cluster.ID().String())

	if r.URL.Path != peerMembersPath {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	ms := h.cluster.Members()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(ms); err != nil {
		if h.lg != nil {
			h.lg.Warn("failed to encode membership members", zap.Error(err))
		} else {
			glog.Warningf("failed to encode members response (%v)", err)
		}
	}
}

type peerMemberPromoteHandler struct {
	lg      *zap.Logger
	cluster *membership.RaftCluster
	server  Service
}

func newPeerMemberPromoteHandler(lg *zap.Logger, s Service) http.Handler {
	return &peerMemberPromoteHandler{
		lg:      lg,
		cluster: s.Cluster(),
		server:  s,
	}
}

func (h *peerMemberPromoteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r, "POST") {
		return
	}
	w.Header().Set("X-Nebulasfs-Cluster-ID", h.cluster.ID().String())

	if !strings.HasPrefix(r.URL.Path, peerMemberPromotePrefix) {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	idStr := strings.TrimPrefix(r.URL.Path, peerMemberPromotePrefix)
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf("member %s not found in cluster", idStr), http.StatusNotFound)
		return
	}

	resp, err := h.server.PromoteMember(r.Context(), id)
	if err != nil {
		switch err {
		case membership.ErrIDNotFound:
			http.Error(w, err.Error(), http.StatusNotFound)
		case membership.ErrMemberNotLearner:
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
		case ErrLearnerNotReady:
			http.Error(w, err.Error(), http.StatusPreconditionFailed)
		default:
			WriteError(h.lg, w, r, err)
		}
		if h.lg != nil {
			h.lg.Warn(
				"failed to promote a member",
				zap.String("member-id", types.ID(id).String()),
				zap.Error(err),
			)
		} else {
			glog.Errorf("error promoting member %s (%v)", types.ID(id).String(), err)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		if h.lg != nil {
			h.lg.Warn("failed to encode members response", zap.Error(err))
		} else {
			glog.Warningf("failed to encode members response (%v)", err)
		}
	}
}
