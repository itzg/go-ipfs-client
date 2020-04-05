package client

import (
	"context"
	"fmt"
	config "github.com/ipfs/go-ipfs-config"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/go-ipfs/core"
	"github.com/ipfs/go-ipfs/core/coreapi"
	"github.com/ipfs/go-ipfs/core/node/libp2p"
	"github.com/ipfs/go-ipfs/plugin/loader"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
	icore "github.com/ipfs/interface-go-ipfs-core"
	icorepath "github.com/ipfs/interface-go-ipfs-core/path"
	peer "github.com/libp2p/go-libp2p-peer"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	ma "github.com/multiformats/go-multiaddr"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
)

var (
	DefaultIpfsBootstrapPeers = []string{
		"/dnsaddr/bootstrap.libp2p.io/ipfs/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
		"/dnsaddr/bootstrap.libp2p.io/ipfs/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
		"/dnsaddr/bootstrap.libp2p.io/ipfs/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
		"/dnsaddr/bootstrap.libp2p.io/ipfs/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
	}
)

type Option func(*IpfsClient)

// WithPluginDir specifies a directory containing IPFS plugins.
// If not specified, only the builtin plugins are loaded.
func WithPluginDir(pluginDir string) Option {
	return func(client *IpfsClient) {
		client.pluginDir = pluginDir
	}
}

// WithBootstrapPeers allows a custom set of IPFS peers to be declared.
// If not specified, DefaultIpfsBootstrapPeers is used.
func WithBootstrapPeers(peers []string) Option {
	return func(client *IpfsClient) {
		client.bootstrapPeers = peers
	}
}

type IpfsClient struct {
	pluginDir      string
	bootstrapPeers []string

	ready     bool
	readyCond *sync.Cond

	ipfs     icore.CoreAPI
	repoPath string
}

func NewIpfsClient(options ...Option) *IpfsClient {
	var readyLocker sync.Mutex
	ipfsClient := &IpfsClient{
		bootstrapPeers: DefaultIpfsBootstrapPeers,
		readyCond:      sync.NewCond(&readyLocker),
	}
	for _, option := range options {
		option(ipfsClient)
	}
	return ipfsClient
}

// Start should be called in a Go routine to start the IPFS node and connect to peers
func (i *IpfsClient) Start(ctx context.Context) {
	err := setupIpfsPlugins(i.pluginDir)
	if err != nil {
		log.Fatal(err)
	}

	repo, err := createTempIpfsRepo(ctx)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to create ipfs repo: %w", err))
	}
	i.repoPath = repo
	go i.cleanupRepo(ctx)

	ipfs, err := createIpfsNode(ctx, repo)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to create ipfs node: %w", err))
	}
	i.ipfs = ipfs

	err = i.connectToIpfsPeers(ctx)
	if err != nil {
		log.Fatal(fmt.Errorf("failed to connect to ipfs peers: %w", err))
	}

	i.setReady()
}

func (i *IpfsClient) setReady() {
	i.readyCond.L.Lock()
	defer i.readyCond.L.Unlock()
	i.ready = true
	i.readyCond.Broadcast()
}

func (i *IpfsClient) waitForReady() {
	i.readyCond.L.Lock()
	for !i.ready {
		i.readyCond.Wait()
	}
	i.readyCond.L.Unlock()
}

func (i *IpfsClient) OpenFile(ctx context.Context, cidStr string) (io.ReadCloser, error) {
	i.waitForReady()

	cid := icorepath.New(cidStr)
	rootNode, err := i.ipfs.Unixfs().Get(ctx, cid)
	if err != nil {
		return nil, err
	}

	file := files.ToFile(rootNode)
	return file, nil
}

func (i *IpfsClient) cleanupRepo(ctx context.Context) {
	<-ctx.Done()

	os.RemoveAll(i.repoPath)
}

func setupIpfsPlugins(externalPluginsPath string) error {
	// Load any external plugins if available on externalPluginsPath
	plugins, err := loader.NewPluginLoader(filepath.Join(externalPluginsPath, "plugins"))
	if err != nil {
		return fmt.Errorf("error loading plugins: %w", err)
	}

	// Load preloaded and external plugins
	if err := plugins.Initialize(); err != nil {
		return fmt.Errorf("error initializing plugins: %w", err)
	}

	if err := plugins.Inject(); err != nil {
		return fmt.Errorf("error initializing plugins: %w", err)
	}

	return nil
}

func (i *IpfsClient) connectToIpfsPeers(ctx context.Context) error {
	var wg sync.WaitGroup
	peerInfos := make(map[peer.ID]*peerstore.PeerInfo, len(i.bootstrapPeers))
	for _, addrStr := range i.bootstrapPeers {
		addr, err := ma.NewMultiaddr(addrStr)
		if err != nil {
			return err
		}
		pii, err := peerstore.InfoFromP2pAddr(addr)
		if err != nil {
			return err
		}
		pi, ok := peerInfos[pii.ID]
		if !ok {
			pi = &peerstore.PeerInfo{ID: pii.ID}
			peerInfos[pi.ID] = pi
		}
		pi.Addrs = append(pi.Addrs, pii.Addrs...)
	}

	wg.Add(len(peerInfos))
	for _, peerInfo := range peerInfos {
		go func(peerInfo *peerstore.PeerInfo) {
			defer wg.Done()
			err := i.ipfs.Swarm().Connect(ctx, *peerInfo)
			if err != nil {
				log.Printf("failed to connect to %s: %s", peerInfo.ID, err)
			}
		}(peerInfo)
	}
	wg.Wait()
	return nil

}

func createTempIpfsRepo(ctx context.Context) (string, error) {
	repoPath, err := ioutil.TempDir("", "cfsync")
	if err != nil {
		return "", fmt.Errorf("failed to get temp dir: %w", err)
	}

	cfg, err := config.Init(ioutil.Discard, 2048)
	if err != nil {
		return "", fmt.Errorf("failed to init ipfs config: %w", err)
	}

	err = fsrepo.Init(repoPath, cfg)
	if err != nil {
		return "", fmt.Errorf("failed to init fsrepo: %w", err)
	}

	return repoPath, nil
}

func createIpfsNode(ctx context.Context, repoPath string) (icore.CoreAPI, error) {
	// Open the repo
	repo, err := fsrepo.Open(repoPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open ipfs repo: %w", err)
	}

	// Construct the node

	nodeOptions := &core.BuildCfg{
		Online:  true,
		Routing: libp2p.DHTClientOption,
		Repo: repo,
	}

	node, err := core.NewNode(ctx, nodeOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create ipfs node: %w", err)
	}

	// Attach the Core API to the constructed node
	return coreapi.NewCoreAPI(node)
}
