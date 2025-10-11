package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type KVStore struct {
	store map[string][]byte
	mu    sync.RWMutex
}

var (
	kvStore KVStore
	logger  *zap.Logger
	peersMu sync.RWMutex
	peers   = make(map[string]bool) // dynamic peer list with health
	selfURL string                  // current node's own address
)

type PeerJoinRequest struct {
	Address string `json:"address"`
}

func WriteKV(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Read error", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	// Write locally
	kvStore.mu.Lock()
	kvStore.store[key] = body
	kvStore.mu.Unlock()

	if r.Header.Get("X-Replicated") == "true" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Fan-out replication
	var wg sync.WaitGroup
	peersMu.RLock()
	for peer := range peers {
		if peer == selfURL {
			continue // do not replicate to self
		}
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			url := fmt.Sprintf("%s/write?key=%s", peer, key)
			req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
			req.Header.Set("Content-Type", "text/plain")
			req.Header.Set("X-Replicated", "true")

			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Do(req)
			if err != nil || resp.StatusCode != http.StatusOK {
				logger.Warn("peer replication failed", zap.String("peer", peer))
				peersMu.Lock()
				delete(peers, peer) // remove unhealthy peer
				peersMu.Unlock()
				return
			}
			resp.Body.Close()
		}(peer)
	}
	peersMu.RUnlock()
	wg.Wait()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Write success for key=%s\n", key)
}

func ReadKV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		logger.Warn("method not allowed")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		logger.Warn("missing key")
		http.Error(w, "Missing 'key' query parameter", http.StatusBadRequest)
		return
	}

	value, ok := kvStore.store[key]
	if !ok {
		logger.Warn("key not found", zap.String("key", key))
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintln(w, "Key not found")
		return
	}

	logger.Info("key read", zap.String("key", key), zap.ByteString("value", value))
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, string(value))
}

// Peer joins cluster
func JoinPeer(w http.ResponseWriter, r *http.Request) {
	var req PeerJoinRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil || req.Address == "" {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if req.Address == selfURL {
		w.WriteHeader(http.StatusOK)
		return // do not add self
	}

	peersMu.Lock()
	peers[req.Address] = true
	peersMu.Unlock()

	logger.Info("Peer joined", zap.String("address", req.Address))
	w.WriteHeader(http.StatusOK)
}

func ListPeers(w http.ResponseWriter, r *http.Request) {
	peersMu.RLock()
	defer peersMu.RUnlock()

	list := make([]string, 0, len(peers))
	for peer := range peers {
		list = append(list, peer)
	}
	_ = json.NewEncoder(w).Encode(list)
}

func BootstrapPeers() {
	initial := os.Getenv("PEERS")
	self := os.Getenv("SELF")
	if self == "" {
		logger.Fatal("SELF env variable not set")
	}
	selfURL = self

	if initial == "" {
		return
	}
	for _, p := range strings.Split(initial, ",") {
		if p == self {
			continue // don't include self in peer list
		}
		peers[p] = true
	}
}

func main() {

	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		panic("logger error")
	}
	defer logger.Sync()

	kvStore = KVStore{store: make(map[string][]byte)}
	BootstrapPeers()

	http.HandleFunc("/write", WriteKV)
	http.HandleFunc("/read", ReadKV)
	http.HandleFunc("/join", JoinPeer)
	http.HandleFunc("/peers", ListPeers)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	logger.Info("starting server", zap.String("port", port))
	_ = http.ListenAndServe(":"+port, nil)
}
