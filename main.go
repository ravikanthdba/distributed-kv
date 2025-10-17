package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// -----------------------------
// Constants and Configs
// -----------------------------

const (
	defaultPort           = "8080"
	defaultRequestTimeout = 2 * time.Second
	headerReplicated      = "X-Replicated"
	envPeers              = "PEERS"
	envSelf               = "SELF"
	envPort               = "PORT"
)

// -----------------------------
// Types and Globals
// -----------------------------

type KVStore struct {
	mu    sync.RWMutex
	store map[string][]byte
}

type PeerJoinRequest struct {
	Address string `json:"address"`
}

var (
	store   = KVStore{store: make(map[string][]byte)}
	logger  *zap.Logger
	selfURL string

	peers   = make(map[string]bool)
	peersMu sync.RWMutex
)

// -----------------------------
// Store Logic
// -----------------------------

func (s *KVStore) Set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[key] = value
}

func (s *KVStore) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.store[key]
	return val, ok
}

// -----------------------------
// Peer Management
// -----------------------------

func addPeer(address string) {
	peersMu.Lock()
	defer peersMu.Unlock()
	peers[address] = true
}

func removePeer(address string) {
	peersMu.Lock()
	defer peersMu.Unlock()
	delete(peers, address)
}

func listPeers() []string {
	peersMu.RLock()
	defer peersMu.RUnlock()
	peerList := make([]string, 0, len(peers))
	for p := range peers {
		peerList = append(peerList, p)
	}
	return peerList
}

// -----------------------------
// HTTP Handlers
// -----------------------------

func WriteKVHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.URL.Query().Get("key"))
	if key == "" {
		respondError(w, http.StatusBadRequest, "Missing 'key' query parameter")
		return
	}

	defer r.Body.Close()
	value, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Error("failed to read body", zap.Error(err))
		respondError(w, http.StatusInternalServerError, "Unable to read body")
		return
	}

	store.Set(key, value)
	logger.Info("Value written", zap.String("key", key), zap.Int("size", len(value)))

	if r.Header.Get(headerReplicated) != "true" {
		go replicateToPeers(key, value)
	}

	respondOK(w, fmt.Sprintf("Write success for key=%s", key))
}

func ReadKVHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.URL.Query().Get("key"))
	if key == "" {
		respondError(w, http.StatusBadRequest, "Missing 'key' query parameter")
		return
	}

	value, ok := store.Get(key)
	if !ok {
		respondError(w, http.StatusNotFound, "Key not found")
		return
	}

	logger.Info("Value read", zap.String("key", key))
	respondText(w, string(value))
}

func JoinPeerHandler(w http.ResponseWriter, r *http.Request) {
	var req PeerJoinRequest
	defer r.Body.Close()

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Address) == "" {
		logger.Warn("Invalid join request", zap.Error(err))
		respondError(w, http.StatusBadRequest, "Invalid peer join request")
		return
	}

	if req.Address == selfURL {
		respondOK(w, "Self address, not added")
		return
	}

	addPeer(req.Address)
	logger.Info("Peer joined", zap.String("peer", req.Address))
	respondOK(w, "Peer added")
}

func ListPeersHandler(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, listPeers())
}

// -----------------------------
// Replication Logic
// -----------------------------

func replicateToPeers(key string, value []byte) {
	peersMu.RLock()
	defer peersMu.RUnlock()

	var wg sync.WaitGroup

	for peer := range peers {
		if peer == selfURL {
			continue
		}

		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			url := fmt.Sprintf("%s/write?key=%s", peer, key)

			req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(value))
			if err != nil {
				logger.Error("Failed to create replication request", zap.String("peer", peer), zap.Error(err))
				return
			}
			req.Header.Set("Content-Type", "text/plain")
			req.Header.Set(headerReplicated, "true")

			client := &http.Client{Timeout: defaultRequestTimeout}
			resp, err := client.Do(req)
			if err != nil || resp.StatusCode != http.StatusOK {
				logger.Warn("Replication failed", zap.String("peer", peer), zap.Error(err))
				removePeer(peer)
				return
			}
			_ = resp.Body.Close()
		}(peer)
	}

	wg.Wait()
}

// -----------------------------
// Bootstrap & Init
// -----------------------------

func BootstrapPeers() {
	self := os.Getenv(envSelf)
	if self == "" {
		logger.Fatal("SELF env variable not set")
	}
	selfURL = self

	initial := os.Getenv(envPeers)
	for _, peer := range strings.Split(initial, ",") {
		peer = strings.TrimSpace(peer)
		if peer != "" && peer != selfURL {
			addPeer(peer)
		}
	}
}

// -----------------------------
// Response Helpers
// -----------------------------

func respondOK(w http.ResponseWriter, msg string) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(msg + "\n"))
}

func respondError(w http.ResponseWriter, code int, msg string) {
	logger.Warn("Request error", zap.Int("code", code), zap.String("msg", msg))
	http.Error(w, msg, code)
}

func respondJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to encode JSON")
	}
}

func respondText(w http.ResponseWriter, text string) {
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintln(w, text)
}

// -----------------------------
// Main
// -----------------------------

func main() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		panic("Failed to initialize logger: " + err.Error())
	}
	defer logger.Sync()

	BootstrapPeers()

	http.HandleFunc("/write", WriteKVHandler)
	http.HandleFunc("/read", ReadKVHandler)
	http.HandleFunc("/join", JoinPeerHandler)
	http.HandleFunc("/peers", ListPeersHandler)

	port := os.Getenv(envPort)
	if port == "" {
		port = defaultPort
	}

	serverAddr := ":" + port
	logger.Info("Starting KV Store", zap.String("address", serverAddr), zap.String("self", selfURL))

	if err := http.ListenAndServe(serverAddr, nil); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal("Server failed", zap.Error(err))
	}
}
