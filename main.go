package main

import (
	"bytes"
	"fmt"
	"go.uber.org/zap"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

type KVStore struct {
	store map[string][]byte
}

var (
	kvStore KVStore
	logger  *zap.Logger
	peers   []string
)

func WriteKV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing 'key' query param", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Read error", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	// Step 1: Local write
	kvStore.store[key] = body
	logger.Info("local write", zap.String("key", key), zap.ByteString("value", body))

	// Step 2: Check if this is a replication request
	if r.Header.Get("X-Replicated") == "true" {
		logger.Info("replication request received — not re‑replicating", zap.String("key", key))
		w.WriteHeader(http.StatusOK)
		return
	}

	// Step 3: Parallel Replication with WaitGroup
	var wg sync.WaitGroup
	successChan := make(chan string, len(peers))
	failChan := make(chan string, len(peers))

	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			url := fmt.Sprintf("%s/write?key=%s", peer, key)
			req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
			req.Header.Set("Content-Type", "text/plain")
			req.Header.Set("X-Replicated", "true") // prevent re-replication

			client := &http.Client{Timeout: 3 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				logger.Error("replication failed", zap.String("peer", peer), zap.Error(err))
				failChan <- peer
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				logger.Info("replication success", zap.String("peer", peer))
				successChan <- peer
			} else {
				logger.Warn("peer returned non-OK", zap.String("peer", peer), zap.Int("status", resp.StatusCode))
				failChan <- peer
			}
		}(peer)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(successChan)
	close(failChan)

	// Count results
	successCount := len(successChan)
	failureCount := len(failChan)

	if failureCount > 0 {
		logger.Warn("partial replication", zap.Int("success", successCount), zap.Int("failure", failureCount))
		http.Error(w, "Partial replication", http.StatusAccepted)
		return
	}

	logger.Info("full replication success", zap.Int("success", successCount))
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Write replicated to %d peers\n", successCount)
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

func main() {
	var err error
	logger, err = zap.NewProduction()
	if err != nil {
		panic("failed to create logger")
	}
	defer logger.Sync()

	kvStore = KVStore{store: make(map[string][]byte)}

	// Read port and peers from environment
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	peerEnv := os.Getenv("PEERS")
	if peerEnv != "" {
		peers = strings.Split(peerEnv, ",")
		logger.Info("configured peers", zap.Strings("peers", peers))
	}

	r := mux.NewRouter()
	r.HandleFunc("/write", WriteKV).Methods("POST")
	r.HandleFunc("/read", ReadKV).Methods("GET")

	addr := fmt.Sprintf(":%s", port)
	logger.Info("Server starting", zap.String("addr", addr))
	if err := http.ListenAndServe(addr, r); err != nil {
		logger.Fatal("server error", zap.Error(err))
	}
}
