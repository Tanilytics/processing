package server

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

var ready atomic.Bool

type statusResponse struct {
	Status string `json:"status"`
}

// SetReady marks the service as ready to serve traffic.
func SetReady() {
	ready.Store(true)
}

// SetNotReady marks the service as not ready to serve traffic.
func SetNotReady() {
	ready.Store(false)
}

// Healthz is a liveness probe. Returns 200 OK if the process is running.
func Healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

// Readyz is a readiness probe. Returns 200 OK only when service is ready.
func Readyz(w http.ResponseWriter, _ *http.Request) {
	if !ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, statusResponse{Status: "not ready"})
		return
	}

	writeJSON(w, http.StatusOK, statusResponse{Status: "ready"})
}

func writeJSON(w http.ResponseWriter, status int, payload statusResponse) {
	body, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		return
	}
}
