package server

import "net/http"

func NewServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", Healthz)
	mux.HandleFunc("/readyz", Readyz)

	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}
