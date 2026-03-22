package server

import "net/http"

func NewServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", Healthz)

	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}
