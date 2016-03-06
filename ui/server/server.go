// Copyright 2016-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package server

import (
	"net/http"

	"github.com/go-kit/kit/log"

	"github.com/olivere/taskqueue"
)

// Server is a simple web server with a WebSocket backend.
type Server struct {
	logger log.Logger
	m      *taskqueue.Manager
}

// New initializes a new Server.
func New(logger log.Logger, m *taskqueue.Manager) *Server {
	return &Server{
		logger: logger,
		m:      m,
	}
}

// Serve initializes the mux and starts the web server at the given address.
func (srv *Server) Serve(addr string) error {
	r := http.DefaultServeMux
	r.Handle("/ws", wsserver{logger: srv.logger, m: srv.m})
	r.Handle("/", http.FileServer(http.Dir("public")))
	go h.run() // run websocket hub
	return http.ListenAndServe(addr, r)
}
