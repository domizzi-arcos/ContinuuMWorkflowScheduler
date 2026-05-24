// Package web serves the dashboard for the scheduler: a small REST API over
// the Redis store, plus a WebSocket hub that fans out a periodic state
// snapshot to every connected client. It also exposes pause/resume and
// restart controls that drive the simulation lifecycle.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/control"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

//go:embed static
var staticFiles embed.FS

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// WSState is broadcast to every connected client every 500 ms. It is a full
// snapshot rather than a delta so a freshly connected dashboard can render
// the entire scene from the first frame.
type WSState struct {
	Servers    []*models.Server     `json:"servers"`
	Tasks      []*models.Task       `json:"tasks"`
	Executions []*models.Execution  `json:"executions"`
	Results    []*models.TaskResult `json:"results"`
	Control    control.Snapshot     `json:"control"`
	Runtime    RuntimeState         `json:"runtime"`
	Timestamp  time.Time            `json:"timestamp"`
}

// RuntimeState describes the lifecycle of the current run as observed by the
// main loop. Status is one of "running", "paused" or "finished";
// RestartAvailable is true after a run has finished and a fresh session can
// be started from the dashboard.
type RuntimeState struct {
	Status           string `json:"status"`
	Active           bool   `json:"active"`
	RestartAvailable bool   `json:"restart_available"`
}

// hub is the WebSocket fan-out point. All client lifecycle changes
// (connect, disconnect) and outbound frames flow through its channels so the
// client map is only ever touched from hub.run's single goroutine — no mutex
// required.
type hub struct {
	clients    map[*wsClient]bool
	broadcast  chan []byte // outbound frames produced by the periodic broadcaster
	register   chan *wsClient
	unregister chan *wsClient
}

func newHub() *hub {
	return &hub{
		clients:    make(map[*wsClient]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *wsClient),
		unregister: make(chan *wsClient),
	}
}

// run is the hub's event loop. It owns the clients map and serialises every
// mutation through its channels. On broadcast a slow client whose send buffer
// is full is evicted (channel closed, removed from the map) rather than
// blocking the fan-out for everyone else.
func (h *hub) run() {
	for {
		select {
		case c := <-h.register:
			h.clients[c] = true
		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}
		case msg := <-h.broadcast:
			for c := range h.clients {
				select {
				case c.send <- msg:
				default:
					// Slow client — drop it rather than stall the hub.
					close(c.send)
					delete(h.clients, c)
				}
			}
		}
	}
}

// wsClient is the hub's view of a single connected dashboard. send is the
// per-client outbound queue; the hub pushes frames into it and writePump
// drains them onto the wire.
type wsClient struct {
	hub  *hub
	conn *websocket.Conn
	send chan []byte
}

func (c *wsClient) writePump() {
	ping := time.NewTicker(54 * time.Second)
	defer func() {
		ping.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ping.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *wsClient) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(4096)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			break
		}
	}
}

// Server is the dashboard HTTP + WebSocket front-end. It owns the mux router,
// the WebSocket hub, and the callbacks the simulation provides for querying
// runtime state and triggering a restart. restart is an indirection because
// the actual implementation (recreating simulationSession in cmd) lives above
// this package and would otherwise create an import cycle.
type Server struct {
	store   *store.RedisStore
	control *control.Controller
	runtime func() RuntimeState
	restart func() error
	hub     *hub
	port    int
	router  *mux.Router
}

// New builds a Server bound to the given store, controller, runtime probe and
// restart callback. The returned Server is fully wired (routes registered)
// but does not start listening until Start is called. runtime or restart may
// be nil; the corresponding endpoints then degrade gracefully.
func New(s *store.RedisStore, ctrl *control.Controller, runtime func() RuntimeState, restart func() error, port int) *Server {
	h := newHub()
	srv := &Server{store: s, control: ctrl, runtime: runtime, restart: restart, hub: h, port: port, router: mux.NewRouter()}
	srv.setupRoutes()
	return srv
}

func (s *Server) setupRoutes() {
	api := s.router.PathPrefix("/api").Subrouter()
	api.Use(corsMiddleware)
	api.HandleFunc("/tasks", s.handleTasks).Methods("GET")
	api.HandleFunc("/executions", s.handleExecutions).Methods("GET")
	api.HandleFunc("/servers", s.handleServers).Methods("GET")
	api.HandleFunc("/results", s.handleResults).Methods("GET")
	api.HandleFunc("/redis", s.handleRedis).Methods("GET")
	api.HandleFunc("/control", s.handleControl).Methods("GET", "POST")
	api.HandleFunc("/control/restart", s.handleRestart).Methods("POST")

	s.router.HandleFunc("/ws", s.handleWS)

	staticFS, _ := fs.Sub(staticFiles, "static")
	s.router.PathPrefix("/").Handler(http.FileServer(http.FS(staticFS)))
}

// Start launches the dashboard. It spins up three goroutines — the hub event
// loop, the periodic state broadcaster, and net/http's ListenAndServe — and
// then blocks on done. Once done is closed it gracefully shuts the HTTP
// server down with a 5 s timeout. Start is intended to be called from the
// main lifecycle goroutine; it returns only after shutdown completes.
func (s *Server) Start(done <-chan struct{}) {
	go s.hub.run()
	go s.broadcaster(done)

	addr := fmt.Sprintf(":%d", s.port)
	slog.Info("web dashboard started", "component", "web", "addr", fmt.Sprintf("http://localhost%s", addr))

	httpSrv := &http.Server{Addr: addr, Handler: s.router}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("web server error", "component", "web", "err", err)
		}
	}()

	<-done
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	httpSrv.Shutdown(ctx)
	slog.Info("web server stopped", "component", "web")
}

// broadcaster builds and ships a fresh WSState every 500 ms for the lifetime
// of the run. The cadence matches the scheduler's poll interval so the
// dashboard sees roughly one frame per scheduling tick. JSON encoding happens
// once here and the resulting byte slice is fanned out to every client by
// the hub — the per-client send is non-blocking, so a single stuck client
// cannot delay the broadcast.
func (s *Server) broadcaster(done <-chan struct{}) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			state, err := s.buildState()
			if err != nil {
				continue
			}
			data, err := json.Marshal(state)
			if err != nil {
				continue
			}
			// Drop this frame if the hub's broadcast buffer is full;
			// the next tick will produce a fresher snapshot anyway.
			select {
			case s.hub.broadcast <- data:
			default:
			}
		case <-done:
			return
		}
	}
}

func (s *Server) buildState() (*WSState, error) {
	servers, err := s.store.GetAllServers()
	if err != nil {
		return nil, err
	}
	tasks, err := s.store.GetAllTasks()
	if err != nil {
		return nil, err
	}
	executions, err := s.store.GetAllExecutions()
	if err != nil {
		return nil, err
	}
	results, err := s.store.GetAllTaskResults()
	if err != nil {
		return nil, err
	}
	controlState := control.Snapshot{}
	if s.control != nil {
		controlState = s.control.Snapshot()
	}
	runtimeState := RuntimeState{Status: "running", Active: true}
	if s.runtime != nil {
		runtimeState = s.runtime()
	}
	return &WSState{
		Servers:    servers,
		Tasks:      tasks,
		Executions: executions,
		Results:    results,
		Control:    controlState,
		Runtime:    runtimeState,
		Timestamp:  time.Now().UTC(),
	}, nil
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	client := &wsClient{hub: s.hub, conn: conn, send: make(chan []byte, 32)}
	s.hub.register <- client
	go client.writePump()
	client.readPump()
}

func jsonOK(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
