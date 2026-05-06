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

// WSState is broadcast to every connected client every 500 ms.
type WSState struct {
	Servers    []*models.Server     `json:"servers"`
	Tasks      []*models.Task       `json:"tasks"`
	Executions []*models.Execution  `json:"executions"`
	Results    []*models.TaskResult `json:"results"`
	Control    control.Snapshot     `json:"control"`
	Runtime    RuntimeState         `json:"runtime"`
	Timestamp  time.Time            `json:"timestamp"`
}

type RuntimeState struct {
	Status           string `json:"status"`
	Active           bool   `json:"active"`
	RestartAvailable bool   `json:"restart_available"`
}

type hub struct {
	clients    map[*wsClient]bool
	broadcast  chan []byte
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
					close(c.send)
					delete(h.clients, c)
				}
			}
		}
	}
}

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

type Server struct {
	store   *store.RedisStore
	control *control.Controller
	runtime func() RuntimeState
	restart func() error
	hub     *hub
	port    int
	router  *mux.Router
}

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
