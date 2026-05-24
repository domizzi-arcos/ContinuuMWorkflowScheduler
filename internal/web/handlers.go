package web

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

// RedisCategoryCount is one row of the Redis key-category histogram returned
// by GET /api/redis. Name is the category prefix (e.g. "task", "execution")
// and Count is how many keys currently fall under it.
type RedisCategoryCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// RedisSnapshot is the JSON payload returned by GET /api/redis. It bundles
// the total key count, a sorted per-category breakdown, and the raw key/value
// entries themselves so the dashboard can render a single coherent view of
// the store at GeneratedAt.
type RedisSnapshot struct {
	KeyCount    int                  `json:"key_count"`
	Categories  []RedisCategoryCount `json:"categories"`
	Entries     []store.RawEntry     `json:"entries"`
	GeneratedAt time.Time            `json:"generated_at"`
}

// ControlRequest is the body accepted by POST /api/control. Paused=true asks
// the controller to pause the simulation; Paused=false resumes it.
type ControlRequest struct {
	Paused bool `json:"paused"`
}

// handleTasks serves GET /api/tasks. Returns a JSON array of *models.Task
// backed by store.RedisStore.GetAllTasks.
func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := s.store.GetAllTasks()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, tasks)
}

// handleExecutions serves GET /api/executions. Returns a JSON array of
// *models.Execution backed by store.RedisStore.GetAllExecutions.
func (s *Server) handleExecutions(w http.ResponseWriter, r *http.Request) {
	executions, err := s.store.GetAllExecutions()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, executions)
}

// handleServers serves GET /api/servers. Returns a JSON array of
// *models.Server backed by store.RedisStore.GetAllServers.
func (s *Server) handleServers(w http.ResponseWriter, r *http.Request) {
	servers, err := s.store.GetAllServers()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, servers)
}

// handleResults serves GET /api/results. Returns a JSON array of
// *models.TaskResult backed by store.RedisStore.GetAllTaskResults.
func (s *Server) handleResults(w http.ResponseWriter, r *http.Request) {
	results, err := s.store.GetAllTaskResults()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, results)
}

// handleRedis serves GET /api/redis. It pulls every raw key from
// store.RedisStore.GetAllRawEntries, derives a per-category histogram, and
// returns a RedisSnapshot. Categories are sorted by descending count with
// ties broken alphabetically so the dashboard renders a stable ordering.
func (s *Server) handleRedis(w http.ResponseWriter, r *http.Request) {
	entries, err := s.store.GetAllRawEntries()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	categoryCounts := make(map[string]int)
	for _, entry := range entries {
		categoryCounts[entry.Category]++
	}

	categories := make([]RedisCategoryCount, 0, len(categoryCounts))
	for name, count := range categoryCounts {
		categories = append(categories, RedisCategoryCount{Name: name, Count: count})
	}
	sort.Slice(categories, func(i, j int) bool {
		if categories[i].Count == categories[j].Count {
			return categories[i].Name < categories[j].Name
		}
		return categories[i].Count > categories[j].Count
	})

	jsonOK(w, RedisSnapshot{
		KeyCount:    len(entries),
		Categories:  categories,
		Entries:     entries,
		GeneratedAt: time.Now().UTC(),
	})
}

// handleControl serves /api/control for both GET and POST. GET returns the
// current control.Snapshot. POST decodes a ControlRequest and forwards to
// control.Controller.Pause or Resume, returning the resulting snapshot.
// Responds 503 if no controller is wired in.
func (s *Server) handleControl(w http.ResponseWriter, r *http.Request) {
	if s.control == nil {
		http.Error(w, "control unavailable", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		jsonOK(w, s.control.Snapshot())
	case http.MethodPost:
		var req ControlRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid control payload", http.StatusBadRequest)
			return
		}
		if req.Paused {
			snapshot := s.control.Pause()
			slog.Info("simulation paused from dashboard", "component", "control")
			jsonOK(w, snapshot)
			return
		}
		snapshot := s.control.Resume()
		slog.Info("simulation resumed from dashboard", "component", "control")
		jsonOK(w, snapshot)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleRestart serves POST /api/control/restart. It invokes the Server's
// restart callback (typically wired by cmd to tear down and reseed a fresh
// simulationSession) and then returns the post-restart control.Snapshot. If
// no controller is attached the response is 204 No Content; if no restart
// callback is wired the response is 503.
func (s *Server) handleRestart(w http.ResponseWriter, r *http.Request) {
	if s.restart == nil {
		http.Error(w, "restart unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := s.restart(); err != nil {
		slog.Error("simulation restart failed", "component", "control", "err", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	slog.Info("simulation restarted from dashboard", "component", "control")
	if s.control == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	jsonOK(w, s.control.Snapshot())
}
