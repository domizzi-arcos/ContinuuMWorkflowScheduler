package web

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

type RedisCategoryCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type RedisSnapshot struct {
	KeyCount    int                  `json:"key_count"`
	Categories  []RedisCategoryCount `json:"categories"`
	Entries     []store.RawEntry     `json:"entries"`
	GeneratedAt time.Time            `json:"generated_at"`
}

type ControlRequest struct {
	Paused bool `json:"paused"`
}

func (s *Server) handleTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := s.store.GetAllTasks()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, tasks)
}

func (s *Server) handleExecutions(w http.ResponseWriter, r *http.Request) {
	executions, err := s.store.GetAllExecutions()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, executions)
}

func (s *Server) handleServers(w http.ResponseWriter, r *http.Request) {
	servers, err := s.store.GetAllServers()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, servers)
}

func (s *Server) handleResults(w http.ResponseWriter, r *http.Request) {
	results, err := s.store.GetAllTaskResults()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jsonOK(w, results)
}

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
