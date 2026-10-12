package runner

import (
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

func newTestStore(t *testing.T) *store.RedisStore {
	t.Helper()
	mr := miniredis.RunT(t)
	s, err := store.NewRedisStore(mr.Addr(), 0)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	return s
}

func TestIsDrained(t *testing.T) {
	tests := []struct {
		name     string
		statuses []string
		results  int
		want     bool
	}{
		{"empty", nil, 0, true},
		{"all done with results", []string{"DONE", "DONE"}, 2, true},
		{"done but results lagging", []string{"DONE", "DONE"}, 1, false},
		{"one pending", []string{"DONE", "PENDING"}, 1, false},
		{"one blocked", []string{"DONE", "BLOCKED"}, 1, false},
		{"one queued", []string{"QUEUED"}, 0, false},
		{"one running", []string{"RUNNING"}, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			for i, st := range tc.statuses {
				ex := &models.Execution{ID: string(rune('a' + i)), Status: st}
				if err := s.StoreExecution(ex); err != nil {
					t.Fatalf("StoreExecution: %v", err)
				}
			}
			for i := 0; i < tc.results; i++ {
				if err := s.StoreTaskResult(&models.TaskResult{ExecutionID: string(rune('a' + i))}); err != nil {
					t.Fatalf("StoreTaskResult: %v", err)
				}
			}
			got, err := IsDrained(s)
			if err != nil {
				t.Fatalf("IsDrained: %v", err)
			}
			if got != tc.want {
				t.Errorf("IsDrained = %v, want %v", got, tc.want)
			}
		})
	}
}
