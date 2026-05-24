// Package control implements a shared pause/resume primitive for the
// simulation's hot loops. Generator, scheduler, worker and tracker goroutines
// all poll a single Controller via Wait and Sleep so the dashboard can pause
// the whole pipeline coherently. Active elapsed time (wall clock minus
// accumulated paused time) is also tracked here.
package control

import (
	"sync"
	"time"
)

const pausePollInterval = 100 * time.Millisecond

// Snapshot is the JSON-friendly view of the controller state exposed to the
// dashboard. PauseStarted is set only while paused.
type Snapshot struct {
	Paused       bool       `json:"paused"`
	PauseStarted *time.Time `json:"pause_started,omitempty"`
}

// Controller coordinates pause/resume across every hot loop in the
// simulation. The zero value is not usable; construct with New. All methods
// are safe for concurrent use.
type Controller struct {
	mu          sync.RWMutex
	paused      bool
	pausedAt    time.Time
	totalPaused time.Duration
}

// New returns a fresh Controller in the running (not paused) state.
func New() *Controller {
	return &Controller{}
}

// Pause transitions the controller into the paused state. Calling Pause when
// already paused is a no-op that still returns the current snapshot.
func (c *Controller) Pause() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.paused {
		return c.snapshotLocked(time.Now().UTC())
	}
	now := time.Now().UTC()
	c.paused = true
	c.pausedAt = now
	return c.snapshotLocked(now)
}

// Resume transitions the controller back to running and folds the time
// spent paused into totalPaused so ActiveElapsed stays monotonic. Calling
// Resume while not paused is a no-op.
func (c *Controller) Resume() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.paused {
		return c.snapshotLocked(time.Now().UTC())
	}
	now := time.Now().UTC()
	c.totalPaused += now.Sub(c.pausedAt)
	c.paused = false
	c.pausedAt = time.Time{}
	return c.snapshotLocked(now)
}

// Snapshot returns the controller's current state for serialization.
func (c *Controller) Snapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snapshotLocked(time.Now().UTC())
}

// IsPaused is a cheap read-locked check used by hot loops that want to log
// "paused" status without going through Wait.
func (c *Controller) IsPaused() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.paused
}

// Reset clears the paused flag and discards the accumulated paused duration.
// Used by the main loop when starting a new session so timing starts fresh.
func (c *Controller) Reset() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = false
	c.pausedAt = time.Time{}
	c.totalPaused = 0
	return c.snapshotLocked(time.Now().UTC())
}

// Wait blocks while the controller is paused and returns true once the
// caller may proceed. It returns false if done is closed first so the caller
// can exit its loop cleanly. The wait polls at pausePollInterval rather than
// using a condition variable so cancellation through done remains prompt.
func (c *Controller) Wait(done <-chan struct{}) bool {
	for {
		select {
		case <-done:
			return false
		default:
		}

		c.mu.RLock()
		paused := c.paused
		c.mu.RUnlock()
		if !paused {
			return true
		}
		select {
		case <-done:
			return false
		case <-time.After(pausePollInterval):
		}
	}
}

// Sleep is a pause-aware time.Sleep: it returns true after d of running
// time has elapsed, or false if done is closed first. The sleep is broken
// into pausePollInterval-sized steps so pauses freeze the countdown and
// shutdown is observed quickly. Time spent paused does not count toward d.
func (c *Controller) Sleep(done <-chan struct{}, d time.Duration) bool {
	remaining := d
	for remaining > 0 {
		if !c.Wait(done) {
			return false
		}
		step := remaining
		if step > pausePollInterval {
			step = pausePollInterval
		}
		start := time.Now()
		timer := time.NewTimer(step)
		select {
		case <-done:
			if !timer.Stop() {
				<-timer.C
			}
			return false
		case <-timer.C:
		}
		remaining -= time.Since(start)
	}
	return true
}

// ActiveElapsed returns wall-clock time since startedAt minus any time the
// controller has spent paused (including an in-progress pause). The result
// is clamped at zero so callers do not need to defend against negatives if
// startedAt is in the future.
func (c *Controller) ActiveElapsed(startedAt time.Time) time.Duration {
	now := time.Now().UTC()
	c.mu.RLock()
	paused := c.paused
	pausedAt := c.pausedAt
	totalPaused := c.totalPaused
	c.mu.RUnlock()

	elapsed := now.Sub(startedAt) - totalPaused
	if paused {
		elapsed -= now.Sub(pausedAt)
	}
	if elapsed < 0 {
		return 0
	}
	return elapsed
}

func (c *Controller) snapshotLocked(now time.Time) Snapshot {
	snapshot := Snapshot{Paused: c.paused}
	if c.paused {
		started := c.pausedAt
		if started.IsZero() {
			started = now
		}
		snapshot.PauseStarted = &started
	}
	return snapshot
}
