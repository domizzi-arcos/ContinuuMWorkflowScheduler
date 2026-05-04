package control

import (
	"sync"
	"time"
)

const pausePollInterval = 100 * time.Millisecond

type Snapshot struct {
	Paused       bool       `json:"paused"`
	PauseStarted *time.Time `json:"pause_started,omitempty"`
}

type Controller struct {
	mu          sync.RWMutex
	paused      bool
	pausedAt    time.Time
	totalPaused time.Duration
}

func New() *Controller {
	return &Controller{}
}

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

func (c *Controller) Snapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snapshotLocked(time.Now().UTC())
}

func (c *Controller) IsPaused() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.paused
}

func (c *Controller) Reset() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.paused = false
	c.pausedAt = time.Time{}
	c.totalPaused = 0
	return c.snapshotLocked(time.Now().UTC())
}

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
