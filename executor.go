package gocron

import (
	"context"
	"sync"
	"github.com/google/uuid"
)

type executor struct {
	jobsWg         sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	tasks          chan job
	stop           chan struct{}
	done           chan struct{}
	logger         Logger
	limitMode      LimitMode
	maxRunningJobs int
	runningJobs    *runningJobs
}

type runningJobs struct {
	mu   sync.Mutex
	jobs map[uuid.UUID]int
}

func (r *runningJobs) add(id uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs[id]++
}

func (r *runningJobs) remove(id uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs[id]--
	if r.jobs[id] <= 0 {
		delete(r.jobs, id)
	}
}

func (r *runningJobs) count(id uuid.UUID) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.jobs[id]
}

func (e *executor) execute(j *job) {
	select {
	case <-e.ctx.Done():
		return
	default:
	}

	e.runningJobs.add(j.ID())
	defer e.runningJobs.remove(j.ID())

	if j.singletonMode != nil {
		switch *j.singletonMode {
		case RescheduleMode:
			if e.runningJobs.count(j.ID()) > 1 {
				return
			}
		case WaitMode:
			select {
			case <-e.ctx.Done():
				return
			case j.singletonQueue <- struct{}{}:
				defer func() {
					<-j.singletonQueue
				}()
			}
		}
	}

	if e.limitMode != nil {
		switch *e.limitMode {
		case RescheduleMode:
			// check limit
		case WaitMode:
			select {
			case <-e.ctx.Done():
				return
			case e.limitQueue <- struct{}{}:
				defer func() {
					<-e.limitQueue
				}()
			}
		}
	}

	// Run the job
	// ...
}
