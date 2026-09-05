package backfill

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Options struct {
	MaxParallel int    `json:"max_parallel"`
	Workers     int    `json:"workers"`
	Retries     int    `json:"retries"`
	Reverse     bool   `json:"reverse"`
	Rerun       string `json:"rerun"`
	OnFailure   string `json:"on_failure"`
}

func (o Options) Validate() error {
	if o.MaxParallel < 1 || o.Workers < 1 {
		return fmt.Errorf("max-parallel and workers must be positive")
	}
	if o.Retries < 0 {
		return fmt.Errorf("retries must not be negative")
	}
	switch o.Rerun {
	case "", "failed", "missing", "all":
	default:
		return fmt.Errorf("rerun must be failed, missing, or all")
	}
	switch o.OnFailure {
	case "continue", "stop", "fail-fast":
	default:
		return fmt.Errorf("on-failure must be continue, stop, or fail-fast")
	}
	return nil
}

func Eligible(status Status, rerun string) bool {
	switch rerun {
	case "all":
		return true
	case "failed":
		return status == Failed
	case "missing":
		return status == Queued || status == Running || status == Cancelled
	default:
		return status != Succeeded
	}
}

type Summary struct {
	Total     int `json:"total"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Cancelled int `json:"cancelled"`
	Queued    int `json:"queued"`
	Running   int `json:"running"`
	Skipped   int `json:"skipped"` // not selected in this invocation, including previous successes
}

func Summarize(ctx context.Context, p Plan, s *Store, rerun string) (Summary, error) {
	total, err := p.Count(ctx)
	if err != nil {
		return Summary{}, err
	}
	summary := Summary{Total: total, Queued: total}
	queuedSkipped := !Eligible(Queued, rerun)
	if queuedSkipped {
		summary.Skipped = total
	}
	for r, err := range s.Records(ctx) {
		if err != nil {
			return summary, err
		}
		if err := p.validateInterval(r.Interval); err != nil {
			return summary, err
		}
		summary.Queued--
		if queuedSkipped {
			summary.Skipped--
		}
		if !Eligible(r.Status, rerun) {
			summary.Skipped++
		}
		switch r.Status {
		case Succeeded:
			summary.Succeeded++
		case Failed:
			summary.Failed++
		case Cancelled:
			summary.Cancelled++
		case Running:
			summary.Running++
		case Queued:
			summary.Queued++
		default:
			return summary, fmt.Errorf("unknown partition status %q", r.Status)
		}
	}
	return summary, nil
}

// Run executes a single attempt. The run ID has already been persisted before
// this is called; implementations must wait for the child to exit on cancellation.
type Run func(context.Context, Interval, string) error

// Execute keeps at most MaxParallel records in memory. The caller holds the store
// lock. A stopped or interrupted invocation leaves unstarted partitions queued.
func Execute(ctx context.Context, m Manifest, s *Store, o Options, run Run) (Summary, error) {
	if err := m.Plan.Validate(); err != nil {
		return Summary{}, err
	}
	if err := o.Validate(); err != nil {
		return Summary{}, err
	}
	before, err := Summarize(ctx, m.Plan, s, o.Rerun)
	if err != nil {
		return before, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		status Status
		err    error
	}
	results := make(chan result, o.MaxParallel)
	active := 0
	remaining := before.Total - before.Skipped
	stopped := false
	var executionErr error
	collect := func() {
		res := <-results
		active--
		if res.err != nil {
			executionErr = errors.Join(executionErr, res.err)
			stopped = true
			cancel()
		}
		if res.status == Failed && o.OnFailure != "continue" {
			stopped = true
			if o.OnFailure == "fail-fast" {
				cancel()
			}
		}
	}
	for i := range m.Plan.Intervals(o.Reverse) {
		if remaining == 0 {
			break
		}
		// Consume available completions before scheduling more work, so stop/fail-fast
		// does not launch additional partitions after observing an exhausted failure.
		for active > 0 && (active == o.MaxParallel || len(results) > 0) {
			collect()
		}
		if stopped || runCtx.Err() != nil {
			break
		}
		record, err := s.Read(i)
		if err != nil {
			executionErr = errors.Join(executionErr, err)
			cancel()
			break
		}
		if !Eligible(record.Status, o.Rerun) {
			continue
		}
		active++
		remaining--
		go func(r Record) {
			status, err := executePartition(runCtx, m.ID, s, r, o.Retries, run)
			results <- result{status, err}
		}(record)
	}
	for active > 0 {
		collect()
	}
	summary, err := Summarize(context.WithoutCancel(ctx), m.Plan, s, o.Rerun)
	summary.Skipped = before.Skipped
	executionErr = errors.Join(executionErr, err, ctx.Err())
	if summary.Failed > 0 {
		executionErr = errors.Join(executionErr, fmt.Errorf("%d backfill partitions failed", summary.Failed))
	}
	return summary, executionErr
}

func executePartition(ctx context.Context, id string, s *Store, r Record, retries int, run Run) (Status, error) {
	// A running attempt after acquiring the exclusive lock was interrupted. Keep
	// its history, but never confuse it with a completed success.
	if len(r.Attempts) > 0 && r.Attempts[len(r.Attempts)-1].Status == Running {
		a := &r.Attempts[len(r.Attempts)-1]
		now := time.Now().UTC()
		a.Status, a.FinishedAt, a.Error = Cancelled, &now, "previous executor interrupted"
	}
	for n := 0; n <= retries; n++ {
		if ctx.Err() != nil {
			return r.Status, nil
		}
		a := Attempt{RunID: id + "__" + uuid.NewString(), Status: Running, StartedAt: time.Now().UTC()}
		r.Status = Running
		r.Attempts = append(r.Attempts, a)
		if err := s.Save(r); err != nil {
			return Running, err
		}
		err := run(ctx, r.Interval, a.RunID)
		finished := time.Now().UTC()
		attempt := &r.Attempts[len(r.Attempts)-1]
		attempt.FinishedAt = &finished
		switch {
		case ctx.Err() != nil:
			r.Status = Cancelled
			attempt.Error = ctx.Err().Error()
		case err != nil:
			r.Status = Failed
			attempt.Error = err.Error()
		default:
			r.Status = Succeeded
		}
		attempt.Status = r.Status
		if err := s.Save(r); err != nil {
			return r.Status, err
		}
		if r.Status != Failed {
			return r.Status, nil
		}
	}
	return r.Status, nil
}

// LockedWriter prevents interleaved child output and races in custom writers.
type LockedWriter struct {
	Mu        sync.Mutex
	WriteFunc func([]byte) (int, error)
}

func (w *LockedWriter) Write(p []byte) (int, error) {
	w.Mu.Lock()
	defer w.Mu.Unlock()
	return w.WriteFunc(p)
}
