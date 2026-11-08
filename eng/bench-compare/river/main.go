// River comparison benchmark (docs/plans/perf-plan-draft.md PF-01, PQ4): no-op jobs against the same PostgreSQL as Runwick.
//
//	drain:  insert -n jobs in bulk, then start one client with -workers slots; report completions/s until all are done.
//	steady: one client; insert one job at a time at -rate/s for -seconds; report achieved completions/s and
//	        insert-to-completion latency (created_at to the completion event in this process).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

type NoopArgs struct {
	Pad string `json:"pad"`
}

func (NoopArgs) Kind() string { return "noop" }

type NoopWorker struct {
	river.WorkerDefaults[NoopArgs]
}

func (w *NoopWorker) Work(ctx context.Context, job *river.Job[NoopArgs]) error { return nil }

func main() {
	conn := flag.String("conn", "postgres://postgres:pg@127.0.0.1:55499/postgres", "admin connection URL")
	db := flag.String("db", "bench_river", "database to (re)create")
	mode := flag.String("mode", "drain", "drain|steady")
	n := flag.Int("n", 20000, "drain: jobs")
	rate := flag.Float64("rate", 100, "steady: inserts per second")
	seconds := flag.Int("seconds", 120, "steady: duration")
	workers := flag.Int("workers", 16, "worker slots")
	pad := flag.Int("payload", 1024, "payload bytes")
	out := flag.String("out", "", "result JSON file")
	cooldown := flag.Int("cooldown-ms", 100, "River FetchCooldown (default 100 ms)")
	calls := flag.Int("calls", 1, "enqueue: concurrent single-job inserters")
	flag.Parse()
	ctx := context.Background()

	admin, err := pgx.Connect(ctx, *conn)
	must(err)
	_, err = admin.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", *db))
	must(err)
	_, err = admin.Exec(ctx, "CREATE DATABASE "+*db)
	must(err)
	admin.Close(ctx)

	cfg, err := pgxpool.ParseConfig(replaceDB(*conn, *db))
	must(err)
	cfg.MaxConns = int32(*workers + 4)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	must(err)
	defer pool.Close()
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	must(err)
	_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	must(err)

	ws := river.NewWorkers()
	river.AddWorker(ws, &NoopWorker{})
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues:        map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: *workers}},
		Workers:       ws,
		FetchCooldown: time.Duration(*cooldown) * time.Millisecond,
	})
	must(err)
	events, cancel := client.Subscribe(river.EventKindJobCompleted, river.EventKindJobFailed)
	defer cancel()
	payload := NoopArgs{Pad: pad1(*pad)}
	result := map[string]any{"system": "river v0.50.0", "mode": *mode, "workers": *workers, "payloadBytes": *pad, "fetchCooldownMs": *cooldown}

	switch *mode {
	case "enqueue":
		// -n single-job Insert calls from -calls goroutines; no worker started. Measures adding, not running.
		var next atomic.Int64
		lat := make([][]float64, *calls)
		start := time.Now()
		done := make(chan struct{})
		for c := 0; c < *calls; c++ {
			go func(c int) {
				for next.Add(1) <= int64(*n) {
					t := time.Now()
					_, err := client.Insert(ctx, payload, nil)
					must(err)
					lat[c] = append(lat[c], float64(time.Since(t).Microseconds())/1000)
				}
				done <- struct{}{}
			}(c)
		}
		for c := 0; c < *calls; c++ {
			<-done
		}
		elapsed := time.Since(start).Seconds()
		all := []float64{}
		for _, l := range lat {
			all = append(all, l...)
		}
		sort.Float64s(all)
		result["jobs"] = *n
		result["calls"] = *calls
		result["insertsPerSec"] = float64(*n) / elapsed
		result["insertLatencyMs"] = map[string]float64{"p50": pct(all, 0.5), "p99": pct(all, 0.99)}
	case "drain":
		params := make([]river.InsertManyParams, 0, *n)
		for i := 0; i < *n; i++ {
			params = append(params, river.InsertManyParams{Args: payload})
		}
		insertStart := time.Now()
		for i := 0; i < len(params); i += 5000 {
			_, err = client.InsertManyFast(ctx, params[i:min(i+5000, len(params))])
			must(err)
		}
		result["insertSeconds"] = time.Since(insertStart).Seconds()
		start := time.Now()
		must(client.Start(ctx))
		done, failed := 0, 0
		for done+failed < *n {
			e := <-events
			if e.Kind == river.EventKindJobCompleted {
				done++
			} else {
				failed++
			}
		}
		elapsed := time.Since(start).Seconds()
		result["jobs"] = *n
		result["failed"] = failed
		result["drainSeconds"] = elapsed
		result["completionsPerSec"] = float64(done) / elapsed
	case "steady":
		must(client.Start(ctx))
		latencies := []float64{}
		stop := time.Now().Add(time.Duration(*seconds) * time.Second)
		var inserted atomic.Int64
		go func() {
			interval := time.Duration(float64(time.Second) / *rate)
			next := time.Now()
			for time.Now().Before(stop) {
				if _, err := client.Insert(ctx, payload, nil); err != nil {
					fmt.Fprintln(os.Stderr, "insert:", err)
				}
				inserted.Add(1)
				next = next.Add(interval)
				if d := time.Until(next); d > 0 {
					time.Sleep(d)
				}
			}
		}()
		start := time.Now()
		deadline := stop.Add(60 * time.Second)
		completed := 0
		for time.Now().Before(deadline) {
			select {
			case e := <-events:
				completed++
				latencies = append(latencies, float64(time.Since(e.Job.CreatedAt).Microseconds())/1000)
			case <-time.After(time.Second):
			}
			if time.Now().After(stop) && int64(completed) >= inserted.Load() {
				break
			}
		}
		elapsed := time.Since(start).Seconds()
		sort.Float64s(latencies)
		result["offeredPerSec"] = *rate
		result["inserted"] = inserted.Load()
		result["completed"] = completed
		result["completionsPerSec"] = float64(completed) / elapsed
		result["latencyMs"] = map[string]float64{"p50": pct(latencies, 0.5), "p95": pct(latencies, 0.95), "p99": pct(latencies, 0.99), "max": pct(latencies, 1)}
	}

	stopCtx, c2 := context.WithTimeout(ctx, 10*time.Second)
	defer c2()
	_ = client.Stop(stopCtx)
	b, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		must(os.WriteFile(*out, b, 0o644))
	}
}

func pad1(n int) string {
	b := make([]byte, max(0, n-16))
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	i := int(p*float64(len(xs))) - 1
	return xs[max(0, min(i, len(xs)-1))]
}

func replaceDB(url, db string) string {
	cfg, err := pgx.ParseConfig(url)
	must(err)
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, db)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
