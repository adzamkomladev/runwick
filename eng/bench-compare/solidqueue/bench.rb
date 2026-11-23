# Solid Queue benchmark driver, run inside the app: (cd app && bin/rails runner ../bench.rb --mode drain --n 20000 --workers 16 --out f.json)
# Same modes and JSON fields as the other harnesses. Every run truncates the solid_queue_* tables first.
# Consumers are `bin/jobs` (Solid Queue's supervisor): min(workers, cores) processes x the rest as threads, polling every 0.1 s.
# Steady latency is created_at (insert) to finished_at (both written by Solid Queue) of every job, read back from PostgreSQL.
require "etc"
o = { "mode" => "drain", "n" => "20000", "calls" => "1", "workers" => "16", "rate" => "100", "seconds" => "30", "max-seconds" => "1800", "out" => "" }
ARGV.each_slice(2) { |k, v| o[k.delete_prefix("--")] = v }
mode, n, calls, workers = o["mode"], o["n"].to_i, o["calls"].to_i, o["workers"].to_i
procs = [workers, Etc.nprocessors].min
ENV["BENCH_PROCESSES"] = procs.to_s
ENV["BENCH_THREADS"] = (workers.to_f / procs).ceil.to_s
pad = "x" * (1024 - 32)
db = ActiveRecord::Base.connection
db.execute("TRUNCATE #{db.tables.grep(/\Asolid_queue_/).join(', ')} RESTART IDENTITY CASCADE")
res = { system: "solid_queue #{SolidQueue::VERSION} (rails #{Rails.version}, ruby #{RUBY_VERSION})", mode: mode, workers: workers,
        processes: procs, threadsPerProcess: ENV["BENCH_THREADS"].to_i }
pct = ->(a, p) { a.empty? ? 0 : a.sort[[[0, (p * a.size).floor - 1].max, a.size - 1].min].round(2) }
# uncached: `rails runner` runs inside the executor, whose query cache would freeze these counts
open_jobs = -> { ActiveRecord::Base.uncached { SolidQueue::Job.where(finished_at: nil).count } }
done_jobs = -> { ActiveRecord::Base.uncached { SolidQueue::Job.where.not(finished_at: nil).count } }
mono = -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) }
start_jobs = -> { Process.spawn("bin/jobs", out: File::NULL, err: File::NULL, pgroup: true) }
stop_jobs = ->(pid) { Process.kill("TERM", -pid); Process.wait(pid) rescue nil }
bulk = ->(count) { (0...count).each_slice(1000) { |s| ActiveJob.perform_all_later(s.map { NoopJob.new(pad) }) } }

case mode
when "enqueue"
  NoopJob.perform_later(pad); db.execute("TRUNCATE solid_queue_jobs, solid_queue_ready_executions RESTART IDENTITY CASCADE")
  left = Queue.new; n.times { left << 1 }; left.close
  lats = []; m = Mutex.new; t0 = mono.()
  calls.times.map { Thread.new { l = []; while left.pop; t = mono.(); NoopJob.perform_later(pad); l << (mono.() - t) * 1000; end; m.synchronize { lats.concat(l) } } }.each(&:join)
  el = mono.() - t0
  res.merge!(jobs: n, calls: calls, accepted: ActiveRecord::Base.uncached { SolidQueue::Job.count }, insertsPerSec: (n / el).round(1), insertLatencyMs: { p50: pct.(lats, 0.5), p95: pct.(lats, 0.95), p99: pct.(lats, 0.99) })
when "bulk"
  t0 = mono.(); bulk.(n); el = mono.() - t0
  res.merge!(jobs: n, batch: 1000, calls: 1, accepted: ActiveRecord::Base.uncached { SolidQueue::Job.count }, insertsPerSec: (n / el).round(1))
when "drain"
  bulk.(n)
  t0 = mono.(); pid = start_jobs.(); first = nil; maxs = o["max-seconds"].to_f
  while mono.() - t0 < maxs
    left = open_jobs.()
    first ||= mono.() if left < n
    break if left.zero?
    sleep 0.05
  end
  t1 = mono.(); d = done_jobs.(); stop_jobs.(pid)
  res.merge!(jobs: n, finished: d, drainSeconds: (t1 - t0).round(3), completionsPerSec: (d / (t1 - t0)).round(1),
             startupSeconds: ((first || t1) - t0).round(2), completionsPerSecExcludingStartup: (d / [t1 - (first || t0), 1e-9].max).round(1))
else
  rate, secs = o["rate"].to_f, o["seconds"].to_f
  pid = start_jobs.(); NoopJob.perform_later(pad)
  dl = mono.() + 120; sleep 0.05 while open_jobs.() > 0 && mono.() < dl
  db.execute("TRUNCATE solid_queue_jobs RESTART IDENTITY CASCADE")
  t0 = mono.(); stop = t0 + secs; nxt = t0; inserted = 0
  while mono.() < stop
    NoopJob.perform_later(pad); inserted += 1
    nxt += 1 / rate; s = nxt - mono.(); sleep s if s > 0
  end
  dl = mono.() + 60; sleep 0.02 while open_jobs.() > 0 && mono.() < dl
  el = mono.() - t0; stop_jobs.(pid)
  lat = ActiveRecord::Base.uncached { db.select_values("SELECT EXTRACT(EPOCH FROM finished_at - created_at) * 1000 FROM solid_queue_jobs WHERE finished_at IS NOT NULL") }.map(&:to_f)
  res.merge!(offeredPerSec: rate, inserted: inserted, completed: lat.size, completionsPerSec: (lat.size / el).round(1),
             latencyMs: { p50: pct.(lat, 0.5), p95: pct.(lat, 0.95), p99: pct.(lat, 0.99), max: pct.(lat, 1) })
end
txt = JSON.pretty_generate(res); puts txt
File.write(o["out"], txt) unless o["out"].empty?
