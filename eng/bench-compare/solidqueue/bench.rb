# Solid Queue benchmark driver, run inside the app: (cd app && bin/rails runner ../bench.rb --mode drain --n 20000 --workers 16 --out f.json)
# Same modes and JSON fields as the other harnesses. Every run truncates the solid_queue_* tables first.
# Consumers are `bin/jobs` (Solid Queue's supervisor): min(workers, cores) processes x the rest as threads, polling every 0.1 s.
# Steady latency is created_at (insert) to finished_at (both written by Solid Queue) of every job, read back from SQLite.
require "etc"
o = { "mode" => "drain", "n" => "20000", "calls" => "1", "workers" => "16", "rate" => "100", "seconds" => "30", "max-seconds" => "1800", "batch" => "1000", "out" => "" }
ARGV.each_slice(2) { |k, v| o[k.delete_prefix("--")] = v }
mode, n, calls, workers = o["mode"], o["n"].to_i, o["calls"].to_i, o["workers"].to_i
procs = [workers, Etc.nprocessors].min
ENV["BENCH_PROCESSES"] = procs.to_s
ENV["BENCH_THREADS"] = (workers.to_f / procs).ceil.to_s
pad = "x" * (1024 - 32)
batch = o["batch"].to_i
phase = ->(name) { $stderr.puts "@@PHASE #{name} #{(Time.now.to_f * 1000).to_i}"; $stderr.flush } # read by ci/run.py
db = ActiveRecord::Base.connection
clear = -> { db.disable_referential_integrity { db.tables.grep(/\Asolid_queue_/).each { |t| db.execute("DELETE FROM #{t}") } } }
clear.()
res = { system: "solid_queue #{SolidQueue::VERSION} (sqlite #{SQLite3::SQLITE_VERSION}, rails #{Rails.version}, ruby #{RUBY_VERSION})", mode: mode, workers: workers,
        processes: procs, threadsPerProcess: ENV["BENCH_THREADS"].to_i }
# The durability the writer connections actually run with (Rails' SQLite defaults unless database.yml says otherwise); workers use the same config.
pragmas = %w[journal_mode synchronous wal_autocheckpoint busy_timeout].to_h { |k| [k, db.select_value("PRAGMA #{k}")] }
res[:settings] = { store: "sqlite", mode: mode, payloadBytes: 1024, callers: calls, workerSlots: workers, bulkChunk: batch, processes: procs, threadsPerProcess: ENV["BENCH_THREADS"].to_i,
                   pragmas: pragmas, sqliteVersion: SQLite3::SQLITE_VERSION, cores: Etc.nprocessors }
# Jobs by state as the store holds them: finished, ready (in the ready table), claimed (being worked), failed, scheduled.
states = -> { ActiveRecord::Base.uncached { { finished: SolidQueue::Job.where.not(finished_at: nil).count, unfinished: SolidQueue::Job.where(finished_at: nil).count,
  ready: SolidQueue::ReadyExecution.count, claimed: SolidQueue::ClaimedExecution.count, failed: SolidQueue::FailedExecution.count, scheduled: SolidQueue::ScheduledExecution.count } } }
pct = ->(a, p) { a.empty? ? 0 : a.sort[[[0, (p * a.size).floor - 1].max, a.size - 1].min].round(2) }
# uncached: `rails runner` runs inside the executor, whose query cache would freeze these counts
open_jobs = -> { ActiveRecord::Base.uncached { SolidQueue::Job.where(finished_at: nil).count } }
done_jobs = -> { ActiveRecord::Base.uncached { SolidQueue::Job.where.not(finished_at: nil).count } }
mono = -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) }
start_jobs = -> { Process.spawn("bin/jobs", out: File::NULL, err: File::NULL, pgroup: true) }
stop_jobs = ->(pid) { Process.kill("TERM", -pid); Process.wait(pid) rescue nil }
bulk = ->(count) { (0...count).each_slice(batch) { |s| ActiveJob.perform_all_later(s.map { NoopJob.new(pad) }) } }

case mode
when "enqueue"
  NoopJob.perform_later(pad); clear.()
  left = Queue.new; n.times { left << 1 }; left.close
  lats = []; m = Mutex.new; phase.("timed-start"); t0 = mono.()
  calls.times.map { Thread.new { l = []; while left.pop; t = mono.(); NoopJob.perform_later(pad); l << (mono.() - t) * 1000; end; m.synchronize { lats.concat(l) } } }.each(&:join)
  el = mono.() - t0; phase.("timed-end")
  res.merge!(jobs: n, calls: calls, accepted: ActiveRecord::Base.uncached { SolidQueue::Job.count }, insertsPerSec: (n / el).round(1), insertLatencyMs: { p50: pct.(lats, 0.5), p95: pct.(lats, 0.95), p99: pct.(lats, 0.99) })
when "bulk"
  phase.("timed-start"); t0 = mono.(); bulk.(n); el = mono.() - t0; phase.("timed-end")
  res.merge!(jobs: n, batch: batch, calls: 1, accepted: ActiveRecord::Base.uncached { SolidQueue::Job.count }, insertsPerSec: (n / el).round(1))
when "drain"
  phase.("seed-start"); bulk.(n); phase.("seed-end")
  res[:readyBeforeStart] = states.()
  phase.("timed-start"); t0 = mono.(); pid = start_jobs.(); first = nil; maxs = o["max-seconds"].to_f; progress = []; nextp = 1.0
  while mono.() - t0 < maxs
    left = open_jobs.()
    if mono.() - t0 >= nextp then progress << [(mono.() - t0).round(1), n - left]; nextp = (mono.() - t0).floor + 1 end
    first ||= mono.() if left < n
    break if left.zero?
    sleep 0.05
  end
  t1 = mono.(); d = done_jobs.(); phase.("timed-end"); stop_jobs.(pid)
  res[:progress] = progress
  res[:clocks] = { workerStartSeconds: (t1 - t0).round(3), workerStartJobsPerSec: (d / (t1 - t0)).round(1), firstClaimSeconds: (t1 - (first || t0)).round(3), firstClaimJobsPerSec: (d / [t1 - (first || t0), 1e-9].max).round(1) }
  res.merge!(jobs: n, finished: d, drainSeconds: (t1 - t0).round(3), completionsPerSec: (d / (t1 - t0)).round(1),
             startupSeconds: ((first || t1) - t0).round(2), completionsPerSecExcludingStartup: (d / [t1 - (first || t0), 1e-9].max).round(1))
else
  rate, secs = o["rate"].to_f, o["seconds"].to_f
  pid = start_jobs.(); NoopJob.perform_later(pad)
  dl = mono.() + 120; sleep 0.05 while open_jobs.() > 0 && mono.() < dl
  clear.()
  phase.("timed-start"); t0 = mono.(); stop = t0 + secs; nxt = t0; inserted = 0
  while mono.() < stop
    NoopJob.perform_later(pad); inserted += 1
    nxt += 1 / rate; s = nxt - mono.(); sleep s if s > 0
  end
  dl = mono.() + 60; sleep 0.02 while open_jobs.() > 0 && mono.() < dl
  el = mono.() - t0; phase.("timed-end"); stop_jobs.(pid)
  lat = ActiveRecord::Base.uncached { db.select_values("SELECT (julianday(finished_at) - julianday(created_at)) * 86400000.0 FROM solid_queue_jobs WHERE finished_at IS NOT NULL") }.map(&:to_f)
  res.merge!(offeredPerSec: rate, inserted: inserted, completed: lat.size, completionsPerSec: (lat.size / el).round(1),
             latencyMs: { p50: pct.(lat, 0.5), p95: pct.(lat, 0.95), p99: pct.(lat, 0.99), max: pct.(lat, 1) })
end
res[:outcome] = states.()
txt = JSON.pretty_generate(res); puts txt
File.write(o["out"], txt) unless o["out"].empty?
