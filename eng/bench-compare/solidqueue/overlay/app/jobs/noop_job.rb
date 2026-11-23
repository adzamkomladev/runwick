require "digest"

class NoopJob < ActiveJob::Base
  # Shared CPU-heavy job (eng/bench-compare/ci/README.md): BENCH_CPU_ITERS rounds of SHA-256 over 1 KiB; unset or 0 = no-op.
  ITERS = ENV.fetch("BENCH_CPU_ITERS", "0").to_i

  def perform(pad)
    return if ITERS <= 0
    b = ("x" * 1024).b
    ITERS.times { b[0, 32] = Digest::SHA256.digest(b) }
  end
end
