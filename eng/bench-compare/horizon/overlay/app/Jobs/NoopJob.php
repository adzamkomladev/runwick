<?php
namespace App\Jobs;

use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;

class NoopJob implements ShouldQueue
{
    use Queueable;
    public function __construct(public string $pad) {}
    public function handle(): void
    {
        // Shared CPU-heavy job (eng/bench-compare/ci/README.md); BENCH_CPU_ITERS unset or 0 = no-op.
        $n = (int) getenv('BENCH_CPU_ITERS');
        $b = str_repeat('x', 1024);
        for ($i = 0; $i < $n; $i++) { $b = hash('sha256', $b, true) . substr($b, 32); }
    }
}
