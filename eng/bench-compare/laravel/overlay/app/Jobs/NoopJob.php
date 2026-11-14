<?php
namespace App\Jobs;

use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;

// $t is taken when the job is constructed, immediately before dispatch(), so latency = dispatch call start to handler end.
// BENCH_LAT_DIR (steady mode only) makes each worker append "t done" to its own file: no database write, no lock.
class NoopJob implements ShouldQueue
{
    use Queueable;
    public float $t;
    public function __construct(public string $pad) { $this->t = microtime(true); }
    public function handle(): void
    {
        if ($dir = getenv('BENCH_LAT_DIR')) {
            file_put_contents("$dir/" . getmypid() . '.lat', sprintf("%.6f %.6f\n", $this->t, microtime(true)), FILE_APPEND);
        }
    }
}
