<?php
namespace App\Console\Commands;

use App\Jobs\NoopJob;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Queue;

// One producer process. single: --n dispatch() calls back to back (or paced by --rate); bulk: Queue::bulk in chunks of 1000.
// Starts at --start-at (unix seconds) so several producers begin together; prints one JSON line.
class BenchProduce extends Command
{
    protected $signature = 'bench:produce {--n=1000} {--mode=single} {--start-at=0} {--rate=0} {--seconds=0}';
    protected $description = 'Benchmark producer';

    public function handle(): int
    {
        $pad = str_repeat('x', 1024 - 16);
        $n = (int) $this->option('n'); $mode = $this->option('mode'); $rate = (float) $this->option('rate');
        Queue::connection('redis')->size(); // open the connection before the clock starts
        while (microtime(true) < (float) $this->option('start-at')) { usleep(200); }
        $t0 = microtime(true); $lat = [];
        if ($mode === 'bulk') {
            for ($i = 0; $i < $n; $i += 1000) {
                Queue::bulk(array_map(fn () => new NoopJob($pad), range(1, min(1000, $n - $i))));
            }
        } elseif ($rate > 0) {
            $stop = $t0 + (float) $this->option('seconds'); $next = $t0; $n = 0;
            while (microtime(true) < $stop) {
                dispatch(new NoopJob($pad)); $n++;
                $next += 1 / $rate; $w = $next - microtime(true); if ($w > 0) usleep((int) ($w * 1e6));
            }
        } else {
            for ($i = 0; $i < $n; $i++) { $t = microtime(true); dispatch(new NoopJob($pad)); $lat[] = (microtime(true) - $t) * 1000; }
        }
        $t1 = microtime(true);
        $this->line(json_encode(['start' => $t0, 'end' => $t1, 'n' => $n, 'lat' => $lat]));
        return 0;
    }
}
