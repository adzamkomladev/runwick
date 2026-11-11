<?php
namespace App\Jobs;

use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Queue\Queueable;

class NoopJob implements ShouldQueue
{
    use Queueable;
    public function __construct(public string $pad) {}
    public function handle(): void {}
}
