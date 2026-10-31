using Runwick.Workloads;

// Workload runner (07 §14). Modes:
//   run <workload> [--out dir] [--reps n] [--warmup-min m] [--measure-min m] ...   orchestrates one G-1 workload
//   worker --conn cs --app a --concurrency n --handler-ms ms --out prefix [--type-ceiling c] [--divisor d]
//   produce --conn cs --app a --rate r --out prefix [--payload bytes] [--seed s] [--producer-calls n]
//   report --results dir --workloads dir --out file [--gate g2]                      G-1 (or G-2) report case table
// Workloads: P-01, P-02, P-08a, P-08b, P-08c, G1-M1, G1-M2, G1-M3.
if (args.Length == 0)
{
    Console.Error.WriteLine("usage: Runwick.Workloads run|worker|produce|report ...");
    return 2;
}

var a = new Args(args[1..]);
return args[0] switch
{
    "worker" => await WorkerMode.RunAsync(a),
    "produce" => await ProducerMode.RunAsync(a),
    "run" => await Orchestrator.RunAsync(a),
    "report" => await ReportMode.RunAsync(a),
    _ => 2,
};
