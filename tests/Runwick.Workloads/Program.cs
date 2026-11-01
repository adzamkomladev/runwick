using Runwick.Workloads;

// Workload runner (07 §14, 15 §11). Modes:
//   run <workload> [--out dir] [--reps n] [--warmup-min m] [--measure-min m] ...   orchestrates one workload
//   worker --conn cs --app a --concurrency n --handler-ms ms --out prefix [--type-ceiling c] [--divisor d]
//          (G-2: --g2 [--queues q:w,..] [--lane-mode m] [--handlers h,..] [--spin] and the policy arguments)
//   produce --conn cs --app a --rate r --out prefix [--payload bytes] [--seed s] [--producer-calls n]
//   produce2 --conn cs --app a --g2 --mode poisson|topup --job kind ...             G-2 producer / lane feeder
//   scheduler | relay | stage --conn cs --app a --g2 ...                           G-2 role processes
//   report --results dir --workloads dir --out file [--gate g2]                      G-1 (or G-2) report case table
// Workloads: G-1 P-01, P-02, P-08a, P-08b, P-08c, G1-M1, G1-M2, G1-M3; G-2 P-04, P-05, P-06r, P-06s, P-09
// (parameters: docs/reports/g2-workloads-design.md).
if (args.Length == 0)
{
    Console.Error.WriteLine("usage: Runwick.Workloads run|worker|produce|produce2|scheduler|relay|stage|report ...");
    return 2;
}

var a = new Args(args[1..]);
return args[0] switch
{
    "worker" => await WorkerMode.RunAsync(a),
    "produce" => await ProducerMode.RunAsync(a),
    "produce2" => await G2ProducerMode.RunAsync(a),
    "scheduler" => await G2Roles.SchedulerAsync(a),
    "relay" => await G2Roles.RelayAsync(a),
    "stage" => await G2Roles.StageAsync(a),
    "run" when a.Positional.Count > 0 && G2Orchestrator.Names.Contains(a.Positional[0]) => await G2Orchestrator.RunAsync(a),
    "run" => await Orchestrator.RunAsync(a),
    "report" => await ReportMode.RunAsync(a),
    _ => 2,
};
