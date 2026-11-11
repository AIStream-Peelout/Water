"""
Queue for the forecast context ablation: one forecaster run per (model, arm, seed).

Reads the arms written by ``prepare_context_ablation.py`` (``design.json``) and launches
``run_training.py`` (hybrid GR4) or ``run_crossformer.py`` for each requested combination, a few
at a time. Every arm of a model gets the same budget, loss, sampler and seed; only the manifest
(the context bank) differs. An arm written as ``<arm>@channels`` runs the Crossformer with the
legacy constant-series context path instead of static conditioning.

The queue is resumable (a run whose evaluation exists is skipped), stops launching after the
first failure, and stops everything if the combined memory of its runs passes the budget. It is
CPU-only and machine-portable: Flow Forecast comes from ``FF_REPO`` and the interpreter from
``PY`` (default: the one running this script).

Example::

    FF_REPO=/path/to/flow-forecast python experiments/catchment_foundation/run_context_ablation.py \
        --design experiments/catchment_foundation/calibration/context_ablation_cout_f0/design.json \
        --tag ctxf0 --workers 3 --seeds 42 --extra none:43
"""
import argparse
import json
import os
import subprocess
import sys
import time
from typing import Dict, List, Optional, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
WATER_ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))

# Pure-physics forcing: gridded precipitation only, no learned multiplier and no station term.
# The station column (p01m) sums cumulative sub-hourly reports and so over-counts rain (about
# twofold in many basins, hourly values up to 300 mm); the station gate would add that phantom
# rain and, being a function of the context, would confound the comparison between arms.
HYBRID_FLAGS = ["--swe", "--anchored", "--no-multiplier", "--lean-tuning",
                "--production-init-fraction", "0.1", "--event-sample-power", "0",
                "--loss", "huber", "--no-wandb"]
CROSSFORMER_FLAGS = ["--loss", "huber", "--threads", "1", "--no-wandb"]


def run_name(tag: str, model: str, arm: str, seed: int) -> str:
    """Returns the run directory name of one job."""
    short = {"hybrid": "hy", "crossformer": "cf"}[model]
    return "%s_%s_%s_s%d" % (tag, short, arm.replace("@", "-"), seed)


def build_command(python: str, model: str, arm: str, seed: int, name: str, manifests: Dict,
                  budget: Dict) -> List[str]:
    """
    Builds the launcher command of one job.

    :param python: Interpreter path.
    :type python: str
    :param model: ``"hybrid"`` or ``"crossformer"``.
    :type model: str
    :param arm: Arm label from the design, optionally suffixed ``@channels``.
    :type arm: str
    :param seed: Training seed.
    :type seed: int
    :param name: Run directory name.
    :type name: str
    :param manifests: Arm label -> manifest path.
    :type manifests: Dict
    :param budget: Per-model ``epochs``, ``samples``, ``batch``, ``lr`` and ``patience``.
    :type budget: Dict
    :return: The argument vector.
    :rtype: List[str]
    """
    base_arm, _, mode = arm.partition("@")
    if base_arm not in manifests:
        raise ValueError("arm %s is not in the design (%s)" % (base_arm, ", ".join(manifests)))
    if mode not in ("", "channels") or (mode and model != "crossformer"):
        raise ValueError("unsupported arm variant: %s for %s" % (arm, model))
    common = ["--name", name, "--manifest", manifests[base_arm], "--seed", str(seed),
              "--epochs", str(budget["epochs"]), "--samples-per-epoch", str(budget["samples"]),
              "--batch-size", str(budget["batch"]), "--lr", str(budget["lr"]),
              "--patience", str(budget["patience"])]
    if model == "hybrid":
        command = [python, os.path.join(HERE, "run_training.py")] + common + HYBRID_FLAGS
        if base_arm != "learned_id":
            command.append("--require-embeddings")
        return command
    command = [python, os.path.join(HERE, "run_crossformer.py")] + common + CROSSFORMER_FLAGS
    command += ["--context-mode", mode or "tokens"]
    if base_arm == "learned_id":
        command.append("--allow-missing-embeddings")
    return command


def is_done(name: str) -> bool:
    """Whether a run already wrote its evaluation."""
    run_dir = os.path.join(HERE, "runs", name)
    return any(os.path.exists(os.path.join(run_dir, "eval_" + split, "basin_band_errors.json"))
               for split in ("ungauged_2023", "gauged_2023"))


def resident_gb(pids: List[int]) -> float:
    """Sums the resident memory of the given processes in GB (CPU-only runs, so RSS is exact)."""
    if not pids:
        return 0.0
    output = subprocess.run(["ps", "-o", "rss=", "-p", ",".join(str(pid) for pid in pids)],
                            capture_output=True, text=True).stdout
    return sum(int(line) for line in output.split() if line.strip().isdigit()) / 1024.0 ** 2


def run_queue(jobs: List[Tuple[str, List[str]]], workers: int, memory_budget_gb: float,
              environment: Dict[str, str]) -> int:
    """
    Runs the jobs with a bounded number of concurrent processes.

    :param jobs: (run name, command) pairs.
    :type jobs: List[Tuple[str, List[str]]]
    :param workers: Concurrent runs.
    :type workers: int
    :param memory_budget_gb: Combined resident-memory ceiling; exceeding it stops everything.
    :type memory_budget_gb: float
    :param environment: Environment of the launched runs.
    :type environment: Dict[str, str]
    :return: 0 on success, 1 if a run failed, 2 if the memory budget was exceeded.
    :rtype: int
    """
    pending = list(jobs)
    running: Dict[str, Tuple[subprocess.Popen, float]] = {}
    failed: Optional[str] = None
    peak = 0.0
    while pending or running:
        while pending and failed is None and len(running) < workers:
            name, command = pending.pop(0)
            run_dir = os.path.join(HERE, "runs", name)
            os.makedirs(run_dir, exist_ok=True)
            log = open(os.path.join(run_dir, "queue.log"), "w")
            process = subprocess.Popen(command, cwd=WATER_ROOT, env=environment, stdout=log,
                                       stderr=subprocess.STDOUT)
            running[name] = (process, time.time())
            print("[start] %s (pid %d)" % (name, process.pid), flush=True)
        time.sleep(15)
        usage = resident_gb([process.pid for process, _ in running.values()])
        peak = max(peak, usage)
        if usage > memory_budget_gb:
            print("[stop] %.1f GB exceeds the %.1f GB budget; terminating the queue"
                  % (usage, memory_budget_gb), flush=True)
            for process, _ in running.values():
                process.terminate()
            return 2
        for name in list(running):
            process, started = running[name]
            code = process.poll()
            if code is None:
                continue
            del running[name]
            minutes = (time.time() - started) / 60.0
            if code == 0 and is_done(name):
                print("[done] %s in %.1f min (queue peak %.1f GB)" % (name, minutes, peak),
                      flush=True)
            else:
                failed = name
                print("[FAIL] %s exited with %d after %.1f min; see runs/%s/queue.log. No "
                      "further runs will start." % (name, code, minutes, name), flush=True)
    print("[queue] finished; peak memory %.1f GB" % peak, flush=True)
    return 1 if failed else 0


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--design", required=True, help="design.json of the prepared arms")
    parser.add_argument("--tag", required=True, help="Prefix of the run names")
    parser.add_argument("--models", nargs="+", default=["hybrid", "crossformer"],
                        choices=("hybrid", "crossformer"))
    parser.add_argument("--arms", nargs="+", default=None,
                        help="Arms to run (default: every arm in the design)")
    parser.add_argument("--seeds", type=int, nargs="+", default=[42])
    parser.add_argument("--extra", action="append", default=[],
                        help="Additional arm:seed job for every model, e.g. none:43")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--memory-budget-gb", type=float, default=18.0)
    parser.add_argument("--hybrid-budget", default="epochs=10,samples=8192,batch=128,lr=1e-3,"
                                                   "patience=3")
    parser.add_argument("--crossformer-budget", default="epochs=8,samples=16384,batch=8,"
                                                        "lr=1e-3,patience=4")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    with open(args.design) as file:
        manifests = json.load(file)["manifests"]
    budgets = {"hybrid": dict(item.split("=") for item in args.hybrid_budget.split(",")),
               "crossformer": dict(item.split("=") for item in args.crossformer_budget.split(","))}
    python = os.environ.get("PY", sys.executable)
    combos = [(arm, seed) for seed in args.seeds for arm in (args.arms or list(manifests))]
    combos += [(item.rpartition(":")[0], int(item.rpartition(":")[2])) for item in args.extra]
    jobs = []
    for model in args.models:
        for arm, seed in combos:
            if arm.endswith("@channels") and model != "crossformer":
                continue
            name = run_name(args.tag, model, arm, seed)
            command = build_command(python, model, arm, seed, name, manifests, budgets[model])
            if is_done(name):
                print("[skip] %s already evaluated" % name)
            else:
                jobs.append((name, command))
    if args.dry_run:
        for name, command in jobs:
            print(name + ":\n   " + " ".join(command))
        return
    environment = dict(os.environ)
    if os.environ.get("FF_REPO"):
        environment["PYTHONPATH"] = os.pathsep.join(
            [os.environ["FF_REPO"]] + ([environment["PYTHONPATH"]]
                                       if environment.get("PYTHONPATH") else []))
    # One or two threads per run: both forecasters are bound by per-step overhead, not by BLAS.
    for variable in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
        environment.setdefault(variable, "2")
    print("[queue] %d runs, %d at a time, memory budget %.0f GB"
          % (len(jobs), args.workers, args.memory_budget_gb), flush=True)
    sys.exit(run_queue(jobs, args.workers, args.memory_budget_gb, environment))


if __name__ == "__main__":
    main()
