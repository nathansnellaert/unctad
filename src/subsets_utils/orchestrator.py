import importlib.util
import json
import os
import sys
import traceback
import multiprocessing
from datetime import datetime
from pathlib import Path
from typing import Callable
from contextvars import ContextVar

# Current task context for IO tracking
_current_task: ContextVar[dict | None] = ContextVar('current_task', default=None)


def track_read(asset: str):
    """Track a read operation. Called by IO functions."""
    task = _current_task.get()
    if task is not None:
        task["reads"].append(asset)


def track_write(asset: str, rows: int = None):
    """Track a write operation. Called by IO functions."""
    task = _current_task.get()
    if task is not None:
        task["writes"].append(asset)
        if rows:
            task["rows_written"] = task.get("rows_written", 0) + rows


def _get_task_id(fn: Callable) -> str:
    """Get unique task ID from function (module.name)."""
    module = fn.__module__
    # Strip 'src.' prefix if present for cleaner IDs
    if module.startswith('src.'):
        module = module[4:]
    return f"{module}.{fn.__name__}"


class DAG:
    def __init__(self, nodes: dict[Callable, list[Callable]]):
        self.nodes = nodes
        self.state = {}
        self._fn_to_id = {}  # Map function to its ID
        self._id_to_fn = {}  # Reverse lookup
        self._dependents = {}  # Reverse graph: node -> list of nodes that depend on it

        # Initialize state and reverse lookup for each node
        for fn in nodes:
            task_id = _get_task_id(fn)
            self._fn_to_id[fn] = task_id
            self._id_to_fn[task_id] = fn
            self._dependents[fn] = []
            self.state[task_id] = {
                "id": task_id,
                "status": "pending",
                "reads": [],
                "writes": [],
            }

        # Build reverse dependency graph
        for fn, deps in nodes.items():
            for dep in deps:
                self._dependents[dep].append(fn)

    def _topological_order(self) -> list[Callable]:
        """Return functions in dependency order.

        Uses DFS-style ordering to run dependent nodes immediately after their
        dependencies complete. This ensures download→transform pairs run together,
        freeing disk space before the next download.
        """
        in_degree = {fn: len(deps) for fn, deps in self.nodes.items()}
        ready = [fn for fn, deg in in_degree.items() if deg == 0]
        order = []

        while ready:
            fn = ready.pop(0)
            order.append(fn)

            for other_fn, deps in self.nodes.items():
                if fn in deps:
                    in_degree[other_fn] -= 1
                    if in_degree[other_fn] == 0:
                        # Insert at FRONT to run dependent immediately (DFS-style)
                        ready.insert(0, other_fn)

        if len(order) != len(self.nodes):
            raise ValueError("Cycle detected in DAG")

        return order

    def _cleanup_upstream_cache(self, fn: Callable):
        """Clean up cached files from upstream nodes if all their dependents are done.

        After a node completes, check each of its dependencies. If ALL nodes that
        depend on that dependency have completed, the dependency's outputs are no
        longer needed and can be deleted from cache.
        """
        from .config import is_cloud, cache_path, raw_key

        if not is_cloud():
            return  # Only clean up in cloud mode

        for dep in self.nodes[fn]:
            dep_id = self._fn_to_id[dep]
            dependents = self._dependents[dep]

            # Check if ALL dependents of this dependency have completed
            all_done = all(
                self.state[self._fn_to_id[d]]["status"] in ("done", "failed", "skipped")
                for d in dependents
            )

            if all_done:
                # Delete cached files written by this dependency
                dep_writes = self.state[dep_id].get("writes", [])
                for asset in dep_writes:
                    cache_file = Path(cache_path(raw_key(asset, "parquet")))
                    if cache_file.exists():
                        cache_file.unlink()
                        print(f"[DAG] Cleaned up cache: {asset}")

    def _run_task(self, fn: Callable, isolate: bool = False) -> dict:
        """Run a single task, optionally in subprocess for memory isolation."""
        task_id = self._fn_to_id[fn]
        task_state = self.state[task_id]
        task_state["status"] = "running"
        task_state["started_at"] = datetime.now().isoformat()

        if isolate:
            return self._run_in_subprocess(fn, task_state)
        else:
            return self._run_inline(fn, task_state)

    def _run_inline(self, fn: Callable, task_state: dict) -> dict:
        """Run task in current process."""
        # Set context for IO tracking
        _current_task.set(task_state)

        try:
            fn()
            task_state["status"] = "done"
        except Exception as e:
            task_state["status"] = "failed"
            task_state["error"] = str(e)
            task_state["traceback"] = traceback.format_exc()
        finally:
            task_state["finished_at"] = datetime.now().isoformat()
            started = datetime.fromisoformat(task_state["started_at"])
            finished = datetime.fromisoformat(task_state["finished_at"])
            task_state["duration_s"] = (finished - started).total_seconds()
            _current_task.set(None)

        return task_state

    def _run_in_subprocess(self, fn: Callable, task_state: dict) -> dict:
        """Run task in subprocess for memory isolation."""
        def worker(fn, queue):
            # Re-setup context in subprocess
            local_state = {"reads": [], "writes": [], "rows_written": 0}
            _current_task.set(local_state)

            try:
                fn()
                queue.put({
                    "status": "done",
                    "reads": local_state["reads"],
                    "writes": local_state["writes"],
                    "rows_written": local_state.get("rows_written", 0),
                })
            except Exception as e:
                queue.put({
                    "status": "failed",
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                })

        queue = multiprocessing.Queue()
        proc = multiprocessing.Process(target=worker, args=(fn, queue))
        proc.start()
        proc.join()

        result = queue.get()
        task_state.update(result)
        task_state["finished_at"] = datetime.now().isoformat()
        started = datetime.fromisoformat(task_state["started_at"])
        finished = datetime.fromisoformat(task_state["finished_at"])
        task_state["duration_s"] = (finished - started).total_seconds()

        return task_state

    def run(self, isolate: bool = False, targets: list[str] | None = None):
        """Execute all tasks in dependency order.

        Args:
            isolate: Run each task in subprocess for memory isolation
            targets: Optional list of node names to run (assumes deps already ran)

        Env vars:
            DAG_TARGET: Comma-separated node names to run (overrides targets arg)
            DAG_ON_FAILURE: "crash" (default) or "continue"
        """
        # Env var overrides targets arg
        env_targets = os.environ.get("DAG_TARGET")
        if env_targets:
            targets = [t.strip() for t in env_targets.split(",")]

        order = self._topological_order()

        # Filter to targets if specified
        if targets:
            target_set = set(targets)
            order = [fn for fn in order if self._fn_to_id[fn].split(".")[-2] in target_set]
            if not order:
                # Try matching full ID or function name
                order = [fn for fn in self._topological_order()
                         if self._fn_to_id[fn] in target_set or fn.__name__ in target_set]
            if not order:
                print(f"[DAG] No nodes matched targets: {targets}")
                print(f"[DAG] Available: {[self._fn_to_id[fn] for fn in self.nodes]}")
                return self

        for fn in order:
            task_id = self._fn_to_id[fn]
            deps = self.nodes[fn]

            # Check deps succeeded
            for dep in deps:
                dep_id = self._fn_to_id[dep]
                if self.state[dep_id]["status"] != "done":
                    self.state[task_id]["status"] = "skipped"
                    self.state[task_id]["error"] = f"Dependency {dep_id} failed"
                    continue

            print(f"[DAG] Running {task_id}...")
            result = self._run_task(fn, isolate=isolate)
            self.save_state()  # Implicit checkpoint after each node
            self._cleanup_upstream_cache(fn)  # Free disk space from completed upstream nodes

            if result["status"] == "done":
                print(f"[DAG] {task_id} done ({result['duration_s']:.1f}s)")
            else:
                print(f"[DAG] {task_id} failed: {result.get('error', 'unknown')}")
                if os.environ.get("DAG_ON_FAILURE", "crash") == "crash":
                    break

        return self

    def to_json(self) -> dict:
        """Export DAG structure and execution state."""
        return {
            "nodes": list(self.state.values()),
            "edges": [
                {"from": self._fn_to_id[dep], "to": self._fn_to_id[fn]}
                for fn, deps in self.nodes.items()
                for dep in deps
            ],
            "status": self._overall_status(),
            "total_duration_s": sum(
                n.get("duration_s", 0) for n in self.state.values()
            ),
        }

    def _overall_status(self) -> str:
        statuses = [n["status"] for n in self.state.values()]
        if "failed" in statuses:
            return "failed"
        if "running" in statuses:
            return "running"
        if all(s == "done" for s in statuses):
            return "done"
        return "pending"

    def save_state(self):
        """Save execution state to LOG_DIR. Called after each node, can also be called explicitly."""
        log_dir = os.environ.get('LOG_DIR')
        if not log_dir:
            return  # No LOG_DIR = local dev without runner, skip
        path = Path(log_dir) / "dag.json"
        path.write_text(json.dumps(self.to_json(), indent=2))


def load_nodes(nodes_dir: Path | str | None = None) -> DAG:
    if nodes_dir is None:
        nodes_dir = Path.cwd() / "src" / "nodes"
    elif isinstance(nodes_dir, str):
        nodes_dir = Path(nodes_dir)

    print(f"Loading nodes from: {nodes_dir}")

    all_nodes: dict[Callable, list[Callable]] = {}

    if not nodes_dir.exists():
        print(f"Warning: nodes directory not found: {nodes_dir}")
        return DAG(all_nodes)

    node_files = sorted(nodes_dir.glob("*.py"))

    for node_file in node_files:
        if node_file.name.startswith("_"):
            continue

        module_name = f"nodes.{node_file.stem}"

        try:
            # Load module dynamically
            spec = importlib.util.spec_from_file_location(module_name, node_file)
            if spec is None or spec.loader is None:
                print(f"Warning: Could not load spec for {node_file}")
                continue

            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)

            # Extract NODES dict if present
            if hasattr(module, "NODES"):
                nodes_dict = getattr(module, "NODES")
                if isinstance(nodes_dict, dict):
                    # Convert {download: transform} to DAG format
                    for download_fn, transform_fn in nodes_dict.items():
                        all_nodes[download_fn] = []
                        all_nodes[transform_fn] = [download_fn]

        except Exception as e:
            print(f"Error loading {node_file.name}: {e}")
            raise

    print(f"Loaded {len(all_nodes) // 2} nodes")
    return DAG(all_nodes)
