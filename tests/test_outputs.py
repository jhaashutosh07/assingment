"""
Functional tests for the data pipeline orchestrator.
Tests verify correct output after the agent has fixed bugs in the codebase.
"""

import json
import os
import subprocess
import sys

import pytest

sys.path.insert(0, "/app")
from dag import DAG


# System Python has pyyaml installed; uvx venv does not.
SYS_PYTHON = "/usr/local/bin/python3"


@pytest.fixture(scope="session", autouse=True)
def run_pipeline():
    """Run the pipeline once before all tests."""
    result = subprocess.run(
        [SYS_PYTHON, "/app/orchestrator.py"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result


# ---------------------------------------------------------------------------
# Pipeline output tests
# ---------------------------------------------------------------------------


class TestPipelineOutput:
    def test_output_file_exists(self):
        """The pipeline must generate the report at the interpolated path."""
        assert os.path.exists(
            "/app/output/sales_report.json"
        ), "Output report not found at /app/output/sales_report.json"

    def test_output_is_valid_json(self):
        """The output file must be parseable JSON."""
        with open("/app/output/sales_report.json") as f:
            data = json.load(f)
        assert isinstance(data, dict), "Top-level JSON should be a dict"

    def test_output_has_correct_record_count(self):
        """Merged output should have 10 records (5 per branch x 2 branches)."""
        with open("/app/output/sales_report.json") as f:
            data = json.load(f)
        assert "records" in data, "Output missing 'records' key"
        assert len(data["records"]) == 10, (
            f"Expected 10 merged records, got {len(data['records'])}"
        )


# ---------------------------------------------------------------------------
# DAG / dependency-resolution tests
# ---------------------------------------------------------------------------


class TestDependencyResolution:
    def test_diamond_dependency_is_not_a_cycle(self):
        """A diamond (A->B->D, A->C->D) must NOT be flagged as cyclic."""
        dag = DAG()
        dag.add_edge("A", "B")
        dag.add_edge("A", "C")
        dag.add_edge("B", "D")
        dag.add_edge("C", "D")
        assert dag.has_cycle() is False, "Diamond dependency false-positive cycle"

    def test_real_cycle_is_detected(self):
        """A genuine cycle (A->B->C->A) must be detected."""
        dag = DAG()
        dag.add_edge("A", "B")
        dag.add_edge("B", "C")
        dag.add_edge("C", "A")
        assert dag.has_cycle() is True, "Real cycle was not detected"

    def test_topological_order_dependencies_first(self):
        """Dependencies must appear before their dependents."""
        dag = DAG()
        dag.add_edge("build", "compile")
        dag.add_edge("test", "build")
        order = dag.topological_sort()
        assert order.index("compile") < order.index("build"), (
            f"compile must come before build, got {order}"
        )
        assert order.index("build") < order.index("test"), (
            f"build must come before test, got {order}"
        )

    def test_topological_order_diamond(self):
        """Diamond dependency: all deps before the merge node."""
        dag = DAG()
        dag.add_edge("merge", "branch_a")
        dag.add_edge("merge", "branch_b")
        dag.add_edge("branch_a", "root")
        dag.add_edge("branch_b", "root")
        order = dag.topological_sort()
        assert order.index("root") < order.index("branch_a"), (
            f"root must come before branch_a, got {order}"
        )
        assert order.index("root") < order.index("branch_b"), (
            f"root must come before branch_b, got {order}"
        )
        assert order.index("branch_a") < order.index("merge"), (
            f"branch_a must come before merge, got {order}"
        )
        assert order.index("branch_b") < order.index("merge"), (
            f"branch_b must come before merge, got {order}"
        )


# ---------------------------------------------------------------------------
# Variable interpolation tests
# ---------------------------------------------------------------------------


class TestVariableInterpolation:
    def test_output_path_was_interpolated(self):
        """${OUTPUT_DIR}/${DATASET}_report.json should resolve correctly."""
        assert os.path.exists("/app/output/sales_report.json"), (
            "Variable interpolation failed: output file path was not resolved"
        )

    def test_multiple_variables_in_string(self):
        """Two ${VAR} references in one string must both be resolved."""
        result = subprocess.run(
            [
                SYS_PYTHON,
                "-c",
                (
                    "import sys; sys.path.insert(0,'/app');"
                    "from config import ConfigLoader;"
                    "c = ConfigLoader.__new__(ConfigLoader);"
                    "c.variables = {'A': 'hello', 'B': 'world'};"
                    "print(c.interpolate('${A}/${B}.txt'))"
                ),
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, f"Interpolation script failed: {result.stderr}"
        assert result.stdout.strip() == "hello/world.txt", (
            f"Expected 'hello/world.txt', got '{result.stdout.strip()}'"
        )


# ---------------------------------------------------------------------------
# Data isolation tests (parallel branches must not corrupt each other)
# ---------------------------------------------------------------------------


class TestDataIsolation:
    def test_domestic_pricing_correct(self):
        """Domestic branch: original amounts multiplied by 1.1."""
        with open("/app/output/sales_report.json") as f:
            data = json.load(f)
        domestic = [r for r in data["records"] if r.get("_source") == 0]
        assert len(domestic) == 5, f"Expected 5 domestic records, got {len(domestic)}"
        first_amount = float(domestic[0]["amount"])
        expected = round(29.99 * 1.1, 2)
        assert first_amount == pytest.approx(expected, abs=0.01), (
            f"Domestic amount should be {expected}, got {first_amount}"
        )

    def test_international_pricing_correct(self):
        """International branch: original amounts multiplied by 1.2."""
        with open("/app/output/sales_report.json") as f:
            data = json.load(f)
        intl = [r for r in data["records"] if r.get("_source") == 1]
        assert len(intl) == 5, f"Expected 5 international records, got {len(intl)}"
        first_amount = float(intl[0]["amount"])
        expected = round(29.99 * 1.2, 2)
        assert first_amount == pytest.approx(expected, abs=0.01), (
            f"International amount should be {expected}, got {first_amount}"
        )

    def test_all_amounts_independent(self):
        """Verify every record amount in both branches is independently correct."""
        originals = [29.99, 49.99, 19.99, 99.99, 75.25]
        with open("/app/output/sales_report.json") as f:
            data = json.load(f)
        domestic = sorted(
            [r for r in data["records"] if r.get("_source") == 0],
            key=lambda r: int(r["id"]),
        )
        intl = sorted(
            [r for r in data["records"] if r.get("_source") == 1],
            key=lambda r: int(r["id"]),
        )
        for i, orig in enumerate(originals):
            d_amt = float(domestic[i]["amount"])
            i_amt = float(intl[i]["amount"])
            assert d_amt == pytest.approx(round(orig * 1.1, 2), abs=0.01), (
                f"Record {i}: domestic {d_amt} != {round(orig * 1.1, 2)}"
            )
            assert i_amt == pytest.approx(round(orig * 1.2, 2), abs=0.01), (
                f"Record {i}: international {i_amt} != {round(orig * 1.2, 2)}"
            )


# ---------------------------------------------------------------------------
# Failure propagation tests
# ---------------------------------------------------------------------------


class TestFailurePropagation:
    def test_transitive_dependency_skip(self):
        """If task A fails, B (depends on A) and C (depends on B) must both be skipped."""
        script = (
            "import sys, json\n"
            "sys.path.insert(0, '/app')\n"
            "from orchestrator import PipelineOrchestrator\n"
            "from dag import DAG\n"
            "\n"
            "orch = PipelineOrchestrator.__new__(PipelineOrchestrator)\n"
            "orch.dag = DAG()\n"
            "orch.data_store = {}\n"
            "orch.results = {}\n"
            "orch.pipeline_config = {\n"
            "    'name': 'fail_test',\n"
            "    'tasks': [\n"
            "        {'name': 'a', 'type': 'fail'},\n"
            "        {'name': 'b', 'type': 'noop', 'depends_on': ['a']},\n"
            "        {'name': 'c', 'type': 'noop', 'depends_on': ['b']},\n"
            "    ]\n"
            "}\n"
            "orch.build_dag()\n"
            "\n"
            "def mock_exec(task):\n"
            "    if task['type'] == 'fail':\n"
            "        raise RuntimeError('forced')\n"
            "\n"
            "orch._execute_task = mock_exec\n"
            "orch._generate_report = lambda: None\n"
            "\n"
            "results = orch.execute()\n"
            "print(json.dumps(results))\n"
        )
        result = subprocess.run(
            [SYS_PYTHON, "-c", script],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, (
            f"Failure propagation test script crashed: {result.stderr}"
        )
        results = json.loads(result.stdout.strip())
        assert results["a"]["status"] == "failed", "Task A should have failed"
        assert results["b"]["status"] == "skipped", (
            f"Task B should be skipped (depends on failed A), got: {results['b']}"
        )
        assert results["c"]["status"] == "skipped", (
            f"Task C should be skipped (transitively depends on failed A), got: {results['c']}"
        )
