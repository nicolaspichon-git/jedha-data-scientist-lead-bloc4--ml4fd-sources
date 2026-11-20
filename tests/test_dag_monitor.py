"""dag_monitor's declarations, checked against each other.

Covers: EC33, EC39, EC45
Lot: L4
Competency: C5, C6

WHY THIS FILE EXISTS, AND WHY IT EXISTS LATE. dag_ingest, dag_train, dag_daily
and dag_purge each carried a source-level test; dag_monitor -- the DAG of the
lot that closes the loop -- carried none. On 2026-11-20 its branch returned
"monitor_model.ask_for_retraining" while the task was plainly
"ask_for_retraining", and nothing said so until Airflow refused the run with
"branch_task_ids must contain only valid task_ids". The refusal landed at the
worst possible moment: after the drift had been injected, measured, and the
alert raised. A @dag function is not a TaskGroup and adds no prefix.

A STRING THAT MUST EQUAL A TASK_ID IS A CROSS-DECLARATION, and this project
checks those. Two kinds live in this DAG: what @task.branch returns, and what
an xcom_pull template names.

These tests read the source instead of importing it, because airflow lives in
its own image and no test in this suite imports it.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "airflow" / "dags" / "dag_monitor.py"
BODY = SOURCE.read_text(encoding="utf-8")
TREE = ast.parse(BODY)


def _literals() -> dict:
    """Module-level NAME = "literal" bindings, so a task_id given as a constant
    resolves to the same string the branch returns."""
    out = {}
    for node in TREE.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and isinstance(
                    node.value, ast.Constant) and isinstance(
                        node.value.value, str):
                out[target.id] = node.value.value
    return out


LITERALS = _literals()


def _resolve(node):
    """A string from a literal or from a module-level constant, else None."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return LITERALS.get(node.id)
    return None


def _declared_task_ids() -> set:
    """Every task_id this DAG declares, by either route Airflow offers: the
    keyword on an operator, and the name of a @task-decorated function."""
    found = set()
    for node in ast.walk(TREE):
        if isinstance(node, ast.Call):
            for keyword in node.keywords:
                if keyword.arg == "task_id":
                    name = _resolve(keyword.value)
                    if name:
                        found.add(name)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for decorator in node.decorator_list:
                # @task / @task.branch -> the function name is the task_id
                call = decorator.func if isinstance(
                    decorator, ast.Call) else decorator
                root = call
                while isinstance(root, ast.Attribute):
                    root = root.value
                if isinstance(root, ast.Name) and root.id == "task":
                    explicit = None
                    if isinstance(decorator, ast.Call):
                        for keyword in decorator.keywords:
                            if keyword.arg == "task_id":
                                explicit = _resolve(keyword.value)
                    found.add(explicit or node.name)
    return found


DECLARED = _declared_task_ids()


def _branch_returns() -> set:
    """Every string the branch can return."""
    for node in ast.walk(TREE):
        if isinstance(node, ast.FunctionDef) and node.name == "decide":
            out = set()
            for inner in ast.walk(node):
                if isinstance(inner, ast.Return) and inner.value is not None:
                    name = _resolve(inner.value)
                    assert name, ast.dump(inner.value)
                    out.add(name)
            return out
    raise AssertionError("decide() not found in %s" % SOURCE.name)


def test_the_tasks_this_dag_declares_are_the_expected_seven():
    """Named here so that adding or losing one is a decision and not a
    surprise. measure and tell are the measurement; decide is the policy; the
    two branch targets are the consequence; record_request is what makes the
    consequence replayable."""
    assert DECLARED == {"champion", "measure", "tell", "decide",
                        "ask_for_retraining", "no_retraining",
                        "record_request"}, DECLARED


def test_the_request_is_recorded_after_the_trigger_and_never_before():
    """THE REPAIR OF 2026-11-20, kept as an invariant. Recording before
    triggering is exactly what lost a request: the state was committed by one
    task, the trigger failed in the next, and every later run then saw a
    standing alert with nothing left to report.

    The default trigger rule is all_success, so the record cannot be written
    unless the TriggerDagRunOperator ran. A rule like all_done would restore
    the defect, quietly -- hence the third assertion."""
    assert "ask_for_retraining >> record_request(report)" in BODY

    wiring = BODY.split("branch >> [")[1].splitlines()[0]
    assert "record_request" not in wiring, (
        "record_request hangs off the branch, so it would run without the "
        "trigger: %s" % wiring)

    assert "trigger_rule" not in BODY, (
        "a trigger rule other than the default would decouple the record "
        "from the deed")


def test_every_branch_target_is_a_declared_task():
    """THE DEFECT OF 2026-11-20. @task.branch picks by returning a task_id as a
    string; nothing before execution compares that string to a real task, and
    Airflow raises only once the branch has already run."""
    unknown = _branch_returns() - DECLARED
    assert not unknown, unknown


def test_the_branch_can_reach_both_targets_and_only_those():
    """A branch with one reachable target is a flag written as a branch."""
    assert _branch_returns() == {"ask_for_retraining", "no_retraining"}


def test_no_branch_target_carries_the_dag_function_as_a_prefix():
    """The exact shape of the defect, kept as its own case: a @dag function is
    not a TaskGroup. If a TaskGroup is ever introduced the prefix becomes
    right, and this test is then the thing that must be revisited -- which is
    the point of failing loudly rather than matching loosely."""
    for name in _branch_returns() | DECLARED:
        assert "." not in name, name


def test_every_xcom_pull_names_a_declared_task():
    """The trigger's conf reads a template: ti.xcom_pull(task_ids='measure').
    A rename would leave the training run with no reason recorded, and nothing
    would fail -- the template would simply render empty."""
    pulled = set(re.findall(r"task_ids=[\'\"](\w+)[\'\"]", BODY))
    assert pulled, "no xcom_pull found: has the conf changed?"
    assert pulled <= DECLARED, pulled - DECLARED


def test_the_trigger_does_not_wait_for_the_training():
    """A monitoring run must not hold a worker for the length of a training,
    and what becomes of the candidate is the gate's business."""
    assert "wait_for_completion=False" in BODY


def test_the_cadence_comes_from_governance_and_never_catches_up():
    """THE CADENCE IS A DEMONSTRATION KNOB, so it is resolved by the switch and
    not repeated here as a literal. The first version of this test required a
    literal, which is what the other five DAGs carry -- and would have made
    flipping monitoring.mode need a second edit in this file, with the suite
    red between the two. See config.monitoring_schedule() for the argument.

    And it never catches up: a past window cannot be re-measured, since the
    transactions it held have since been scored by whichever model was then in
    service."""
    assert "schedule=config.load().monitoring_schedule()" in BODY
    assert not re.search(r'schedule="', BODY), "a literal cadence came back"
    assert "catchup=False" in BODY
