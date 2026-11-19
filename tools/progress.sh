#!/bin/sh
# How far each DAG run got, task by task.
#
# A FILE rather than a string passed to bash -c: PowerShell's argument layer
# eats double quotes on their way to a native command, and this script needs
# them. That lesson cost two rounds on the replay conf; it is not relearned here.
#
# Reads the log tree rather than asking the Airflow CLI: its table flags and
# column order shift between versions, and a wrong bet there has cost us two
# rounds already.

for dag in /opt/airflow/logs/dag_id=*; do
    [ -d "$dag" ] || continue
    echo
    echo "=== $(basename "$dag" | sed 's/dag_id=//')"

    for task in "$dag"/*/task_id=*; do
        [ -d "$task" ] || continue
        log=$(ls -t "$task"/*.log 2>/dev/null | head -1)
        [ -n "$log" ] || continue

        name=$(basename "$task" | sed 's/task_id=//')
        when=$(date -r "$log" '+%d/%m %H:%M:%S')
        # Airflow writes this line last, so its absence means still running --
        # or killed before it could write one.
        state=$(grep -o 'Marking task as [A-Z]*' "$log" | tail -1 |
                sed 's/Marking task as //')
        printf '  %-24s %s  %s\n' "$name" "$when" "${state:-EN COURS ?}"
    done | sort -k2,3 -r
done
echo
