#!/bin/sh
# The most recent log of one task, with its AGE stated up front.
#
# WHY THE AGE. The previous version printed only the path. When a task had not
# run again, it showed an 80-minute-old failure that read exactly like a fresh
# one -- and the path, which does carry the run's timestamp, is easy to skim
# past. A stale log presented as current is worse than no log.
#
# A FILE rather than a string passed to bash -c: PowerShell's argument layer
# eats double quotes on their way to a native command.
#
# Usage: sh tasklog.sh <task_id>   (or set FD_TASK)

task="${1:-$FD_TASK}"
if [ -z "$task" ]; then
    echo "usage: tasklog.sh <task_id>"
    exit 2
fi

log=$(ls -t /opt/airflow/logs/dag_id=*/*/task_id="$task"/*.log 2>/dev/null | head -1)
if [ -z "$log" ]; then
    echo "AUCUN JOURNAL TROUVE pour $task"
    exit 2
fi

age=$(( $(date +%s) - $(date -r "$log" +%s) ))
when=$(date -r "$log" '+%d/%m %H:%M:%S')

if [ "$age" -ge 300 ]; then
    minutes=$(( age / 60 ))
    echo "=============================================================="
    echo " ATTENTION : ce journal date de $when, soit $minutes minutes."
    echo " La tache n'a donc PAS tourne depuis. Si vous venez de lancer"
    echo " une execution, elle n'a pas encore atteint cette tache :"
    printf ' %s\n' 'verifiez avec   .\fd-manager.ps1 progress'
    echo "=============================================================="
    echo
else
    echo "Journal de $when (il y a ${age}s)."
    echo
fi

echo "===== $log"
echo
cat "$log"
