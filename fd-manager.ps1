<#
.SYNOPSIS
    Entry point for the project's tasks. PowerShell on Windows.

.DESCRIPTION
    Stands in for a Makefile, which Windows 11 Home does not have. Every task
    stops at the first error: native commands (docker, psql, pytest) raise no
    exception, only $LASTEXITCODE tells, so it is tested at each step.

    Comments are in English, like the rest of the versioned code, and so is
    the built-in help. The messages printed while a task runs stay in French.

.EXAMPLE
    .\fd-manager.ps1 pepper
    .\fd-manager.ps1 up
    .\fd-manager.ps1 init
    .\fd-manager.ps1 check
    .\fd-manager.ps1 replay -Batch 6f1c....
    .\fd-manager.ps1 train -CutOff 2020-11-30

.NOTES
    Covers: EC06
    Lot: L0
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('help', 'setup', 'pepper', 'up', 'down', 'status', 'logs', 'psql',
                 'airflow-user', 'reset', 'init', 'cleanup', 'load', 'train',
                 'ingest', 'observe', 'lock', 'mlflow-reset', 'progress',
                 'alerts', 'health', 'abandoned', 'verdicts', 'report',
                 'check-leak',
                 'realtime',
                 'backfill', 'reference',
                 'measure-hour',
                 'rechoose-threshold',
                 'compare-recipes',
                 'batches',
                 'digests',
                 'purge',
                 'promote',
                 'monitor',
                 'drift',
                 'replay', 'forget', 'dashboard', 'scoring-service',
                 'tasklog', 'dag-test', 'accept', 'test', 'lint', 'traceability',
                 'typology',
                 'check')]
    [string]$Task = 'help',

    [string]$Batch,

    [ValidateSet('postgres', 'airflow-init', 'airflow-scheduler',
                 'airflow-webserver', 'mlflow', 'dashboard', 'producer',
                 'scoring-service')]
    [string]$Service,

    [string]$TaskId = 'preflight',

    [string]$CutOff,

    # N4: train a recipe without the named features. Omitted, the run follows
    # scoring.drop_features in governance.yml, which is where the decision
    # lives. -Drop none asks for the FULL recipe, the only way back to it since
    # a drop list can never add a column.
    #
    # The digest differs for each list, so every recipe stays rebuildable.
    #
    # [string[]] AND NOT [string]: in PowerShell the comma is the ARRAY
    # operator, so -Drop txn_hour,txn_weekday hands over two strings and
    # binding them to a [string] fails before the script runs a single line.
    # Declared as an array, all three spellings arrive and are joined below:
    #   -Drop txn_hour,txn_weekday
    #   -Drop 'txn_hour','txn_weekday'
    #   -Drop 'txn_hour,txn_weekday'
    [string[]]$Drop,

    # Only used by 'observe', to forward --calls / --burst to the script.
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest,

    # logs does not follow by default: a following tail needs Ctrl+C to leave,
    # which is a poor default for a task one runs in order to read an error.
    [switch]$Follow,

    # Only used by 'ingest': break the last reading on purpose, for EC25.
    [switch]$Corrupt,

    # Only used by 'realtime': a DAG that runs 1,440 times a day has to be
    # startable and stoppable without editing anything.
    [switch]$Start,

    [switch]$Stop,

    # Only used by 'drift'. -Factor is a double rather than a switch because
    # the amount multiplier is the measurement: 3 and 30 inject very different
    # drifts, and 30 clips most frauds against validation.max_amount.
    #
    # NOT TESTED WITH -eq 0, because 0 is exactly the value tools/drift.py
    # refuses -- treating it as "not supplied" would swallow the refusal and
    # report the switch instead. PSBoundParameters says what was typed.
    [double]$Factor,

    [switch]$Off,

    # Only used by 'health': the controls are PRINTED by default and sent only
    # when asked. Looking at a transition must not consume the message that
    # would have announced it.
    [switch]$Notify,

    # Used by 'abandoned' and 'digests': the runs (resp. the batches) are LISTED
    # by default and written only when asked. Looking at a journal must not
    # change it.
    [switch]$Apply,

    # Used by 'purge'. -Before moves the retention boundary for every
    # store at once: a tool of demonstration and of trial, since at 90
    # days nothing would be deleted before February 2027. -Force is the
    # explicit confirmation the guard asks for beyond the declared share.
    [string]$Before,
    [switch]$Verify,
    # Used by 'promote'. Without it the gate judges the NEWEST registered
    # version, which is what the scheduler will do; naming one is for
    # re-judging a version by hand, after a policy change for instance.
    [string]$Candidate,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Cmdlets follow Get-Location while .NET APIs follow CurrentDirectory: without
# aligning them, a relative path resolves somewhere other than the current
# directory. We also move into the script's own folder so a task works from
# anywhere.
Set-Location $PSScriptRoot
[Environment]::CurrentDirectory = $PWD.Path

function Import-DotEnv {
    <#  docker compose reads .env by itself, PowerShell does not: the tasks that
        call psql directly need POSTGRES_USER and POSTGRES_DB.  #>
    if (-not (Test-Path '.env')) { return }
    foreach ($line in Get-Content '.env') {
        if ($line -match '^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*)$') {
            Set-Item -Path "env:$($Matches[1])" -Value $Matches[2].Trim()
        }
    }
}
Import-DotEnv

# Docker Desktop prints a "What's next / Gordon" hint whenever a compose
# command exits non-zero. tasklog exits 2 on purpose when a task has no log, so
# the hint would appear in the middle of a perfectly normal answer.
$env:DOCKER_CLI_HINTS = 'false'

$PgUser = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { 'postgres' }
$PgDb   = if ($env:POSTGRES_DB)   { $env:POSTGRES_DB }   else { 'ml4fd-1' }

$Psql = @('compose', 'exec', '-T', 'postgres',
          'psql', '-v', 'ON_ERROR_STOP=1', '-U', $PgUser, '-d', $PgDb)

function Invoke-Step {
    <#  Run a native command and stop if it fails.  #>
    param(
        [Parameter(Mandatory)][string]$Label,
        [Parameter(Mandatory)][scriptblock]$Body
    )
    Write-Host "--- $Label" -ForegroundColor Cyan
    $global:LASTEXITCODE = 0        # a stale value would otherwise skew the test
    & $Body
    if ($LASTEXITCODE -ne 0) {
        Write-Host ""
        Write-Host "ECHEC : $Label (code $LASTEXITCODE)" -ForegroundColor Red
        Write-Host "La chaine s'arrete ici." -ForegroundColor Red
        Write-Host ""
        exit 1
    }
}

function Enable-Dag {
    <#  Make a DAG active, before anything triggers it.

        A trigger on a paused DAG is not an error, and that is the whole
        problem. Airflow creates the run, prints it, and returns 0; the
        scheduler then ignores it, because it skips paused DAGs. The run sits
        in 'queued' for ever. So `load` reported success, the chain carried on,
        and the database stayed empty -- the failure was silent on both sides
        (2026-11-19).

        Nothing in docker-compose.yml sets DAGS_ARE_PAUSED_AT_CREATION, so
        Airflow's default applies and every DAG arrives paused. That is the
        normal state of a first run, not an accident, which is why this is
        handled here rather than reported as a problem to fix by hand.

        Unpausing is idempotent, so it is done unconditionally instead of
        after reading the state: one call to the container instead of two, and
        no parsing of an output whose shape changes between versions.
    #>
    param(
        [Parameter(Mandatory)][string]$DagId,
        [string]$Cadence
    )
    Invoke-Step "activation de $DagId" {
        & docker compose exec -T airflow-scheduler airflow dags unpause $DagId
    }
    if ($Cadence) {
        Write-Host "    $DagId is active from now on: $Cadence" -ForegroundColor DarkGray
    }
}

function Assert-Env {
    <#  Without .env, docker compose substitutes an empty string for every
        variable and postgres starts with no password and no named database.
        Better to refuse.  #>
    if (-not (Test-Path '.env')) {
        Write-Host ""
        Write-Host "Il n'y a pas de fichier .env." -ForegroundColor Red
        Write-Host "Lancez d'abord :  .\fd-manager.ps1 setup" -ForegroundColor Yellow
        Write-Host ""
        exit 1
    }
    $hasPepper = @(Get-Content '.env' |
                   Where-Object { $_ -match '^ML4FD_PEPPER=.+' }).Count -gt 0
    if (-not $hasPepper) {
        Write-Host ""
        Write-Host "ML4FD_PEPPER est vide dans .env." -ForegroundColor Red
        Write-Host "La pseudonymisation serait sans poivre, donc reversible." -ForegroundColor Red
        Write-Host "Lancez :  .\fd-manager.ps1 setup" -ForegroundColor Yellow
        Write-Host ""
        exit 1
    }
    <#  L1: the two service databases are created AT FIRST BOOT, by
        sql/initdb/010_databases.sh, out of these two values. Missing, the
        cluster initialises WITHOUT the airflow and mlflow databases -- and
        the entrypoint never replays that directory, so the only way back is
        to drop the volume. Refusing here costs less than finding out from an
        airflow-init that will not start.  #>
    foreach ($name in @('AIRFLOW_DB_PASSWORD', 'MLFLOW_DB_PASSWORD')) {
        $filled = @(Get-Content '.env' |
                    Where-Object { $_ -match ('^' + $name + '=.+') }).Count -gt 0
        if (-not $filled) {
            Write-Host ""
            Write-Host "$name est vide dans .env." -ForegroundColor Red
            Write-Host "Les bases de service d'Airflow et de MLflow ne seraient pas creees." -ForegroundColor Red
            Write-Host "Lancez :  .\fd-manager.ps1 setup" -ForegroundColor Yellow
            Write-Host ""
            exit 1
        }
    }
}

function Assert-Docker {
    <#  The daemon is not answering: Docker Desktop is closed, or WSL2 is not
        ready.  #>
    $global:LASTEXITCODE = 0
    $null = & docker info 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host ""
        Write-Host "Le demon Docker ne repond pas." -ForegroundColor Red
        Write-Host "Ouvrez Docker Desktop et attendez que l'icone cesse de clignoter," -ForegroundColor Yellow
        Write-Host "puis relancez. Sous Windows 11 Home, Docker Desktop exige WSL2." -ForegroundColor Yellow
        Write-Host ""
        exit 1
    }
}

function Write-ConfFile {
    <#  Writes a JSON conf to disk and returns its container path.

        Shared by replay and train so the two cannot drift. NO double quote may
        cross PowerShell on its way to a native command -- its argument layer
        eats them -- so the quotes are produced by [char]34 and the file is
        written byte for byte, without a BOM. No value may hold a space: bash
        reads the file back by UNQUOTED command substitution, which keeps the
        quotes but splits on whitespace -- and the pairs are joined without a
        space after the comma for the same reason.

        Several keys since N4: a run may carry both a cut-off and an amputated
        feature list. A comma-separated list of column names holds no space, so
        it travels through this route unchanged.  #>
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string[]]$Keys,
        [Parameter(Mandatory)][string[]]$Values
    )
    if ($Keys.Count -ne $Values.Count) {
        Write-Host "Write-ConfFile : $($Keys.Count) cle(s) pour $($Values.Count) valeur(s)" -ForegroundColor Red
        exit 1
    }
    $q = [char]34
    $pairs = @()
    for ($i = 0; $i -lt $Keys.Count; $i++) {
        if ($Values[$i] -match '\s') {
            Write-Host "La valeur ne doit pas contenir d'espace : '$($Values[$i])'" -ForegroundColor Red
            exit 1
        }
        $pairs += ($q + $Keys[$i] + $q + ':' + $q + $Values[$i] + $q)
    }
    $path = Join-Path $PWD ".local_cache/$Name"
    [System.IO.File]::WriteAllText(
        $path,
        '{' + ($pairs -join ',') + '}',
        (New-Object System.Text.UTF8Encoding($false)))
    return '/opt/project/.local_cache/' + $Name
}

function New-Pepper {
    <#  Pepper for the pseudonymisation HMAC: 32 bytes from a cryptographic
        generator. Get-Random will not do -- it is not secure.  #>
    $bytes = [byte[]]::new(32)
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    return (($bytes | ForEach-Object { $_.ToString('x2') }) -join '')
}

switch ($Task) {

    'help' {
        Write-Host "`nAvailable tasks`n" -ForegroundColor Yellow
        @(
            @{ n = 'setup';        d = 'create .env and write an ML4FD_PEPPER into it (run this first)' }
            @{ n = 'pepper';       d = 'print an ML4FD_PEPPER without touching .env' }
            @{ n = 'up';           d = 'start postgres, airflow, mlflow, streamlit' }
            @{ n = 'down';         d = 'stop everything' }
            @{ n = 'reset';        d = 'destroys the volumes and starts over' }
            @{ n = 'status';       d = 'state of the services and of the DAGs, with the logs of what exited' }
            @{ n = 'logs';         d = 'logs: .\fd-manager.ps1 logs -Service airflow-scheduler [-Follow]' }
            @{ n = 'psql';         d = 'interactive psql session on the database' }
            @{ n = 'airflow-user'; d = 'list the Airflow accounts and reset admin / admin' }
            @{ n = 'init';         d = 'apply the DDL and the function (replayable)' }
            @{ n = 'load';         d = 'trigger the loading of the labelled history' }
            @{ n = 'train';        d = 'trigger a training, challenging and promotion: .\fd-manager.ps1 train [-CutOff 2020-11-30] [-Drop none]' }
            @{ n = 'cleanup';      d = 'drop the roles and tables of the old naming (no prefix)' }
            @{ n = 'progress';     d = 'where the runs stand, task by task' }
            @{ n = 'observe';      d = 'observe and measure the payment API, reporting under runtime/observations/' }
            @{ n = 'ingest';       d = 'trigger one real-time cycle: .\fd-manager.ps1 ingest [-Corrupt]' }
            @{ n = 'alerts';       d = 'state of the alerts and of their notification' }
            @{ n = 'health';       d = 'the four pipeline controls (-Notify to send)' }
            @{ n = 'abandoned';    d = 'list the runs left in progress (-Apply to close them)' }
            @{ n = 'report';       d = 'produce the morning report (--date 2026-11-16)' }
            @{ n = 'verdicts';     d = 'write the retained verdicts, outside any scoring' }
            @{ n = 'check-leak';   d = 'require the same score with is_fraud=0 and is_fraud=1' }
            @{ n = 'realtime';     d = 'state of the per-minute DAG: .\fd-manager.ps1 realtime [-Start|-Stop]' }
            @{ n = 'backfill';     d = 'recompute the stored features and reconcile the two modes' }
            @{ n = 'reference';    d = 'copy the measurements of the older run.json files into the database (-- --dry-run)' }
            @{ n = 'measure-hour'; d = 'what txn_hour does to the model on the stream' }
            @{ n = 'rechoose-threshold'; d = 'rechoose the threshold on the stream distribution' }
            @{ n = 'compare-recipes';    d = 'compare two recipes: --against <version>' }
            @{ n = 'lock';         d = 'record the resolved versions in airflow/requirements.lock' }
            @{ n = 'mlflow-reset'; d = 'recreate the MLflow volume (destroys its history)' }
            @{ n = 'batches';      d = 'list the raw-zone batches and their run' }
            @{ n = 'digests';      d = 'batches with no digest (-Apply to record it)' }
            @{ n = 'purge';        d = 'dry-run purge (-Before; -Apply to delete)' }
            @{ n = 'promote';      d = 'judge a model by hand; dag_train now does it on its own (-Candidate <v>; -Apply to move the alias)' }
            @{ n = 'monitor';      d = 'drift, performance and the retraining they ask for; -Apply triggers the run' }
            @{ n = 'drift';        d = 'the injected amount drift: show it, set it (-Factor N), lift it (-Off)' }
            @{ n = 'replay';       d = 'replay a batch: .\fd-manager.ps1 replay -Batch <uuid>' }
            @{ n = 'forget';       d = 'forget a failed batch: forget -Batch <uuid> [-Apply]' }
            @{ n = 'dashboard';    d = 'start the dashboard (http://localhost:8501) [-Stop]' }
            @{ n = 'scoring-service'; d = 'start the scoring service (http://localhost:5001/docs) [-Stop]' }
            @{ n = 'tasklog';      d = 'show the last log: .\fd-manager.ps1 tasklog -TaskId backfill_features' }
            @{ n = 'dag-test';     d = 'run one task live: .\fd-manager.ps1 dag-test -TaskId preflight' }
            @{ n = 'test';         d = 'unit tests, with no database and no Airflow' }
            @{ n = 'lint';         d = 'ruff' }
            @{ n = 'traceability'; d = 'regenerate TRACEABILITY.md' }
            @{ n = 'accept';       d = 'the quantified acceptance criteria of the history loading' }
            @{ n = 'check';        d = 'everything: acceptance, traceability, tests, concordance' }
        ) | ForEach-Object { Write-Host ("  {0,-13} {1}" -f $_.n, $_.d) }

        <#  No dossier codes here. The help is read at the prompt by somebody
            who wants to run a task, not by somebody holding the dossier: a
            line that says what the task does is worth more than a line that
            says which work package asked for it. The codes stay in the source,
            above each task, where the traceability tool looks for them.  #>
        Write-Host ''
    }
    'setup' {
        if (-not (Test-Path '.env')) {
            Copy-Item '.env.example' '.env'
            Write-Host "  .env cree depuis .env.example" -ForegroundColor Green
        }
        else {
            Write-Host "  .env existe deja, conserve" -ForegroundColor Yellow
        }

        $lines = @(Get-Content '.env')
        if ($lines -match '^ML4FD_PEPPER=.+') {
            Write-Host "  ML4FD_PEPPER deja renseigne, laisse intact" -ForegroundColor Yellow
            Write-Host "  (le changer invaliderait tous les client_key deja calcules)"
        }
        else {
            $p = New-Pepper
            $lines = $lines | ForEach-Object {
                if ($_ -match '^ML4FD_PEPPER=') { "ML4FD_PEPPER=$p" } else { $_ }
            }
            # No BOM: otherwise docker compose would read a variable named
            # <BOM>POSTGRES_USER on the first line.
            [System.IO.File]::WriteAllLines(
                (Join-Path $PWD '.env'), $lines,
                (New-Object System.Text.UTF8Encoding($false)))
            Write-Host "  ML4FD_PEPPER genere et inscrit" -ForegroundColor Green
        }

        # THE DASHBOARD'S PASSWORD (S6/R0). Generated like the pepper, and for
        # the same reason it is never versioned. The line is ADDED when absent:
        # every .env made before S6 lacks it, and a replace-only loop would
        # silently leave the dashboard without a password -- unable to log in,
        # which is safe, but for no reason anybody would guess.
        $lines = @(Get-Content '.env')
        if ($lines -match '^FD_DASHBOARD_PASSWORD=.+') {
            Write-Host "  FD_DASHBOARD_PASSWORD deja renseigne, laisse intact" -ForegroundColor Yellow
        }
        else {
            $d = New-Pepper
            if ($lines -match '^FD_DASHBOARD_PASSWORD=') {
                $lines = $lines | ForEach-Object {
                    if ($_ -match '^FD_DASHBOARD_PASSWORD=') { "FD_DASHBOARD_PASSWORD=$d" } else { $_ }
                }
            }
            else {
                $lines += ''
                $lines += '# Password of fd_dashboard, the role the dashboard logs in as (S6).'
                $lines += "FD_DASHBOARD_PASSWORD=$d"
            }
            [System.IO.File]::WriteAllLines(
                (Join-Path $PWD '.env'), $lines,
                (New-Object System.Text.UTF8Encoding($false)))
            Write-Host "  FD_DASHBOARD_PASSWORD genere et inscrit" -ForegroundColor Green
            Write-Host "  (a appliquer a la base :  .\fd-manager.ps1 init)"
        }

        # THE TWO SERVICE DATABASE PASSWORDS (L1). Same treatment as the
        # dashboard's, and for the same reason: a versioned file is not a place
        # for a secret. The line is ADDED when absent -- every .env made before
        # L1 lacks it, and a replace-only loop would leave the value empty,
        # which Assert-Env then refuses at `up` for no reason anybody would
        # guess.
        # THE COMMENT FOLLOWS THE ROLE, since 2026-11-20. The loop wrote
        # "owner of a service database" above every line it added, which was
        # true of the two it had and false of FD_SCORING_SERVICE_PASSWORD -- a secret
        # whose own .env line would have described someone else.
        $secrets = @(
            @{ n = 'AIRFLOW_DB_PASSWORD'; c = 'Password of the owner of a service database (L1).' }
            @{ n = 'MLFLOW_DB_PASSWORD';  c = 'Password of the owner of a service database (L1).' }
            @{ n = 'FD_SCORING_SERVICE_PASSWORD';     c = 'Password of fd_scoring_service, the role the scoring service logs in as (L5).' }
        )
        foreach ($secret in $secrets) {
            $name = $secret.n
            $lines = @(Get-Content '.env')
            if ($lines -match ('^' + $name + '=.+')) {
                Write-Host "  $name deja renseigne, laisse intact" -ForegroundColor Yellow
                continue
            }
            $v = New-Pepper
            if ($lines -match ('^' + $name + '=')) {
                $lines = $lines | ForEach-Object {
                    if ($_ -match ('^' + $name + '=')) { "$name=$v" } else { $_ }
                }
            }
            else {
                $lines += ''
                $lines += ('# ' + $secret.c)
                $lines += "$name=$v"
            }
            [System.IO.File]::WriteAllLines(
                (Join-Path $PWD '.env'), $lines,
                (New-Object System.Text.UTF8Encoding($false)))
            Write-Host "  $name genere et inscrit" -ForegroundColor Green
        }

        Write-Host ""
        Write-Host "Pret. Ouvrez Docker Desktop, puis :  .\fd-manager.ps1 up" -ForegroundColor Cyan
        Write-Host ""
    }

    'pepper' {
        $p = New-Pepper
        Write-Host "`nML4FD_PEPPER=$p`n" -ForegroundColor Green
        Write-Host "A coller dans .env. Ne jamais versionner, ne jamais journaliser."
        Write-Host "Le changer invalide tous les client_key deja calcules.`n" -ForegroundColor Yellow
    }

    'up' {
        Assert-Env
        Assert-Docker
        # The noise comes from BuildKit: with no terminal to redraw in place,
        # its progress frame is re-emitted in full on every refresh. An
        # environment variable rather than a flag: --progress is a GLOBAL flag
        # of "docker compose", placed before the verb, and its availability
        # varies between versions.
        $env:BUILDKIT_PROGRESS = 'plain'
        # --remove-orphans: renaming a service leaves the old container in
        # place, and that one goes on holding the published port. That is how
        # the former "airflow" standalone container, with its random password,
        # survived the move to three services.
        # --build on every up: without it, docker compose starts the image it
        # already has, and a change to requirements.txt is silently ignored --
        # which cost us a round on 'No module named sklearn'. Layers are cached,
        # so an unchanged requirements.txt makes this nearly free.
        Invoke-Step 'docker compose up --build' {
            & docker compose up -d --build --remove-orphans
        }
        Write-Host ""
        & docker compose ps
        Write-Host ""
        Write-Host "Airflow    : http://localhost:8080   (admin / admin)" -ForegroundColor Cyan
        Write-Host "MLflow     : http://localhost:5000" -ForegroundColor Cyan
        Write-Host "Streamlit  : http://localhost:8501   (fd_dashboard, lecture seule)" -ForegroundColor Cyan
        # THE PUBLISHED PORT AND NOT THE CONTAINER'S. The producer listens on
        # 4000 and compose publishes 8801 to the loopback; the scheduler reaches
        # it on producer:4000 inside the network, a browser on 8801 outside.
        # Printing both is the point: confusing the two costs a quarter of an hour.
        Write-Host "Producer   : http://localhost:8801/current-transactions" -ForegroundColor Cyan
        Write-Host "             (producer:4000 depuis les conteneurs)" -ForegroundColor DarkGray
        if (-not $env:FD_DASHBOARD_PASSWORD) {
            Write-Host "  FD_DASHBOARD_PASSWORD est vide : le tableau de bord ne pourra pas se connecter." -ForegroundColor Yellow
            Write-Host "  .\fd-manager.ps1 setup   puis   .\fd-manager.ps1 init" -ForegroundColor Yellow
        }
        Write-Host "PostgreSQL : localhost:5432" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Pour consulter la base :  .\fd-manager.ps1 psql"
        Write-Host ""
        Write-Host "Un conteneur << Started >> peut avoir echoue ensuite."
        Write-Host "Verifiez avec :  .\fd-manager.ps1 status"
        Write-Host ""
    }
    'down' {
        Assert-Docker
        Invoke-Step 'docker compose down' {
            & docker compose down --remove-orphans
        }
    }

    'reset' {
        Assert-Docker
        Write-Host ""
        Write-Host "Cette tache SUPPRIME les volumes : donnees chargees, tables" -ForegroundColor Red
        Write-Host "d'Airflow et entrepot MLflow seront perdus." -ForegroundColor Red
        Write-Host ""
        if ((Read-Host "Taper 'oui' pour confirmer") -ne 'oui') {
            Write-Host "Annule." -ForegroundColor Yellow
            return
        }
        Invoke-Step 'docker compose down -v' {
            & docker compose down -v --remove-orphans
        }
        Write-Host ""
        Write-Host "Volumes supprimes. Repartir avec :" -ForegroundColor Cyan
        Write-Host "  .\fd-manager.ps1 up"
        Write-Host "  .\fd-manager.ps1 init"
        Write-Host ""
    }

    'status' {
        Assert-Docker
        Write-Host ""
        & docker compose ps -a
        Write-Host ""
        # "ps -q" returns an identifier only for a RUNNING container, so empty
        # output means the service has exited. Safer than --filter
        # status=running, whose support varies.
        # airflow-init is absent from this list: it migrates the schema and
        # then exits. Seeing it as "Exited (0)" is the expected behaviour.
        $expected = @('postgres', 'airflow-scheduler', 'airflow-webserver',
                      'mlflow', 'producer', 'dashboard', 'scoring-service')
        $up = 0
        foreach ($svc in $expected) {
            $id = @(& docker compose ps -q $svc) -join ''
            if ($id) {
                $up++
            }
            else {
                Write-Host "  $svc ne tourne pas. Ses dernieres lignes :" -ForegroundColor Red
                & docker compose logs --tail 15 $svc
                Write-Host ""
            }
        }
        if ($up -eq $expected.Count) {
            Write-Host "Les $($expected.Count) services tournent." -ForegroundColor Green
            Write-Host "(airflow-init apparait << Exited (0) >> : travail fini.)" -ForegroundColor DarkGray
            Write-Host ""
        }

        <#  THE PAUSE STATE, which no other task showed and which is the one
            thing that explains a chain reporting success over work that never
            began. A trigger on a paused DAG returns 0, so the run is created
            and the scheduler never picks it up: `load` looked like it had
            worked and the database stayed empty (2026-11-19).

            Read through the Airflow CLI rather than by querying the dag table
            of the metadata database. That database belongs to fd_airflow, and
            L1 exists precisely so that the business role cannot reach it --
            reading it from here would spend the separation it set up.

            The listing is matched line by line against the DAGs this project
            declares, so a DAG that fails to import is reported as absent
            instead of silently missing from the output. The trailing \s in the
            pattern matters: without it dag_ingest would match
            dag_ingest_verdicts as well.  #>
        Write-Host "DAGs" -ForegroundColor Cyan
        $declared = @(
            @{ id = 'dag_load_history';    note = 'on trigger only, no cadence' }
            @{ id = 'dag_train';           note = 'Monday 03:00' }
            @{ id = 'dag_ingest';          note = 'every minute' }
            @{ id = 'dag_ingest_verdicts'; note = 'governance verdicts.schedule' }
            @{ id = 'dag_monitor';         note = 'governance monitoring.schedule' }
            @{ id = 'dag_daily';           note = 'daily 06:00, catches up missed days' }
            @{ id = 'dag_purge';           note = 'Sunday 04:00, the only one that deletes' }
        )
        $listing = @(& docker compose exec -T airflow-scheduler `
                        airflow dags list -o plain 2>&1)
        if ($listing.Count -eq 0) {
            Write-Host "  listing unavailable: the scheduler did not answer." -ForegroundColor Red
        }
        else {
            $paused = 0
            foreach ($d in $declared) {
                $line = $listing |
                        Where-Object { $_ -match ('^\s*' + $d.id + '\s') } |
                        Select-Object -First 1
                if (-not $line) {
                    $state = 'not loaded'; $colour = 'Red'
                }
                elseif ($line -match '\bTrue\b') {
                    $state = 'paused'; $colour = 'Yellow'; $paused++
                }
                elseif ($line -match '\bFalse\b') {
                    $state = 'active'; $colour = 'Green'
                }
                else {
                    $state = 'unknown'; $colour = 'DarkGray'
                }
                Write-Host ("  {0,-20} {1,-11} {2}" -f $d.id, $state, $d.note) `
                    -ForegroundColor $colour
            }
            Write-Host ""
            if ($paused -gt 0) {
                Write-Host "A paused DAG accepts a trigger and never runs it: the run stays" -ForegroundColor DarkGray
                Write-Host "'queued' with no start_date. load, train and replay now make their" -ForegroundColor DarkGray
                Write-Host "DAG active first; the others are started by hand:" -ForegroundColor DarkGray
                Write-Host "  docker compose exec -T airflow-scheduler airflow dags unpause <dag_id>" -ForegroundColor Cyan
                Write-Host "  .\fd-manager.ps1 realtime -Start        # dag_ingest" -ForegroundColor Cyan
                Write-Host ""
            }
        }
    }

    'logs' {
        Assert-Docker
        $tail = if ($Service) { 200 } else { 60 }
        if ($Follow) {
            Write-Host "Suivi en continu. Ctrl+C pour sortir." -ForegroundColor DarkGray
            if ($Service) { & docker compose logs --tail $tail --follow $Service }
            else          { & docker compose logs --tail $tail --follow }
        }
        else {
            if ($Service) { & docker compose logs --tail $tail $Service }
            else          { & docker compose logs --tail $tail }
        }
    }

    'airflow-user' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "Utilisateurs connus d'Airflow :" -ForegroundColor Cyan
        & docker compose exec -T airflow-scheduler airflow users list
        Write-Host ""
        # Delete then recreate: safer than reset-password, whose availability
        # varies between Airflow versions. The command is assembled by
        # concatenation rather than with continuations: mixing PowerShell's
        # backtick and bash's backslash in one call only works by accident.
        $cmd = 'airflow users delete --username admin || true; ' +
               'airflow users create --username admin --password admin ' +
               '--firstname Nicolas --lastname Pichon --role Admin ' +
               '--email admin@example.com'
        Invoke-Step 'remise a plat de admin' {
            & docker compose exec -T airflow-scheduler bash -c $cmd
        }
        Write-Host ""
        Write-Host "admin / admin   sur http://localhost:8080" -ForegroundColor Green
        Write-Host ""
    }

    'cleanup' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "Nettoyage des objets laisses par l'ancien nommage." -ForegroundColor Cyan
        Write-Host "Le script refuse de supprimer une table qui contient des lignes."
        Write-Host ""
        # Not -v ON_ERROR_STOP=1 here: the script reports with RAISE WARNING on
        # a table it declines to drop, and that is an outcome, not a failure.
        & docker compose exec -T postgres `
            psql -U $PgUser -d $PgDb -f /work/sql/maintenance/010_drop_legacy_objects.sql
        Write-Host ""
    }

    'train' {
        Assert-Env
        Assert-Docker

        $keys = @()
        $values = @()
        $label = 'declenchement de S2'

        if ($CutOff) {
            # A bare date is normalised to an explicit instant rather than left
            # with an implicit zone: a cut-off off by a few hours would silently
            # change which verdicts are considered known.
            if ($CutOff -match '^\d{4}-\d{2}-\d{2}$') {
                $instant = $CutOff + 'T00:00:00Z'
                Write-Host "Coupure '$CutOff' normalisee en $instant" -ForegroundColor DarkGray
            }
            elseif ($CutOff -match '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$') {
                $instant = $CutOff
            }
            else {
                Write-Host ""
                Write-Host "Coupure illisible : $CutOff" -ForegroundColor Red
                Write-Host "Attendu : 2020-11-30  ou  2020-11-30T00:00:00Z" -ForegroundColor Yellow
                Write-Host ""
                exit 1
            }
            $keys += 'cut_off'
            $values += $instant
            $label = "$label, coupure $instant"
        }
        else {
            Write-Host "Sans -CutOff : la coupure vient de data_interval_end," -ForegroundColor DarkGray
            Write-Host "ramenee a labels.horizon si elle la depasse." -ForegroundColor DarkGray
        }

        if ($Drop) {
            # Joined here, once. Interpolating the array itself into a message
            # would print "txn_hour txn_weekday" -- space-separated, which is
            # exactly the shape Write-ConfFile refuses, and the message would
            # disagree with what is sent.
            $dropped = ($Drop -join ',')

            # Validated here only for shape. WHICH names are acceptable is the
            # DAG's business -- datasets.without() holds the recipe and refuses
            # an unknown name, so a typo must not be silently dropped on either
            # side.
            if ($dropped -notmatch '^[a-z_]+(,[a-z_]+)*$') {
                Write-Host ""
                Write-Host "Liste de features illisible : $dropped" -ForegroundColor Red
                Write-Host "Attendu : des noms de colonnes separes par des virgules," -ForegroundColor Yellow
                Write-Host "sans espace. Par exemple : txn_hour,txn_weekday" -ForegroundColor Yellow
                Write-Host ""
                exit 1
            }
            Write-Host ""
            if ($dropped -eq 'none') {
                Write-Host "Recette COMPLETE demandee : les 7 features, dont" -ForegroundColor Yellow
                Write-Host "txn_hour et txn_weekday que governance.yml retire." -ForegroundColor Yellow
                Write-Host "Sur le flux ces deux colonnes portent l'heure de NOTRE" -ForegroundColor DarkGray
                Write-Host "appel et non celle du paiement : -18 fraudes sur 205," -ForegroundColor DarkGray
                Write-Host "mesure du 16 novembre 2026." -ForegroundColor DarkGray
            }
            else {
                Write-Host "Recette AMPUTEE de : $dropped" -ForegroundColor Yellow
                Write-Host "L'empreinte de recette sera differente, donc la recette en" -ForegroundColor DarkGray
                Write-Host "service reste reconstructible et le modele enregistre intact." -ForegroundColor DarkGray
            }
            Write-Host ""
            $keys += 'drop_features'
            $values += $dropped
            $label = "$label, sans $dropped"
        }

        # THE RUN CAN CHANGE WHAT IS SERVED, since 2026-11-20. dag_train ends
        # on challenge_and_promote, so a training is no longer a read-only act:
        # a candidate that passes the gate takes the champion alias and the
        # scoring path reads it at the next call. An operator typing this
        # command used to get a registered version and nothing more, so the
        # change is announced rather than discovered.
        Write-Host ""
        Write-Host "Ce passage peut deplacer le champion." -ForegroundColor Yellow
        Write-Host "dag_train finit sur challenge_and_promote : un candidat qui" -ForegroundColor DarkGray
        Write-Host "passe le gate prend l'alias, et le service le lit au scoring" -ForegroundColor DarkGray
        Write-Host "suivant. Refus comme promotion sont journalises dans" -ForegroundColor DarkGray
        Write-Host "fd_promotions. Pour juger sans rien deplacer : promote." -ForegroundColor DarkGray
        Write-Host ""

        # Unlike dag_load_history, this one carries a cadence: making it
        # active also lets it run on its own every Monday. Said rather than
        # done quietly -- a command that triggers one training should not
        # silently sign up for a weekly one.
        Enable-Dag 'dag_train' 'Monday, 03:00'

        if ($keys.Count -gt 0) {
            $confPath = Write-ConfFile 'train-conf.json' $keys $values
            $script = 'airflow dags trigger dag_train --conf $(cat ' +
                      $confPath + ')'
            Invoke-Step $label {
                & docker compose exec -T airflow-scheduler bash -c $script
            }
        }
        else {
            Invoke-Step $label {
                & docker compose exec -T airflow-scheduler `
                    airflow dags trigger dag_train
            }
        }

        Write-Host ""
        Write-Host "Suivre :  .\fd-manager.ps1 tasklog -TaskId build_dataset" -ForegroundColor Cyan
        Write-Host ""
    }

    'mlflow-reset' {
        Assert-Docker
        Write-Host ""
        Write-Host "Recreation du volume MLflow." -ForegroundColor Cyan
        Write-Host ""
        Write-Host "MLflow fige l'emplacement des artefacts A LA CREATION de" -ForegroundColor DarkGray
        Write-Host "l'experience. Une experience nee avec une racine locale y" -ForegroundColor DarkGray
        Write-Host "renvoie les clients pour toujours : il faut la recreer." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "Ce qui est DETRUIT  : l'historique MLflow (runs, metriques)." -ForegroundColor Yellow
        Write-Host "Ce qui est CONSERVE : datasets/, les Parquet, model.joblib," -ForegroundColor Green
        Write-Host "                      run.json, requirements.lock, et la base" -ForegroundColor Green
        Write-Host "                      PostgreSQL. La chaine de tracabilite de" -ForegroundColor Green
        Write-Host "                      M8 vit sur le disque, pas dans MLflow." -ForegroundColor Green
        Write-Host ""
        $answer = Read-Host "Taper OUI pour continuer"
        if ($answer -ne 'OUI') {
            Write-Host "Abandonne." -ForegroundColor Yellow
            exit 0
        }

        Invoke-Step 'arret du service mlflow' {
            & docker compose rm -sf mlflow
        }
        # The volume is NOT guessed from the folder name: docker names it
        # <project>_mlruns, and the project can come from a name: key in the
        # compose file or from COMPOSE_PROJECT_NAME. Here the containers are
        # aia-ml4fd-*, which is neither folder. So it is looked up.
        $volumes = @(& docker volume ls --quiet | Where-Object { $_ -like '*mlruns*' })
        if ($volumes.Count -eq 0) {
            Write-Host "  aucun volume *mlruns* : rien a supprimer" -ForegroundColor Yellow
        }
        foreach ($name in $volumes) {
            & docker volume rm $name
            if ($LASTEXITCODE -eq 0) {
                Write-Host "  volume $name supprime" -ForegroundColor Green
            }
            else {
                Write-Host "  volume $name NON supprime (encore utilise ?)" -ForegroundColor Red
            }
        }
        Invoke-Step 'redemarrage' {
            & docker compose up -d --build mlflow
        }
        Write-Host ""
        Write-Host "Relancer :  .\fd-manager.ps1 train" -ForegroundColor Cyan
        Write-Host ""
    }

    'ingest' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "N2 : un cycle d'ingestion temps reel, a la main." -ForegroundColor Cyan
        Write-Host "Le DAG est le lot N7 ; ceci eprouve la chaine N1-N3." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "  -Corrupt casse volontairement la derniere lecture (EC25)." -ForegroundColor DarkGray
        Write-Host ""
        $extra = @()
        if ($Corrupt) { $extra += '--corrupt' }
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/ingest_once.py @extra @Rest
        Write-Host ""
    }

    'measure-hour' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "N4 : effet de txn_hour sur le flux." -ForegroundColor Cyan
        Write-Host "Rescore un modele existant, 26 scenarios. Il ne reentraine rien," -ForegroundColor DarkGray
        Write-Host "donc il ne peut pas changer ce qu'il mesure. Quelques secondes." -ForegroundColor DarkGray
        Write-Host ""
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/measure_hour_effect.py @Rest
        Write-Host ""
    }

    'rechoose-threshold' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "N4 : le seuil rechoisi sur la distribution du flux." -ForegroundColor Cyan
        Write-Host "Le seuil 0.93 a ete choisi sur l'heure du PAIEMENT. Sur le flux" -ForegroundColor DarkGray
        Write-Host "il rend 31 alertes/jour au lieu de 38 : la capacite de 60 est" -ForegroundColor DarkGray
        Write-Host "employee a moitie, et de la capacite inemployee est du rappel" -ForegroundColor DarkGray
        Write-Host "abandonne." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "Ajuste une SONDE sur la premiere partie de l'entrainement :" -ForegroundColor DarkGray
        Write-Host "un seuil choisi sur des lignes que le modele a memorisees" -ForegroundColor DarkGray
        Write-Host "n'est pas un seuil. Comptez quelques minutes." -ForegroundColor DarkGray
        Write-Host ""
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/rechoose_threshold.py @Rest
        Write-Host ""
    }

    'compare-recipes' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "N4 : comparaison appariee de deux recettes." -ForegroundColor Cyan
        Write-Host "  .\fd-manager.ps1 compare-recipes --against <version>" -ForegroundColor DarkGray
        Write-Host "  (la version est le nom du repertoire sous datasets/," -ForegroundColor DarkGray
        Write-Host "   12 caracteres, jamais l'empreinte complete de 64)" -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "Fraude par fraude, sur les memes 205 positifs : confronter" -ForegroundColor DarkGray
        Write-Host "deux rappels laisserait +/-0.048 de bruit sur l'ecart, de quoi" -ForegroundColor DarkGray
        Write-Host "rendre un gain de neuf fraudes indecidable." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "  --current-threshold <valeur> lit la recette en service a un" -ForegroundColor DarkGray
        Write-Host "  seuil rechoisi. Sans lui, son manifeste porte encore le seuil" -ForegroundColor DarkGray
        Write-Host "  choisi sur l'heure du PAIEMENT, et la comparaison mesure" -ForegroundColor DarkGray
        Write-Host "  alors aussi le reglage." -ForegroundColor DarkGray
        Write-Host ""
        if (-not ($Rest -join ' ' -match '--against')) {
            Write-Host "Indiquez la recette candidate :" -ForegroundColor Red
            Write-Host "  .\fd-manager.ps1 compare-recipes --against <version>" -ForegroundColor Yellow
            Write-Host "Les versions presentes sont les dossiers de datasets/fraud_scoring/." -ForegroundColor Yellow
            Write-Host ""
            exit 1
        }
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/compare_recipes.py @Rest
        Write-Host ""
    }

    'realtime' {
        Assert-Env
        Assert-Docker
        $dagId = 'dag_ingest'

        if ($Start -and $Stop) {
            Write-Host ""
            Write-Host "-Start et -Stop ensemble : choisissez." -ForegroundColor Red
            Write-Host ""
            exit 1
        }

        if ($Stop) {
            Invoke-Step "mise en pause de $dagId" {
                & docker compose exec -T airflow-scheduler `
                    airflow dags pause $dagId
            }
            Write-Host ""
            Write-Host "En pause. Les executions en cours s'achevent ; aucune" -ForegroundColor Yellow
            Write-Host "nouvelle ne demarre. Les alertes deja levees restent." -ForegroundColor DarkGray
            Write-Host ""
            return
        }

        if ($Start) {
            Write-Host ""
            Write-Host "Cadence : une execution par minute, 5 appels." -ForegroundColor Cyan
            Write-Host "5 x 1440 = 7200 appels/jour, soit exactement le debit" -ForegroundColor DarkGray
            Write-Host "annonce par l'API et celui sur lequel reposent le seuil" -ForegroundColor DarkGray
            Write-Host "et la capacite de 60 alertes/jour." -ForegroundColor DarkGray
            Write-Host ""
            Invoke-Step "activation de $dagId" {
                & docker compose exec -T airflow-scheduler `
                    airflow dags unpause $dagId
            }
            Write-Host ""
            Write-Host "Actif. Pour arreter :  .\fd-manager.ps1 realtime -Stop" -ForegroundColor Cyan
            Write-Host ""
            return
        }

        Write-Host ""
        Write-Host "N7 : etat du DAG temps reel." -ForegroundColor Cyan
        Write-Host ""
        & docker compose exec -T airflow-scheduler `
            airflow dags details $dagId
        Write-Host ""
        Write-Host "Dix dernieres executions :" -ForegroundColor Cyan
        & docker compose exec -T airflow-scheduler `
            airflow dags list-runs -d $dagId --no-backfill -o table 2>&1 |
            Select-Object -First 14
        Write-Host ""
        Write-Host "Un DAG nouvellement cree est EN PAUSE : demarrez-le avec" -ForegroundColor DarkGray
        Write-Host "  .\fd-manager.ps1 realtime -Start" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Les compteurs cote base :" -ForegroundColor Cyan
        & docker @Psql -c @'
SELECT state, count(*) AS executions,
       sum(n_read) AS lignes_lues, sum(n_rejected) AS rejetees,
       round(avg(duration_s), 2) AS duree_moyenne_s,
       max(ended_at)::timestamp(0) AS derniere
FROM   fd_pipeline_runs
WHERE  dag_id = 'dag_ingest'
GROUP  BY state
ORDER  BY state;
'@
        Write-Host ""
        Write-Host "Une execution restee en 'running' est une execution que la" -ForegroundColor DarkGray
        Write-Host "garde de M1 ne compte ni en succes ni en echec : la tache" -ForegroundColor DarkGray
        Write-Host "mark_failed existe pour qu'il n'y en ait pas." -ForegroundColor DarkGray
        Write-Host ""
    }

    'check-leak' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "N6 : le verdict fuit-il vers le scoring ?" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "La meme charge utile scoree deux fois, une fois is_fraud=0 et" -ForegroundColor DarkGray
        Write-Host "une fois is_fraud=1, avec le modele enregistre : le MEME score" -ForegroundColor DarkGray
        Write-Host "est exige. C'est le seul critere de S3 qui mesure une ABSENCE," -ForegroundColor DarkGray
        Write-Host "parce qu'une fuite ne laisse aucune trace dans les metriques :" -ForegroundColor DarkGray
        Write-Host "un modele qui lit l'etiquette score magnifiquement." -ForegroundColor DarkGray
        Write-Host ""
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/check_leak.py @Rest
        Write-Host ""
    }

    'health' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "N9 : les quatre controles du pipeline, stade (3c)." -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Fraicheur, executions vides, taux de rejet, invariant de volume." -ForegroundColor DarkGray
        Write-Host "Le DAG les passe chaque minute et ne notifie QUE les changements" -ForegroundColor DarkGray
        Write-Host "d'etat : sans cela une fraicheur cassee enverrait 1 440 messages" -ForegroundColor DarkGray
        Write-Host "identiques par jour. Ici les quatre verdicts sont affiches quel" -ForegroundColor DarkGray
        Write-Host "que soit leur etat, et rien n'est envoye sans -Notify." -ForegroundColor DarkGray
        Write-Host ""
        $opsArgs = @()
        if ($Notify) { $opsArgs += '--notify' }
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/ops_check.py @opsArgs @Rest
        Write-Host ""
        Write-Host "Etat enregistre des controles :" -ForegroundColor DarkGray
        & docker @Psql -c @'
SELECT control, state, since::timestamp(0) AS depuis,
       left(detail, 60) AS detail
FROM   fd_ops_state
ORDER BY state DESC, control;
'@
        Write-Host ""
    }

    'abandoned' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "N9 : les executions que personne n'a fermees." -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Une execution morte entre runs.start() et close_run reste en" -ForegroundColor DarkGray
        Write-Host "'running' pour toujours. La garde de M1 compte les 'success' et" -ForegroundColor DarkGray
        Write-Host "les 'failed' : une ligne en 'running' n'est dans aucun des deux," -ForegroundColor DarkGray
        Write-Host "donc ce n'est pas un echec qu'elle voit -- c'est une execution" -ForegroundColor DarkGray
        Write-Host "qui n'a silencieusement jamais eu lieu." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "Le verdict est un age : une heure. Depuis L1, Airflow a sa" -ForegroundColor DarkGray
        Write-Host "propre base, donc dag_run n'est pas lisible d'ici. Le callback" -ForegroundColor DarkGray
        Write-Host "du DAG ferme deja chaque echec qu'Airflow sait rapporter ; il" -ForegroundColor DarkGray
        Write-Host "reste les executions tuees avec leur conteneur, qui attendent" -ForegroundColor DarkGray
        Write-Host "donc une heure avant d'etre fermables." -ForegroundColor DarkGray
        Write-Host ""
        & docker @Psql -f '/work/sql/checks/abandoned_runs.sql'
        if ($LASTEXITCODE -ne 0) {
            Write-Host ""
            Write-Host "La lecture a echoue (code $LASTEXITCODE)." -ForegroundColor Red
            Write-Host ""
            exit 1
        }
        if (-not $Apply) {
            Write-Host ""
            Write-Host "Rien n'a ete modifie. Pour fermer ces executions :" -ForegroundColor Cyan
            Write-Host "  .\fd-manager.ps1 abandoned -Apply" -ForegroundColor Cyan
            Write-Host ""
            Write-Host "Fermer une ligne ne touche aucune transaction, aucune alerte" -ForegroundColor DarkGray
            Write-Host "et aucun verdict : les donnees d'une execution en echec sont" -ForegroundColor DarkGray
            Write-Host "deja en zone brute, et c'est 'replay -Batch <id>' qui les" -ForegroundColor DarkGray
            Write-Host "reprend." -ForegroundColor DarkGray
            Write-Host ""
            return
        }
        Write-Host ""
        Invoke-Step 'fermeture des executions abandonnees' {
            & docker @Psql -f '/work/sql/maintenance/030_close_abandoned_runs.sql'
        }
        Write-Host ""
        Write-Host "Relancer sans -Apply doit maintenant ne rien lister." -ForegroundColor Green
        Write-Host ""
    }

    'report' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "S4 : le rapport du matin." -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Quatre sections sur DEUX axes de temps : les paiements et les" -ForegroundColor DarkGray
        Write-Host "alertes sur l'axe du paiement, les verdicts sur celui de la" -ForegroundColor DarkGray
        Write-Host "connaissance -- ils portent sur des paiements d'il y a 30 jours." -ForegroundColor DarkGray
        Write-Host "Les confondre rendrait le rapport faux sans lever d'erreur." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "La fenetre vient d'une DATE et jamais de l'horloge : deux" -ForegroundColor DarkGray
        Write-Host "executions sur la meme journee rendent le meme document a" -ForegroundColor DarkGray
        Write-Host "l'octet pres. Tout sous le role fd_report, sans acces a la" -ForegroundColor DarkGray
        Write-Host "zone brute." -ForegroundColor DarkGray
        Write-Host ""
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/daily_report.py @Rest
        Write-Host ""
    }

    'verdicts' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "N6 : ecriture des verdicts retenus." -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Le verdict est REEL : rien n'est simule. Il est ecrit avec" -ForegroundColor DarkGray
        Write-Host "label_ts = txn_ts + delai, et ce qui le retient est que tout" -ForegroundColor DarkGray
        Write-Host "consommateur filtre sur label_ts <= coupure." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "Cette tache ne partage rien avec le scoring : c'est la" -ForegroundColor DarkGray
        Write-Host "quatrieme garantie de N6. --dry-run compte sans ecrire." -ForegroundColor DarkGray
        Write-Host ""
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/write_verdicts.py @Rest
        Write-Host ""
    }

    'alerts' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "N5 : les alertes et leur notification." -ForegroundColor Cyan
        Write-Host ""
        & docker @Psql -c @'
SELECT a.notified_at IS NOT NULL AS notifiee, a.channel, count(*) AS alertes,
       min(a.raised_at)::timestamp(0) AS plus_ancienne,
       max(a.attempts) AS tentatives_max
FROM   fd_frauds a
GROUP BY a.notified_at IS NOT NULL, a.channel
ORDER BY 1;
'@
        Write-Host ""
        Write-Host "Alertes en attente ayant deja echoue, et pourquoi :" -ForegroundColor DarkGray
        & docker @Psql -c @'
SELECT a.txn_id, a.attempts, left(a.last_error, 70) AS motif
FROM   fd_frauds a
WHERE  a.notified_at IS NULL AND a.attempts > 0
ORDER BY a.attempts DESC, a.raised_at
LIMIT  10;
'@
        Write-Host ""
        Write-Host "Capacite : plafond declare dans governance.yml, applique sur" -ForegroundColor DarkGray
        Write-Host "24 h glissantes. Au-dela, les alertes restent LEVEES." -ForegroundColor DarkGray
        Write-Host ""
    }

    'backfill' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "Recalcul des features stockees, puis reconciliation." -ForegroundColor Cyan
        Write-Host "A lancer quand la reconciliation signale des lignes sans features :" -ForegroundColor DarkGray
        Write-Host "un lot a echappe a features.apply_batch." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "  --dry-run compte sans rien ecrire." -ForegroundColor DarkGray
        Write-Host ""
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/backfill_features.py @Rest
        Write-Host ""
    }

    'reference' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "R10 : copie en base des mesures de M5 (rappel, precision, alertes/jour)." -ForegroundColor Cyan
        Write-Host "Une seule fois, pour les recettes entrainees avant R10 : depuis," -ForegroundColor DarkGray
        Write-Host "fit_model les ecrit lui-meme a cote de run.json." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "  --dry-run lit les manifestes sans rien ecrire ; --all reecrit tout." -ForegroundColor DarkGray
        Write-Host ""
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/backfill_reference.py @Rest
        Write-Host ""
    }

    'observe' {
        Assert-Docker
        Write-Host ""
        Write-Host "N0 : observation de l'API temps reel." -ForegroundColor Cyan
        Write-Host "L'adresse mesuree est realtime.endpoint, celle que lit" -ForegroundColor DarkGray
        Write-Host "l'ingestion. --endpoint <url> mesure une autre adresse." -ForegroundColor DarkGray
        Write-Host "Environ 5 minutes. La mesure est ecrite sous runtime/observations/." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "Le fichier produit ne contient AUCUNE donnee nominative :" -ForegroundColor DarkGray
        Write-Host "seules les formes sont relevees (type, longueur, decompte)." -ForegroundColor DarkGray
        Write-Host ""

        # THE INGESTION IS A CAMPAIGN TOO, and nobody counted it until a
        # measurement said so. Under the LocalExecutor dag_ingest runs in this
        # same container, and the producer keys its limiter on the client
        # address: one bucket for both. Measured on 2026-11-20 -- 20 calls back
        # to back found 8 slots of 13, the other 5 being the running DAG's. It
        # cuts the other way as well, and worse: the ingestion asks for up to
        # 10 of the 13 (5 calls, each retried once on a 429), so a campaign run
        # beside it can starve the very chain it is measuring.
        $ingest = @(& docker compose exec -T airflow-scheduler `
                       airflow dags list -o plain 2>&1) |
                  Where-Object { $_ -match '^\s*dag_ingest\s' } |
                  Select-Object -First 1
        if ($ingest -and $ingest -match '\bFalse\b') {
            Write-Host "dag_ingest est actif, et il partage le limiteur du producer" -ForegroundColor Yellow
            Write-Host "avec cette campagne : meme conteneur, donc meme adresse." -ForegroundColor Yellow
            Write-Host ""
            Write-Host "Deux consequences. Le plafond releve ici sera celui qui reste," -ForegroundColor DarkGray
            Write-Host "soit treize moins les cinq appels de l'ingestion. Et la" -ForegroundColor DarkGray
            Write-Host "campagne peut affamer l'ingestion, qui demande jusqu'a dix" -ForegroundColor DarkGray
            Write-Host "creneaux sur treize une fois les reessais comptes." -ForegroundColor DarkGray
            Write-Host ""
            Write-Host "Pour une mesure seule :  .\fd-manager.ps1 realtime -Stop" -ForegroundColor Cyan
            Write-Host ""
        }

        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/observe_api.py @Rest
        Write-Host ""
    }

    'lock' {
        Assert-Docker
        Write-Host ""
        Write-Host "Releve des versions reellement resolues par pip." -ForegroundColor Cyan
        Write-Host "airflow/requirements.txt porte des PLANCHERS ; ce verrou dit" -ForegroundColor DarkGray
        Write-Host "ce qui a tourne. C'est lui qui fait la reproductibilite (M8)." -ForegroundColor DarkGray
        Write-Host ""
        $out = Join-Path $PWD 'airflow/requirements.lock'
        $text = & docker compose exec -T airflow-scheduler python3 -m pip freeze
        if ($LASTEXITCODE -ne 0) {
            Write-Host "pip freeze a echoue dans le conteneur." -ForegroundColor Red
            exit 1
        }
        [System.IO.File]::WriteAllLines(
            $out, $text, (New-Object System.Text.UTF8Encoding($false)))
        Write-Host ("  ecrit : airflow/requirements.lock ({0} lignes)" -f $text.Count)
        Write-Host ""
    }

    'progress' {
        Assert-Docker
        Write-Host ""
        Write-Host "Taches ayant produit un journal, par DAG, la plus recente en tete." -ForegroundColor Cyan
        Write-Host "Une tache absente n'a jamais demarre." -ForegroundColor DarkGray
        Write-Host ""
        # The script is a versioned FILE, not a string: PowerShell's argument
        # layer eats double quotes on their way to a native command, and this
        # one needs them.
        & docker compose exec -T airflow-scheduler sh /opt/project/tools/progress.sh
        Write-Host ""
    }

    'batches' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "Lots presents dans la zone brute :" -ForegroundColor Cyan
        Write-Host "execs = nombre d'executions qui portent ce lot (chargement + rejeux)." -ForegroundColor DarkGray
        Write-Host ""
        & docker @Psql -f '/work/sql/checks/raw_batches.sql'
        Write-Host ""
        Write-Host "Rejouer :  .\fd-manager.ps1 replay -Batch <batch_id>" -ForegroundColor Cyan
        Write-Host ""
    }

    'purge' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        if ($Apply) {
            Write-Host "S5 / Q3 : la purge. Des lignes vont etre SUPPRIMEES." -ForegroundColor Yellow
        } else {
            Write-Host "S5 / Q2 : la purge a blanc. Rien n'est supprime." -ForegroundColor Cyan
        }
        Write-Host ""
        Write-Host "S5 est le seul processus qui DETRUIT : ailleurs une erreur se" -ForegroundColor DarkGray
        Write-Host "repare par un rejeu, ici elle est definitive. Ce mode compte ce" -ForegroundColor DarkGray
        Write-Host "qui partirait, nomme les lots refuses faute d'empreinte relevee," -ForegroundColor DarkGray
        Write-Host "et signale le depassement de la part declaree." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "A 90 jours de conservation, la purge ne supprimerait rien avant" -ForegroundColor DarkGray
        Write-Host "fevrier 2027 : --before fixe une autre borne pour l'essayer." -ForegroundColor DarkGray
        Write-Host "  .\fd-manager.ps1 purge -Before 2026-11-01 [-Verify]" -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "Pour supprimer reellement : -Apply (et -Force si la part" -ForegroundColor DarkGray
        Write-Host "declaree est depassee). Rien d'autre ne supprime." -ForegroundColor DarkGray
        Write-Host ""
        $purgeArgs = @()
        if ($Before) { $purgeArgs += '--before'; $purgeArgs += $Before }
        if ($Verify) { $purgeArgs += '--verify' }
        if ($Force)  { $purgeArgs += '--force' }
        # Two runners, not one with a flag: a destructive command one
        # letter away from a harmless one gets typed by mistake.
        $purgeTool = if ($Apply) { 'purge_run.py' } else { 'purge_plan.py' }
        & docker compose exec -T airflow-scheduler `
            python3 "/opt/project/tools/$purgeTool" @purgeArgs @Rest
        if ($LASTEXITCODE -ne 0) { throw "purge : code $LASTEXITCODE" }
        Write-Host ""
    }

    'digests' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "S5 / Q1 : l'empreinte des lots de la zone brute." -ForegroundColor Cyan
        Write-Host ""
        Write-Host "La purge refuse un lot dont l'empreinte n'a pas ete relevee : on ne" -ForegroundColor DarkGray
        Write-Host "detruit pas la preuve avant de l'avoir prise. Les ecritures actuelles" -ForegroundColor DarkGray
        Write-Host "la relevent d'elles-memes ; ceci releve celle des lots anterieurs." -ForegroundColor DarkGray
        Write-Host ""
        $digestArgs = @()
        if ($Apply) { $digestArgs += '--apply' }
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/digest_batches.py @digestArgs
        if ($LASTEXITCODE -ne 0) { throw "digests : code $LASTEXITCODE" }
        Write-Host ""
    }

    'promote' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "The promotion gate." -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Registering is not promoting. Every training leaves a version in the" -ForegroundColor DarkGray
        Write-Host "registry; only this gate moves the 'champion' alias, which is what the" -ForegroundColor DarkGray
        Write-Host "scoring service actually loads." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "The two models are compared on the rows neither of them has seen: the" -ForegroundColor DarkGray
        Write-Host "candidate's test split, restricted to the transactions later than the" -ForegroundColor DarkGray
        Write-Host "end of the champion's training period. The decision itself is taken by" -ForegroundColor DarkGray
        Write-Host "ml4fd/promote.py, and journalled in fd_promotions." -ForegroundColor DarkGray
        Write-Host ""
        $promoteArgs = @()
        if ($Apply)     { $promoteArgs += '--apply' }
        if ($Candidate) { $promoteArgs += @('--candidate', $Candidate) }
        # Inside the container: the gate needs the registry, the datasets
        # volume and the database, and the container already has the three.
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/promote.py @promoteArgs
        $promoteCode = $LASTEXITCODE
        if ($promoteCode -eq 1) {
            # 1 is the refusal to decide on too thin a window, not a failure of
            # the tool: the champion stays in service, which is the safe
            # direction. Throwing here would stop a chain for a working guard.
            Write-Host ""
            Write-Host "No decision taken: the comparison window is too thin." -ForegroundColor Yellow
            Write-Host "The champion stays in service. Try again after a load." -ForegroundColor Yellow
            Write-Host ""
        }
        elseif ($promoteCode -ne 0) { throw "promote : code $promoteCode" }
        elseif (-not $Apply) {
            Write-Host ""
            Write-Host "Dry run. To apply the decision:" -ForegroundColor Cyan
            Write-Host "  .\fd-manager.ps1 promote -Apply" -ForegroundColor Cyan
            Write-Host ""
        }
    }

    'drift' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "The injected amount drift." -ForegroundColor Cyan
        Write-Host ""
        Write-Host "A drift has to enter the pipeline as data or it is not a" -ForegroundColor DarkGray
        Write-Host "drift. This writes the switch governance declares; the" -ForegroundColor DarkGray
        Write-Host "producer multiplies the amount of every transaction it" -ForegroundColor DarkGray
        Write-Host "serves, before the raw zone, so the shift travels through" -ForegroundColor DarkGray
        Write-Host "validation, the features, the scoring and the monitoring" -ForegroundColor DarkGray
        Write-Host "exactly as a real change of the stream would." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "  no argument    reports what is in force" -ForegroundColor DarkGray
        Write-Host "  -Factor 3      multiplies every served amount by three" -ForegroundColor DarkGray
        Write-Host "  -Off           lifts the drift" -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "Read on every request: the producer needs no restart." -ForegroundColor DarkGray
        Write-Host ""

        if ($Off -and $PSBoundParameters.ContainsKey('Factor')) {
            Write-Host "-Factor et -Off ensemble : choisissez." -ForegroundColor Red
            Write-Host ""
            exit 1
        }

        # TRANSLATED HERE AND NOT PASSED THROUGH. The task is named in
        # PowerShell, so its options are too; tools/drift.py keeps the POSIX
        # flags it needs to stay runnable on its own inside the container.
        # tests/test_drift.py ties the two spellings together.
        $extra = @()
        if ($Off) { $extra += '--off' }
        if ($PSBoundParameters.ContainsKey('Factor')) {
            $extra += @('--factor', $Factor)
        }
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/drift.py @extra @Rest
        Write-Host ""
    }

    'monitor' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "The monitoring of the model in service." -ForegroundColor Cyan
        Write-Host ""
        Write-Host "Four controls: the inputs moving away from the training reference," -ForegroundColor DarkGray
        Write-Host "the flag rate moving, the measured performance against the floor the" -ForegroundColor DarkGray
        Write-Host "specification declares, and the quality of what the stream delivers." -ForegroundColor DarkGray
        Write-Host "Every figure lands in fd_monitoring; one state per control lands in" -ForegroundColor DarkGray
        Write-Host "fd_ops_state, and only a change of state is announced." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "A drift alert asks dag_train for a candidate. It cannot put it in" -ForegroundColor DarkGray
        Write-Host "service: the gate decides that, which is why the trigger is allowed" -ForegroundColor DarkGray
        Write-Host "to fire on a drift that turns out harmless." -ForegroundColor DarkGray
        Write-Host ""

        if ($Apply) {
            Enable-Dag 'dag_monitor' 'cadence from governance monitoring.schedule'
            # AND WHAT IT TRIGGERS, which nothing activated until
            # 2026-11-20. A TriggerDagRunOperator against a PAUSED dag
            # creates the run and returns success; the scheduler then
            # ignores it, so the request is recorded in
            # fd_retrain_requests, the alert counts as answered, and
            # nothing ever trains. Activating a trigger without
            # activating its target is the defect of 19 November again,
            # one link further down the chain.
            Enable-Dag 'dag_train' 'cadence from governance, and on request'
            Invoke-Step 'declenchement de dag_monitor' {
                & docker compose exec -T airflow-scheduler `
                    airflow dags trigger dag_monitor
            }
            Write-Host ""
            Write-Host "Follow it:  .\fd-manager.ps1 tasklog -TaskId measure" -ForegroundColor Cyan
            Write-Host ""
        }

        Write-Host "Current state of the four controls:" -ForegroundColor Cyan
        & docker @Psql -c @'
SELECT control, state, since::timestamp(0) AS depuis, left(detail, 54) AS detail
FROM   fd_ops_state
WHERE  control IN ('input_drift', 'prediction_drift', 'performance',
                   'data_quality')
ORDER  BY state DESC, control;
'@
        Write-Host ""
        Write-Host "Last measurements, most recent first:" -ForegroundColor Cyan
        & docker @Psql -c @'
SELECT measured_at::timestamp(0) AS mesure, control, metric,
       round(value, 6) AS valeur, round(threshold, 6) AS limite, state,
       n_rows, n_labelled
FROM   fd_monitoring
ORDER  BY measurement_id DESC
LIMIT  14;
'@
        Write-Host ""
        Write-Host "The series one control draws over the long run (EC45):" -ForegroundColor DarkGray
        Write-Host "  SELECT window_end, value FROM fd_monitoring" -ForegroundColor DarkGray
        Write-Host "   WHERE metric = 'recall_flagged' ORDER BY window_end;" -ForegroundColor DarkGray
        Write-Host ""
        if (-not $Apply) {
            Write-Host "Read only. To take a measurement now:" -ForegroundColor Cyan
            Write-Host "  .\fd-manager.ps1 monitor -Apply" -ForegroundColor Cyan
            Write-Host ""
        }
    }

    'psql' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "Session psql. Quelques commandes :" -ForegroundColor Cyan
        Write-Host "  \dt              les tables"
        Write-Host "  \d fd_transactions  le schema d'une table"
        Write-Host "  \q               quitter"
        Write-Host ""
        # -it rather than -T: an interactive session needs a terminal.
        & docker compose exec -it postgres `
            psql -U $PgUser -d $PgDb
    }

    'init' {
        Assert-Env; Assert-Docker
        # BEFORE THE DDL, and the order is the point. L5 renamed fd_api_calls
        # to fd_scoring_service_calls; the DDL is replayable through
        # CREATE TABLE IF NOT EXISTS, so on a database where init had already
        # run it would create an EMPTY table under the new name and leave the
        # recorded calls in the old one. Renaming first makes the DDL a no-op.
        # Idempotent: it says so and does nothing when there is nothing to do.
        Invoke-Step 'renommage des objets du service de scoring' {
            & docker @Psql -f '/work/sql/maintenance/060_rename_scoring_service_objects.sql'
        }
        foreach ($f in Get-ChildItem 'sql/ddl/*.sql' | Sort-Object Name) {
            $name = $f.Name          # captured: $_ does not exist in the block
            Invoke-Step "DDL $name" {
                & docker @Psql -f "/work/sql/ddl/$name"
            }
        }
        Invoke-Step 'fonction fd_client_features' {
            & docker @Psql -f '/work/sql/functions/fd_client_features.sql'
        }
        # The recorded paths follow the directories they name (S6/R13).
        # Idempotent: a path already under runtime/ matches nothing.
        Invoke-Step 'chemins enregistres sous runtime/' {
            & docker @Psql -f '/work/sql/maintenance/050_move_runtime_paths.sql'
        }
        # The dashboard's password lives in .env, never in sql/ddl (S6/R0).
        # Without it fd_dashboard exists but cannot log in -- said, not hidden.
        if ($env:FD_DASHBOARD_PASSWORD) {
            if ($env:FD_DASHBOARD_PASSWORD -notmatch '^[0-9a-f]{64}$') {
                Write-Host "FD_DASHBOARD_PASSWORD n'a pas la forme attendue (64 caracteres hexadecimaux)." -ForegroundColor Red
                Write-Host "Videz la ligne dans .env puis relancez :  .\fd-manager.ps1 setup" -ForegroundColor Yellow
                exit 1
            }
            Invoke-Step 'mot de passe de fd_dashboard' {
                & docker @Psql -v "dashboard_password=$($env:FD_DASHBOARD_PASSWORD)" -f '/work/sql/roles/dashboard_password.sql'
            }
        }
        else {
            Write-Host "--- fd_dashboard : pas de FD_DASHBOARD_PASSWORD dans .env, le role ne peut pas se connecter" -ForegroundColor Yellow
            Write-Host "    Le generer :  .\fd-manager.ps1 setup   puis   .\fd-manager.ps1 init" -ForegroundColor Yellow
        }
        # The scoring service's password, same treatment and same reasons
        # (sql/roles/scoring_service_password.sql, 185_scoring_service_role.sql).
        if ($env:FD_SCORING_SERVICE_PASSWORD) {
            if ($env:FD_SCORING_SERVICE_PASSWORD -notmatch '^[0-9a-f]{64}$') {
                Write-Host "FD_SCORING_SERVICE_PASSWORD n'a pas la forme attendue (64 caracteres hexadecimaux)." -ForegroundColor Red
                Write-Host "Videz la ligne dans .env puis relancez :  .\fd-manager.ps1 setup" -ForegroundColor Yellow
                exit 1
            }
            Invoke-Step 'mot de passe de fd_scoring_service' {
                & docker @Psql -v "scoring_service_password=$($env:FD_SCORING_SERVICE_PASSWORD)" -f '/work/sql/roles/scoring_service_password.sql'
            }
        }
        else {
            Write-Host "--- fd_scoring_service : pas de FD_SCORING_SERVICE_PASSWORD dans .env, le role ne peut pas se connecter" -ForegroundColor Yellow
            Write-Host "    Le generer :  .\fd-manager.ps1 setup   puis   .\fd-manager.ps1 init" -ForegroundColor Yellow
        }
        Write-Host "`nDDL applique. Le relancer ne doit produire aucune erreur.`n" -ForegroundColor Green
    }

    'load' {
        Assert-Env; Assert-Docker
        # schedule=None: making it active adds no cadence, only the right to
        # run when it is triggered.
        Enable-Dag 'dag_load_history'
        Invoke-Step 'declenchement de S1' {
            & docker compose exec -T airflow-scheduler airflow dags trigger dag_load_history
        }
    }

    'dashboard' {
        Assert-Env; Assert-Docker
        if ($Stop) {
            Invoke-Step 'arret du tableau de bord' { & docker compose stop dashboard }
            return
        }
        # WITHOUT ITS PASSWORD the service would start and every page would say
        # so. Saying it here, before building, costs nothing.
        if (-not $env:FD_DASHBOARD_PASSWORD) {
            Write-Host ""
            Write-Host "FD_DASHBOARD_PASSWORD est vide dans .env : fd_dashboard ne peut pas se connecter." -ForegroundColor Red
            Write-Host "  .\fd-manager.ps1 setup   puis   .\fd-manager.ps1 init" -ForegroundColor Yellow
            Write-Host ""
            exit 1
        }
        $env:BUILDKIT_PROGRESS = 'plain'
        # --force-recreate: ALWAYS a new process. The code is mounted, but
        # Python keeps a module it has imported, and Streamlit re-reads the
        # PAGES on every run while ml4fd/ stays as it was when the container
        # started -- outside the app's directory, and no file event crosses a
        # Docker mount on Windows anyway. Without it, `up` leaves an unchanged
        # container running: a new page then calls a function the old module
        # does not have (AttributeError: rejects_view, 2026-11-17).
        Invoke-Step 'docker compose up --build --force-recreate dashboard' {
            & docker compose up -d --build --force-recreate dashboard
        }
        Write-Host ""
        Write-Host "Tableau de bord : http://localhost:8501" -ForegroundColor Cyan
        Write-Host "  connecte en fd_dashboard : lecture de fd_reports, fd_frauds, fd_transactions et quelques autres, rien de plus"
        Write-Host "  journaux :  .\fd-manager.ps1 logs -Service dashboard"
        Write-Host ""
    }

    'scoring-service' {
        Assert-Env; Assert-Docker
        if ($Stop) {
            Invoke-Step 'arret du service de scoring' { & docker compose stop scoring-service }
            return
        }
        # WITHOUT ITS PASSWORD the service starts and every call fails on the
        # database. Said here, before building, rather than found in a log.
        if (-not $env:FD_SCORING_SERVICE_PASSWORD) {
            Write-Host ""
            Write-Host "FD_SCORING_SERVICE_PASSWORD est vide dans .env : fd_scoring_service ne peut pas se connecter." -ForegroundColor Red
            Write-Host "  .\fd-manager.ps1 setup   puis   .\fd-manager.ps1 init" -ForegroundColor Yellow
            Write-Host ""
            exit 1
        }
        # AND WITHOUT THE PEPPER the pseudonyms would differ from the
        # pipeline's, so a call and a transaction of one cardholder would stop
        # joining -- a defect no error would announce.
        if (-not $env:ML4FD_PEPPER) {
            Write-Host ""
            Write-Host "ML4FD_PEPPER est vide dans .env : les pseudonymes du service" -ForegroundColor Red
            Write-Host "ne correspondraient pas a ceux de l'ingestion." -ForegroundColor Red
            Write-Host "  .\fd-manager.ps1 pepper" -ForegroundColor Yellow
            Write-Host ""
            exit 1
        }
        $env:BUILDKIT_PROGRESS = 'plain'
        # --force-recreate, for the reason the dashboard's block states at
        # length: the code is mounted, but a running container keeps the
        # ml4fd modules it imported at start-up.
        Invoke-Step 'docker compose up --build --force-recreate scoring-service' {
            & docker compose up -d --build --force-recreate scoring-service
        }
        Write-Host ""
        Write-Host "Service de scoring : http://localhost:5001" -ForegroundColor Cyan
        Write-Host "  contrat    : http://localhost:5001/openapi.json"
        Write-Host "  doc lisible: http://localhost:5001/docs"
        Write-Host "  etat       : http://localhost:5001/health   (dit la version servie)"
        Write-Host "  (scoring-service:5000 depuis les conteneurs)" -ForegroundColor DarkGray
        Write-Host "  connecte en fd_scoring_service : trois colonnes de fd_transactions en lecture," -ForegroundColor DarkGray
        Write-Host "  fd_scoring_service_calls en ajout seul, rien de plus" -ForegroundColor DarkGray
        Write-Host "  journaux :  .\fd-manager.ps1 logs -Service scoring-service"
        Write-Host ""
    }

    'forget' {
        Assert-Env; Assert-Docker
        if (-not $Batch) {
            Write-Host ""
            Write-Host "Indiquez le lot a oublier :" -ForegroundColor Red
            Write-Host "  .\fd-manager.ps1 forget -Batch <uuid> [-Apply]" -ForegroundColor Yellow
            Write-Host "Les lots :  .\fd-manager.ps1 batches" -ForegroundColor Yellow
            Write-Host ""
            exit 1
        }
        # Validated where it was typed, like replay: a malformed identifier
        # must not reach the container.
        if ($Batch -notmatch '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') {
            Write-Host ""
            Write-Host "Ce n'est pas un identifiant de lot : $Batch" -ForegroundColor Red
            Write-Host ""
            exit 1
        }
        Write-Host ""
        if ($Apply) {
            # A DIFFERENT SCRIPT, not a flag of the same one: the dry run has
            # no way to delete at all (tools/forget_plan.py).
            # -Force does not confirm the deletion (-Apply does): it lifts the
            # caution of a succeeded run, and only that.
            if ($Force) {
                & docker compose exec -T airflow-scheduler python3 /opt/project/tools/forget_run.py $Batch --force --even-if-successful
            } else {
                & docker compose exec -T airflow-scheduler python3 /opt/project/tools/forget_run.py $Batch --force
            }
            $code = $LASTEXITCODE
            Write-Host ""
            if ($code -eq 0) {
                Write-Host "Lot oublie, et journalise dans fd_purges." -ForegroundColor Green
            } elseif ($code -eq 2) {
                Write-Host "Refuse : rien n'a ete supprime." -ForegroundColor Yellow
                Write-Host "Si le refus est une precaution, -Force la leve ; un refus de donnee, non." -ForegroundColor DarkGray
            } else {
                Write-Host "Echec (code $code)." -ForegroundColor Red
            }
            Write-Host ""
            exit $code
        }
        Write-Host "Mode a blanc : rien ne sera supprime." -ForegroundColor Cyan
        Write-Host ""
        & docker compose exec -T airflow-scheduler python3 /opt/project/tools/forget_plan.py $Batch
        $code = $LASTEXITCODE
        Write-Host ""
        if ($code -eq 1) { exit 1 }
    }

    'replay' {
        Assert-Env; Assert-Docker
        if (-not $Batch) {
            Write-Host ""
            Write-Host "Indiquez le lot a rejouer :" -ForegroundColor Red
            Write-Host "  .\fd-manager.ps1 replay -Batch <uuid>" -ForegroundColor Yellow
            Write-Host "Les lots disponibles :  .\fd-manager.ps1 batches" -ForegroundColor Yellow
            Write-Host ""
            exit 1
        }
        # The identifier is validated here rather than inside the container: a
        # malformed value must fail where it was typed, and it is about to be
        # interpolated into a shell string.
        if ($Batch -notmatch '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') {
            Write-Host ""
            Write-Host "Ce n'est pas un identifiant de lot : $Batch" -ForegroundColor Red
            Write-Host "Attendu : un UUID. Les lots :  .\fd-manager.ps1 batches" -ForegroundColor Yellow
            Write-Host ""
            exit 1
        }

        # NO double quote crosses PowerShell. Its argument-passing layer eats
        # the double quotes of any string handed to a native command, and docker
        # is one -- so a JSON built here, or even a bash script holding quotes,
        # arrives mangled and Airflow refuses it. Escaping is not reliable
        # across versions either; the sound answer is to keep quotes out of the
        # strings PowerShell passes at all.
        #
        # The JSON is therefore written to a file, byte for byte, and bash reads
        # it back by UNQUOTED command substitution. Two properties make that
        # work, and both were verified rather than assumed:
        #   - bash does not apply quote removal to the result of an expansion,
        #     so the double quotes from the file survive;
        #   - the JSON holds NO SPACE, so it stays a single word. With a space
        #     after the colon it would split into three arguments.
        #
        # batch_id is the only key the DAG reads. A key it ignores would read as
        # a setting that does something.
        Enable-Dag 'dag_load_history'
        $confPath = Write-ConfFile 'replay-conf.json' @('batch_id') @($Batch)
        $script = 'airflow dags trigger dag_load_history --conf $(cat ' +
                  $confPath + ')'

        Invoke-Step "rejeu du lot $Batch" {
            & docker compose exec -T airflow-scheduler bash -c $script
        }
    }

    'tasklog' {
        Assert-Docker
        Write-Host ""
        # A script FILE, which also states the log's AGE. A stale log shown as
        # current is worse than no log: it cost a round of debugging at 02:00,
        # when an 80-minute-old failure read exactly like a fresh one.
        $global:LASTEXITCODE = 0
        & docker compose exec -T airflow-scheduler `
            sh /opt/project/tools/tasklog.sh $TaskId
        if ($LASTEXITCODE -eq 2) {
            Write-Host ""
            Write-Host "La tache n'a peut-etre jamais demarre : une tache en amont" -ForegroundColor Yellow
            Write-Host "a echoue, et Airflow n'execute pas la suite." -ForegroundColor Yellow
            Write-Host "Voir l'ordre des taches :  .\fd-manager.ps1 progress" -ForegroundColor Cyan
        }
        Write-Host ""
    }

    'dag-test' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "Execution de la tache '$TaskId' en direct." -ForegroundColor Cyan
        Write-Host "<< airflow tasks test >> execute la tache sans passer par"
        Write-Host "l'ordonnanceur ni par les fichiers de journal : la trace"
        Write-Host "complete arrive ici, a l'ecran."
        Write-Host ""
        $when = (Get-Date -Format 'yyyy-MM-dd')
        & docker compose exec -T airflow-scheduler `
            airflow tasks test dag_load_history $TaskId $when
        if ($LASTEXITCODE -ne 0) {
            Write-Host ""
            Write-Host "La tache a echoue -- la cause est dans la trace ci-dessus." -ForegroundColor Yellow
            Write-Host ""
        }
    }

    'typology' {
        Assert-Env
        Assert-Docker
        # Run inside the container: the measurement needs psycopg and the
        # database, and the container already has both.
        Invoke-Step 'mesure des magasins' {
            & docker compose exec -T airflow-scheduler `
                python /opt/project/tools/measure_stores.py --print
        }
        Write-Host ""
        Write-Host "Pour ecrire TYPOLOGY.md, lancez depuis votre venv :" -ForegroundColor Cyan
        Write-Host "  python tools/measure_stores.py"
        Write-Host ""
    }

    'test'         { Invoke-Step 'pytest' { & python -m pytest -q } }
    'lint'         { Invoke-Step 'ruff'   { & python -m ruff check . } }
    'traceability' { Invoke-Step 'tracabilite' { & python tools/build_traceability.py } }

    'accept' {
        Assert-Env
        Assert-Docker
        Write-Host ""
        Write-Host "Criteres d'acceptation. Tous doivent lire OK." -ForegroundColor Cyan
        Write-Host ""
        Write-Host "=== S1 : chargement historique (L0 a L8)"
        & docker @Psql -f '/work/sql/checks/s1_acceptance.sql'

        Write-Host ""
        Write-Host "=== S2 : jeu d'entrainement (M0 a M4)"
        & docker @Psql -f '/work/sql/checks/s2_acceptance.sql'

        Write-Host ""
        Write-Host "=== S2 : artefacts et chaine de tracabilite (M5 a M8)"
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/check_s2.py /opt/project/runtime/datasets

        Write-Host ""
        Write-Host "=== S3 : ingestion temps reel (N1 a N9)"
        & docker @Psql -f '/work/sql/checks/s3_acceptance.sql'

        Write-Host ""
        Write-Host "=== S4 : rapport du matin (P1 a P5)"
        & docker @Psql -f '/work/sql/checks/s4_acceptance.sql'

        Write-Host ""
        Write-Host "=== S4 : documents, empreintes et decomptes recalcules"
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/check_s4.py

        Write-Host ""
        Write-Host "=== S3 : fichiers et declarations (N8)"
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/check_s3.py

        Write-Host ""
        Write-Host "=== S5 : purge et retention (Q1 a Q6)"
        & docker @Psql -f '/work/sql/checks/s5_acceptance.sql'

        Write-Host ""
        Write-Host "=== S5 : declaration, DAG et fichiers des jeux"
        & docker compose exec -T airflow-scheduler `
            python3 /opt/project/tools/check_s5.py

        Write-Host ""
        Write-Host "=== S6 : le role fd_dashboard, lu dans le catalogue (R0 a R5)"
        & docker @Psql -f '/work/sql/checks/s6_acceptance.sql'

        Write-Host ""
        Write-Host "=== S6 : le conteneur du tableau de bord, tel qu'il tourne"
        # On the HOST, not in the scheduler: there is no Docker inside a container.
        & python tools/check_s6.py

        Write-Host ""
        Write-Host "=== L5 : le service de scoring, lu dans le catalogue"
        & docker @Psql -f '/work/sql/checks/l5_acceptance.sql'

        Write-Host ""
        Write-Host "=== L5 : le service de scoring, tel qu'il repond"
        # On the HOST too: it calls the service on 127.0.0.1:5001.
        & python tools/check_l5.py

        Write-Host ""
        Write-Host "=== Les fichiers Parquet sur le disque"
        if (Test-Path 'datasets') {
            Get-ChildItem -Path 'datasets' -Recurse -Filter '*.parquet' |
                ForEach-Object {
                    Write-Host ("  {0,10:N0} octets  {1}" -f $_.Length,
                                $_.FullName.Substring($PWD.Path.Length + 1))
                }
            if (-not (Get-ChildItem -Path 'datasets' -Recurse -Filter '*.parquet')) {
                Write-Host "  aucun fichier Parquet : S2 n'a pas encore construit de jeu"
            }
        }
        else {
            Write-Host "  le dossier datasets/ n'existe pas encore"
        }
        Write-Host ""
    }

    'check' {
        Assert-Env; Assert-Docker
        Invoke-Step 'criteres d''acceptation de S1' {
            & docker @Psql -f '/work/sql/checks/s1_acceptance.sql'
        }
        Invoke-Step 'criteres d''acceptation de S2' {
            & docker @Psql -f '/work/sql/checks/s2_acceptance.sql'
        }
        Invoke-Step 'artefacts et chaine de S2' {
            & docker compose exec -T airflow-scheduler `
                python3 /opt/project/tools/check_s2.py /opt/project/runtime/datasets
        }
        Invoke-Step 'criteres d''acceptation de S3' {
            & docker @Psql -f '/work/sql/checks/s3_acceptance.sql'
        }
        Invoke-Step 'fichiers et declarations de S3' {
            & docker compose exec -T airflow-scheduler `
                python3 /opt/project/tools/check_s3.py
        }
        Invoke-Step 'criteres d''acceptation de S4' {
            & docker @Psql -f '/work/sql/checks/s4_acceptance.sql'
        }
        Invoke-Step 'documents et decomptes de S4' {
            & docker compose exec -T airflow-scheduler `
                python3 /opt/project/tools/check_s4.py
        }
        Invoke-Step 'criteres d''acceptation de S5' {
            & docker @Psql -f '/work/sql/checks/s5_acceptance.sql'
        }
        Invoke-Step 'declaration, DAG et fichiers de S5' {
            & docker compose exec -T airflow-scheduler `
                python3 /opt/project/tools/check_s5.py
        }
        Invoke-Step 'criteres d''acceptation de S6' {
            & docker @Psql -f '/work/sql/checks/s6_acceptance.sql'
        }
        Invoke-Step 'conteneur du tableau de bord (S6)' {
            & python tools/check_s6.py
        }
        Invoke-Step 'criteres d''acceptation de L5' {
            & docker @Psql -f '/work/sql/checks/l5_acceptance.sql'
        }
        Invoke-Step 'service de scoring reel (L5)' {
            # On the HOST, like check_s6: there is no Docker inside a container,
            # and the service answers on 127.0.0.1:5001 here.
            & python tools/check_l5.py
        }
        Invoke-Step 'absence de fuite du verdict' {
            & docker compose exec -T airflow-scheduler `
                python3 /opt/project/tools/check_leak.py
        }
        Invoke-Step 'tracabilite (les deux sens)' {
            & python tools/build_traceability.py --check
        }
        Invoke-Step 'tests unitaires' { & python -m pytest -q }
        Invoke-Step 'concordance des deux modes (doit etre vide)' {
            & docker @Psql -f '/work/sql/checks/reconcile_features.sql'
        }
        Invoke-Step 'fermeture des compteurs (doit etre vide)' {
            & docker @Psql -f '/work/sql/checks/counters_close.sql'
        }
        Invoke-Step 'matins sans rapport (doit etre vide)' {
            & docker @Psql -f '/work/sql/checks/s4_absence.sql'
        }
        Invoke-Step 'volumetrie mesuree' {
            & docker @Psql -f '/work/sql/checks/volumetry.sql'
        }
        Write-Host "`nTous les criteres passent.`n" -ForegroundColor Green
    }
}
