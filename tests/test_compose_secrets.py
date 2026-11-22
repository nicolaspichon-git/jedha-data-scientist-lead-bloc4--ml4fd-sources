"""No secret is written in a versioned file -- the half of B4-EC29 a test can hold.

Block: B4.
Lot: B4-L6.
Competency: B4-C4.
Covers: B4-EC29.

B4-EC29 asks for two things: secrets managed by the chain's own store, and no
value in clear in the repository. The first is a property of the workflow
(GITHUB_TOKEN, nothing created by hand). The second is a property of the files,
and files can be read: this module reads them.

WHY A TEST AND NOT A HABIT. On 2026-11-22 docker-compose.yml carried two
secrets in clear -- the session key of the Airflow webserver and the password
of the admin account -- and nobody had noticed, because nothing looked. A rule
that only lives in a reviewer's head is broken the first evening the reviewer
is tired.

WHAT COUNTS AS WRITTEN IN CLEAR. A value is clean when it is an interpolation
that compose resolves from .env: ${NAME}, or ${NAME:?message}, which refuses to
start without it, or ${NAME:-} with an EMPTY default, which writes nothing and
only makes the variable optional -- the AWS credentials are, since a warm cache
needs none. A NON-EMPTY default is not clean: ${NAME:-admin} puts "admin" in the
repository exactly as plainly as "admin" does, and starts silently with it.

ROTATION, NOT HISTORY. A secret that was once committed stays readable in the
commits before this test. The remedy is to change its value, which makes the
old one worthless -- not to rewrite the history.
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "docker-compose.yml"
EXAMPLE = ROOT / ".env.example"

# A key is secret-like when its name says so. KEY alone would catch every
# AWS_ACCESS_KEY_ID and every harmless "_KEY" setting; SECRET already covers
# SECRET_KEY and AWS_SECRET_ACCESS_KEY.
SECRET_NAME = re.compile(r"PASSWORD|SECRET|TOKEN|PEPPER", re.I)

# ${NAME}, ${NAME:?message} / ${NAME?message}, or ${NAME:-} / ${NAME-} with an
# empty default: resolved from .env, refused, or optional -- never a value.
CLEAN = re.compile(r"^\$\{[A-Za-z_][A-Za-z0-9_]*(?::?\?[^}]*|:?-)?\}$")

# On a command line: --password VALUE or --password=VALUE.
PASSWORD_FLAG = re.compile(r"--password[ =]+(\S+)")

# user:password@ inside a URL.
URL_CREDENTIAL = re.compile(r"://[^/\s:@]+:([^@\s/]+)@")

# What .env.example may say instead of a value: nothing, or an obvious
# placeholder that no deployment could mistake for a real secret.
PLACEHOLDERS = {"", "change-me"}


def _is_interpolated(value: str) -> bool:
    return bool(CLEAN.match(value.strip()))


def _references_a_variable(value: str) -> bool:
    """A shell or compose variable rather than a literal: $NAME, "$NAME",
    $$NAME (compose's escape, so the CONTAINER's shell reads it), ${NAME}."""
    return bool(re.match(r"""^["']?\$\$?\{?[A-Za-z_]""", value))


def _environment(service: dict) -> dict[str, str]:
    """A service's environment, in either of compose's two spellings."""
    env = service.get("environment") or {}
    if isinstance(env, list):
        pairs = (item.split("=", 1) for item in env)
        env = {k: (v[0] if v else "") for k, *v in pairs}
    return {str(k): "" if v is None else str(v) for k, v in env.items()}


def _commands(service: dict) -> list[str]:
    """Every string a service hands to a shell: command, entrypoint, healthcheck."""
    out = []
    for field in ("command", "entrypoint"):
        value = service.get(field)
        if value:
            out.append(" ".join(value) if isinstance(value, list) else str(value))
    test = (service.get("healthcheck") or {}).get("test")
    if test:
        out.append(" ".join(test) if isinstance(test, list) else str(test))
    return out


def literal_secrets(compose: dict) -> list[str]:
    """Every place a secret is written as a value rather than referenced."""
    found = []
    for name, service in (compose.get("services") or {}).items():
        for key, value in _environment(service).items():
            if SECRET_NAME.search(key) and value and not _is_interpolated(value):
                found.append("%s: environment %s is written in clear" % (name, key))
            for password in URL_CREDENTIAL.findall(value):
                if not _references_a_variable(password):
                    found.append("%s: environment %s embeds a password in a URL"
                                 % (name, key))
        for command in _commands(service):
            for value in PASSWORD_FLAG.findall(command):
                if not _references_a_variable(value):
                    found.append("%s: a command passes --password in clear" % name)
    return found


def secret_like_keys(compose: dict) -> int:
    """How many secret-like keys there are to check -- the guard against a test
    that passes because the file stopped looking like what it reads."""
    return sum(1 for service in (compose.get("services") or {}).values()
               for key in _environment(service) if SECRET_NAME.search(key))


def _load() -> dict:
    return yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))


# --- the repository ----------------------------------------------------------

def test_no_secret_is_written_in_the_compose_file():
    found = literal_secrets(_load())
    assert not found, "\n".join(found)


def test_the_check_has_something_to_check():
    """Not vacuous: the compose file does carry secret-like keys, so the test
    above read them rather than finding none."""
    assert secret_like_keys(_load()) >= 3


def test_dotenv_is_never_versioned():
    lines = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert ".env" in [line.strip() for line in lines]


def test_the_example_holds_no_value():
    """.env.example is versioned, and is copied to make .env: a real value typed
    into it once would ship to every clone."""
    for number, line in enumerate(EXAMPLE.read_text(encoding="utf-8").splitlines(), 1):
        if line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if SECRET_NAME.search(key):
            assert value.strip() in PLACEHOLDERS, \
                ".env.example:%d: %s carries a value" % (number, key)


# --- the detector, so the guard cannot pass for the wrong reason --------------

def _service(**fields) -> dict:
    return {"services": {"x": fields}}


def test_a_literal_is_caught():
    assert literal_secrets(_service(environment={"API_SECRET_KEY": "demo"}))


def test_a_default_counts_as_a_literal():
    """${NAME:-admin} starts silently with "admin": as plain as writing it."""
    assert literal_secrets(_service(environment={"ADMIN_PASSWORD": "${P:-admin}"}))


def test_an_empty_default_is_clean():
    """Optional, not written: the AWS credentials of the corpora."""
    assert not literal_secrets(_service(
        environment={"AWS_SECRET_ACCESS_KEY": "${AWS_SECRET_ACCESS_KEY:-}"}))


def test_an_interpolation_is_clean():
    assert not literal_secrets(_service(environment={"ADMIN_PASSWORD": "${P}"}))
    assert not literal_secrets(_service(
        environment={"ADMIN_PASSWORD": "${P:?set P in .env}"}))


def test_the_list_spelling_is_read_too():
    assert literal_secrets(_service(environment=["DB_PASSWORD=hunter2"]))


def test_a_password_on_a_command_line_is_caught():
    assert literal_secrets(_service(command=["bash", "-c",
                                             "airflow users create --password admin"]))


def test_a_password_read_from_the_environment_is_clean():
    """$$ is compose's escape: the container's shell reads the variable, and
    the value never appears in the file or on the command line compose builds."""
    assert not literal_secrets(_service(
        command='airflow users create --password "$$AIRFLOW_ADMIN_PASSWORD"'))


def test_a_password_inside_a_url_is_caught():
    assert literal_secrets(_service(environment={
        "CONN": "postgresql://fd_airflow:hunter2@postgres/airflow"}))
    assert not literal_secrets(_service(environment={
        "CONN": "postgresql://fd_airflow:${AIRFLOW_DB_PASSWORD}@postgres/airflow"}))
