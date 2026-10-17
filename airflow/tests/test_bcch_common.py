"""Runtime differences between local Airflow and Composer (bcch_common)."""

import os
import stat

import bcch_common
import pytest
from airflow.sdk.exceptions import AirflowException, AirflowFailException


@pytest.fixture
def requirements(tmp_path):
    path = tmp_path / "requirements-dbt.txt"
    path.write_text("dbt-core==1.12.5\n")
    return path


@pytest.fixture
def fake_pip(monkeypatch):
    """subprocess.run that 'builds' a venv by creating its dbt binary."""

    calls = []

    def run(command, check):
        calls.append(command)
        if command[1:3] == ["-m", "venv"]:
            os.makedirs(os.path.join(command[3], "bin"))
        elif command[1] == "install":
            venv = os.path.dirname(os.path.dirname(command[0]))
            open(os.path.join(venv, "bin", "dbt"), "w").close()

    monkeypatch.setattr(bcch_common.subprocess, "run", run)
    return calls


@pytest.fixture
def composer_env(monkeypatch, tmp_path, requirements):
    monkeypatch.delenv("DBT_BIN", raising=False)
    monkeypatch.setenv("DBT_REQUIREMENTS", str(requirements))
    monkeypatch.setenv("DBT_VENV_ROOT", str(tmp_path / "venvs"))
    os.makedirs(tmp_path / "venvs")


def test_dbt_bin_uses_configured_executable(monkeypatch, tmp_path):
    # Local image: dbt baked into its own venv
    dbt = tmp_path / "dbt"
    dbt.write_text("#!/bin/sh\n")
    dbt.chmod(dbt.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("DBT_BIN", str(dbt))

    assert bcch_common.dbt_bin() == str(dbt)


def test_dbt_bin_builds_venv_from_lock_once(composer_env, fake_pip):
    first = bcch_common.dbt_bin()
    second = bcch_common.dbt_bin()

    assert first == second
    assert first.endswith("/bin/dbt")
    # venv + pip install, only for the first call
    assert len(fake_pip) == 2
    assert fake_pip[1][-2:] == ["-r", os.environ["DBT_REQUIREMENTS"]]


def test_changed_lock_gets_new_venv(composer_env, fake_pip, requirements):
    first = bcch_common.dbt_bin()
    requirements.write_text("dbt-core==1.13.0\n")

    assert bcch_common.dbt_bin() != first


def test_interrupted_build_is_rebuilt(composer_env, fake_pip):
    binary = bcch_common.dbt_bin()
    venv = os.path.dirname(os.path.dirname(binary))
    os.remove(os.path.join(venv, ".complete"))

    bcch_common.dbt_bin()

    assert len(fake_pip) == 4


def test_failed_build_is_retryable(composer_env, monkeypatch):
    def fail(command, check):
        raise bcch_common.subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(bcch_common.subprocess, "run", fail)

    with pytest.raises(AirflowException) as raised:
        bcch_common.dbt_bin()

    assert not isinstance(raised.value, AirflowFailException)


def test_dbt_bin_without_any_configuration_fails(monkeypatch):
    monkeypatch.delenv("DBT_BIN", raising=False)
    monkeypatch.delenv("DBT_REQUIREMENTS", raising=False)

    with pytest.raises(AirflowFailException):
        bcch_common.dbt_bin()


def test_token_from_environment_skips_variable(monkeypatch):
    monkeypatch.setenv("BCCH_API_TOKEN", "from-dotenv")

    def no_variables(*args, **kwargs):
        raise AssertionError("Variable.get must not be called")

    monkeypatch.setattr(bcch_common.Variable, "get", no_variables)

    assert bcch_common.runtime_env() == {}


def test_token_from_variable_when_environment_lacks_it(monkeypatch):
    monkeypatch.delenv("BCCH_API_TOKEN", raising=False)
    monkeypatch.setattr(
        bcch_common.Variable,
        "get",
        lambda key, default=None: "from-secret-manager" if key == "bcch_api_token" else default,
    )

    assert bcch_common.runtime_env() == {"BCCH_API_TOKEN": "from-secret-manager"}


def test_no_token_anywhere_leaves_environment_alone(monkeypatch):
    # src/ then fails with its own clear "not configured" error
    monkeypatch.delenv("BCCH_API_TOKEN", raising=False)
    monkeypatch.setattr(bcch_common.Variable, "get", lambda key, default=None: default)

    assert bcch_common.runtime_env() == {}
