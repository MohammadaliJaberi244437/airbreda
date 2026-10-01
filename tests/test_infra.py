"""Deploy files that the VM rebuild (ADR-003, README "Deploy to the VM") depends on."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_crontab_file_ends_with_a_newline():
    # cronie's crontab(1) on Amazon Linux 2023 rejects a file whose last entry has no
    # trailing newline ("premature EOF ... errors in crontab file, can't install").
    assert (ROOT / "infra" / "crontab.txt").read_bytes().endswith(b"\n")


def test_crontab_runs_both_jobs_against_the_vm_env_file():
    lines = [ln for ln in (ROOT / "infra" / "crontab.txt").read_text().splitlines()
             if ln and not ln.startswith("#")]
    assert len(lines) == 2
    assert lines[0].startswith("25 * * * * ") and "airbreda-air" in lines[0]
    assert lines[1].startswith("*/10 * * * * ") and "airbreda-traffic" in lines[1]
    assert all("--env-file /home/ec2-user/airbreda/.env" in ln for ln in lines)
