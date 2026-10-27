from __future__ import annotations

import json
import tarfile
import tempfile
from pathlib import Path

from .aws_commands import (
    AwsCli,
    CommandError,
    check_deadline,
    remaining_timeout,
    run_capture,
    run_stream,
    s3_uri,
    send_ssm_command,
    wait_ssm_command,
)
from .aws_models import RUN_ROOT, AwsBasicConfig, TerraformOutputs


def terraform_apply(config: AwsBasicConfig, deadline: float) -> None:
    check_deadline(deadline)
    run_stream(
        ["terraform", "init", "-input=false"],
        cwd=config.terraform_dir,
        timeout_secs=remaining_timeout(deadline),
    )
    check_deadline(deadline)
    run_stream(
        [
            "terraform",
            "apply",
            "-input=false",
            "-auto-approve",
            *terraform_state_args(config),
            *terraform_vars(config),
        ],
        cwd=config.terraform_dir,
        timeout_secs=remaining_timeout(deadline),
    )


def terraform_destroy(config: AwsBasicConfig) -> None:
    if not config.terraform_dir.exists():
        return
    rc = run_stream(
        [
            "terraform",
            "destroy",
            "-input=false",
            "-auto-approve",
            *terraform_state_args(config),
            *terraform_vars(config),
        ],
        cwd=config.terraform_dir,
        check=False,
        timeout_secs=300,
    )
    if rc != 0:
        raise CommandError(f"terraform destroy failed with return code {rc}")


def terraform_outputs(config: AwsBasicConfig) -> TerraformOutputs:
    text = run_capture(
        ["terraform", "output", *terraform_state_args(config), "-json"],
        cwd=config.terraform_dir,
        timeout_secs=60,
    )
    return TerraformOutputs.from_terraform_json(json.loads(text))


def terraform_state_args(config: AwsBasicConfig) -> list[str]:
    return [f"-state={config.output_dir / 'terraform.tfstate'}"]


def terraform_vars(config: AwsBasicConfig) -> list[str]:
    pairs = {
        "run_id": config.run_id,
        "owner": config.owner,
        "instance_type": config.instance_type,
        "region": config.region,
        "availability_zone": config.availability_zone,
        "benchmark_port_start": str(config.base_port),
        "benchmark_port_end": str(config.base_port + len(config.variants) * 1_000 + len(config.rates) + 100),
    }
    args: list[str] = []
    for key, value in pairs.items():
        if value != "":
            args.extend(["-var", f"{key}={value}"])
    return args


def setup_remote_host(
    config: AwsBasicConfig,
    aws: AwsCli,
    instance_id: str,
    bucket: str,
    role: str,
    deadline: float,
) -> None:
    script = f"""set -euo pipefail
if ! command -v aws >/dev/null 2>&1; then
  sudo dnf install -y awscli || sudo dnf install -y awscli-2
fi
sudo dnf install -y gcc gcc-c++ make pkgconf-pkg-config openssl-devel curl tar gzip coreutils
sudo mkdir -p {RUN_ROOT}
sudo chown "$USER":"$USER" {RUN_ROOT}
rm -rf {RUN_ROOT}/source {RUN_ROOT}/results {RUN_ROOT}/source.tar.gz
mkdir -p {RUN_ROOT}/source {RUN_ROOT}/results
aws s3 cp {s3_uri(bucket, config.s3_prefix, "source.tar.gz")} {RUN_ROOT}/source.tar.gz
tar -xzf {RUN_ROOT}/source.tar.gz -C {RUN_ROOT}/source
export PATH="$HOME/.cargo/bin:$PATH"
if ! command -v rustup >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
fi
export PATH="$HOME/.cargo/bin:$PATH"
. "$HOME/.cargo/env" 2>/dev/null || true
rustup toolchain install {config.rust_toolchain} --profile minimal
rustup default {config.rust_toolchain}
cd {RUN_ROOT}/source
cargo build --release --workspace
(
  echo "role={role}"
  echo "instance_id={instance_id}"
  uname -a || true
  rustc -Vv || true
  cargo -Vv || true
  lscpu || true
) > {RUN_ROOT}/results/{role}_host.txt
aws s3 cp {RUN_ROOT}/results/{role}_host.txt {s3_uri(bucket, config.s3_prefix, f"host/{role}_host.txt")}
"""
    command_id = send_ssm_command(aws, instance_id, script, timeout_seconds=1800)
    wait_ssm_command(aws, command_id, instance_id, deadline)


def create_source_bundle(repo_root: Path, run_id: str) -> Path:
    temp_dir = Path(tempfile.mkdtemp(prefix=f"kioto-{run_id}-"))
    bundle = temp_dir / "source.tar.gz"
    excluded_roots = {".git", "target", "results", "plans", "__pycache__"}

    with tarfile.open(bundle, "w:gz") as tar:
        for path in repo_root.rglob("*"):
            rel = path.relative_to(repo_root)
            if should_exclude(rel, excluded_roots):
                continue
            tar.add(path, arcname=rel)
    return bundle


def should_exclude(path: Path, excluded_roots: set[str]) -> bool:
    parts = path.parts
    if not parts:
        return False
    if parts[0] in excluded_roots:
        return True
    if any(part in {"__pycache__", ".terraform"} for part in parts):
        return True
    name = path.name
    return name.endswith(".pyc") or name.endswith(".tfstate") or ".tfstate." in name or name == ".DS_Store"
