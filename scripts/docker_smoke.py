import os
import shutil
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4


def main() -> int:
    docker = shutil.which("docker")
    if docker is None:
        print("Docker CLI is unavailable.")
        return 2
    subprocess.run([docker, "info", "--format", "{{.ServerVersion}}"], check=True, timeout=30)
    root = Path(__file__).resolve().parents[1]
    project = "companion-smoke-" + uuid4().hex[:10]
    with TemporaryDirectory(prefix=project) as directory:
        work = Path(directory)
        blank_env = work / "empty.env"
        blank_env.touch()
        override = work / "smoke.yaml"
        override.write_text(
            "services:\n  companion:\n    network_mode: none\n"
            "    environment:\n      MODELS_PATH: /app/data/smoke-empty-models.yaml\n",
            encoding="utf-8",
        )
        command = [
            docker, "compose", "--env-file", str(blank_env), "-p", project,
            "-f", str(root / "compose.yaml"), "-f", str(override),
        ]
        env = {**os.environ, "COMPANION_ENV_FILE": str(blank_env)}

        def compose(*args: str, capture: bool = False, timeout: int = 90):
            return subprocess.run(
                [*command, *args], cwd=root, env=env, check=True, text=True,
                stdout=subprocess.PIPE if capture else None, timeout=timeout,
            )

        try:
            compose("up", "--build", "--wait", "--wait-timeout", "90", timeout=900)
            cli = ("exec", "-T", "companion", "python", "-m", "aibottelegram")
            compose(*cli, "healthcheck")
            snapshot = compose(*cli, "backup", capture=True).stdout.strip()
            restored = ("exec", "-T", "-e", "DATA_DIR=/app/data/restore-check",
                        "companion", "python", "-m", "aibottelegram")
            compose(*restored, "restore", snapshot)
            compose(*restored, "init")
            container = compose("ps", "-q", "companion", capture=True).stdout.strip()
            subprocess.run([docker, "stats", "--no-stream", "--format", "{{json .}}", container],
                           check=True, timeout=30)
        finally:
            compose("down", "--volumes", "--remove-orphans")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (subprocess.SubprocessError, OSError) as error:
        raise SystemExit(str(error)) from None
