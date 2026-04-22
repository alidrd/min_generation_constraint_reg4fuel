import paramiko
from pathlib import Path


EULER_HOST = "euler.ethz.ch"
EULER_USER = "adarudi"
SSH_KEY = Path.home() / ".ssh" / "id_ed25519"


def get_client() -> paramiko.SSHClient:
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect(
        hostname=EULER_HOST,
        username=EULER_USER,
        key_filename=str(SSH_KEY),
    )
    return client


def run_command(client: paramiko.SSHClient, command: str) -> tuple[str, str]:
    _, stdout, stderr = client.exec_command(command)
    return stdout.read().decode(), stderr.read().decode()


def test_connection() -> None:
    with get_client() as client:
        out, err = run_command(client, "hostname && whoami && module list 2>&1 | head -5")
        print(out)
        if err:
            print("stderr:", err)


if __name__ == "__main__":
    test_connection()
