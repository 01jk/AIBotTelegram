import getpass
import json
import os
import re
import stat
import sys
import tempfile
import warnings
from dataclasses import dataclass
from pathlib import Path

import aiohttp
from pydantic import SecretStr

MAX_KEY_FILE = 4096
KEY_TEXT = re.compile(r"sk-or-v1-[A-Za-z0-9_-]{16,128}")


class KeyFileError(RuntimeError):
    pass


@dataclass(frozen=True)
class StoredKey:
    secret: SecretStr
    owner: str


def load_key(path: Path) -> StoredKey:
    try:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        with os.fdopen(os.open(path, flags), "rb") as file:
            info = os.fstat(file.fileno())
            if not stat.S_ISREG(info.st_mode) or (os.name != "nt" and info.st_mode & 0o077):
                raise KeyFileError("key file must be a regular private file (chmod 600)")
            contents = file.read(MAX_KEY_FILE + 1)
        if len(contents) > MAX_KEY_FILE:
            raise ValueError
        data = json.loads(contents)
        key, owner = data["key"], data["owner"]
        if not isinstance(key, str) or not key or not isinstance(owner, str) or not owner:
            raise ValueError
        return StoredKey(SecretStr(key), owner)
    except (OSError, UnicodeError, ValueError, KeyError, TypeError):
        raise KeyFileError("could not read the OpenRouter key file") from None


def write_key(path: Path, credential: StoredKey, *, initialize: bool = False) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".openrouter-", dir=path.parent)
    temp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump({"key": credential.secret.get_secret_value(), "owner": credential.owner}, file)
            file.flush()
            os.fsync(file.fileno())
        if initialize:
            os.link(temp, path)
        else:
            os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def read_new_key() -> SecretStr:
    if not sys.stdin.isatty():
        raise KeyFileError("key replacement requires an interactive terminal with hidden input")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            key = getpass.getpass("New OpenRouter key (hidden): ").strip()
    except (EOFError, getpass.GetPassWarning):
        raise KeyFileError("key replacement requires an interactive terminal with hidden input") from None
    if not key or len(key) > 256 or not key.isascii() or not key.isprintable() or any(ch.isspace() for ch in key):
        raise KeyFileError("invalid OpenRouter key format")
    return SecretStr(key)


async def verified_key(http: aiohttp.ClientSession, base_url: str, key: SecretStr) -> StoredKey:
    try:
        async with http.get(
            f"{base_url.rstrip('/')}/key", headers={"Authorization": "Bearer " + key.get_secret_value()},
            allow_redirects=False, timeout=aiohttp.ClientTimeout(total=10, connect=4),
        ) as response:
            if response.status != 200:
                raise KeyFileError(f"OpenRouter key check returned HTTP {response.status}")
            body = await response.json(content_type=None)
    except (aiohttp.ClientError, TimeoutError, ValueError):
        raise KeyFileError("OpenRouter key verification failed") from None
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict) or data.get("is_management_key") or data.get("is_provisioning_key"):
        raise KeyFileError("use a regular OpenRouter inference key")
    creator, organization = data.get("creator_user_id"), data.get("organization_id")
    if not isinstance(creator, str) or not creator or (organization is not None and not isinstance(organization, str)):
        raise KeyFileError("OpenRouter did not confirm the key owner")
    owner = json.dumps([creator, organization], separators=(",", ":"))
    return StoredKey(key, owner)


async def install_key(
    http: aiohttp.ClientSession, base_url: str, path: Path, key: SecretStr, *, initialize: bool = False,
) -> None:
    new = await verified_key(http, base_url, key)
    try:
        write_key(path, new, initialize=initialize)
    except FileExistsError:
        raise KeyFileError("the OpenRouter key file already exists") from None
    except OSError:
        raise KeyFileError("could not save the OpenRouter key file") from None
