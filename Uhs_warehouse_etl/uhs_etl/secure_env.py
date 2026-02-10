"""
Helpers for reading secrets from environment variables.

Supports Windows DPAPI-protected values stored in env vars
with a "dpapi:" prefix for per-user decryption.
"""

# from __future__ import annotations is a Python “future import.” It comes from the built‑in __future__ module and
# enables behavior from newer Python versions. Here it makes type annotations be stored as strings instead of evaluated
#  immediately. That helps with forward references and avoids runtime issues with types that aren’t defined yet.
from __future__ import annotations

import base64  # Encode/decode binary DPAPI payloads into text for env vars.
import os  # Read environment variables from the current process.

try:
    import win32crypt  # Windows DPAPI wrapper (provided by pywin32).
except Exception:  # pragma: no cover - optional on non-Windows
    # On non-Windows or missing pywin32, keep a sentinel(store placeholder value(None)) so the rest of the code can later say “if it’s None,
    # DPAPI isn’t available” and raise a clear error.
    win32crypt = None

# Add a prefix to every env var.
DPAPI_PREFIX = "dpapi:"


def _require_dpapi() -> None:
    # Guard function: ensures the Windows DPAPI library is available.
    # If not, raise a clear error telling the user to install pywin32.
    if win32crypt is None:
        raise RuntimeError("win32crypt is not available; install pywin32.")


def protect_dpapi(plaintext: str) -> str:
    """
    Protect a plaintext string using Windows DPAPI (current user scope).

    Returns a string suitable for env vars, prefixed with "dpapi:".
    """
    # Make sure we have the Windows DPAPI library available.
    _require_dpapi()
    # Assert for type checkers; win32crypt is guaranteed after _require_dpapi().
    assert win32crypt is not None
    # Do not allow empty input; we need real text to protect.
    if plaintext is None:
        raise ValueError("plaintext is required.")

    # Encrypt the text using Windows DPAPI for the current user account.
    # The output is binary (bytes), so we encode it to base64 for storage.
    blob = win32crypt.CryptProtectData(
        plaintext.encode("utf-8"),
        None,
        None,
        None,
        None,
        0,
    )
    # Prefix with "dpapi:" so callers know this value is encrypted.
    return DPAPI_PREFIX + base64.b64encode(blob).decode("ascii")


def unprotect_dpapi(value: str) -> str:
    """
    Unprotect a DPAPI value (with or without "dpapi:" prefix).
    """
    # Make sure the Windows DPAPI library is available before decrypting.
    _require_dpapi()
    # Assert for type checkers; win32crypt is guaranteed after _require_dpapi().
    assert win32crypt is not None
    # Do not allow empty input; we need an actual encrypted value.
    if value is None:
        raise ValueError("value is required.")

    # Remove the "dpapi:" prefix if it exists so only the payload remains.
    if value.startswith(DPAPI_PREFIX):
        value = value[len(DPAPI_PREFIX) :]

    try:
        # Convert the base64 text back into the original encrypted bytes.
        blob = base64.b64decode(value)
    except Exception as exc:
        raise ValueError("Invalid base64 DPAPI payload.") from exc

    try:
        # Decrypt the bytes using Windows DPAPI for the current user.
        _, data = win32crypt.CryptUnprotectData(blob, None, None, None, 0)
    except Exception as exc:
        # If the user/context doesn't match, decryption will fail.
        raise RuntimeError("DPAPI decryption failed for current user.") from exc

    # Convert decrypted bytes back to a normal string.
    return data.decode("utf-8")


def read_secret_env(var_name: str) -> str | None:
    """
    Read an env var and decrypt if it uses the DPAPI prefix.
    """
    # Look up the environment variable in the current process.
    value = os.environ.get(var_name)
    # If it is missing or empty, return None to signal "no value".
    if not value:
        return None
    # If the value starts with "dpapi:", decrypt it for the current user.
    if value.startswith(DPAPI_PREFIX):
        return unprotect_dpapi(value)
    # Otherwise return the plain text value as-is.
    return value
