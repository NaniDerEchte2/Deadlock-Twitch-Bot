"""
Field-level AES-256-GCM encryption for sensitive database fields.

Standalone fallback for split Twitch runtime when ``service.field_crypto`` is
not importable.
"""

from __future__ import annotations

import logging
import os
import secrets
import struct

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

log = logging.getLogger(__name__)


class CryptoError(Exception):
    """Base exception for crypto operations."""


class KeyMissing(CryptoError):
    """Encryption key not found."""


class DecryptFailed(CryptoError):
    """Decryption failed (wrong key, corrupted data, AAD mismatch)."""


class InvalidPayload(CryptoError):
    """Invalid encrypted payload format."""


class FieldCrypto:
    """AES-256-GCM field-level encryption."""

    VERSION = 1
    NONCE_SIZE = 12
    KEY_SIZE = 32

    def __init__(self):
        self._keys: dict[str, bytes] = {}
        self._load_keys()

    def _load_keys(self) -> None:
        key_v1 = (os.getenv("DB_MASTER_KEY_V1") or "").strip()
        if key_v1:
            try:
                self._keys["v1"] = bytes.fromhex(key_v1)
                if len(self._keys["v1"]) != self.KEY_SIZE:
                    raise KeyMissing(
                        f"Key v1 has invalid size: {len(self._keys['v1'])} bytes (expected {self.KEY_SIZE})"
                    )
                log.info("Loaded encryption key: v1 (env)")
                return
            except ValueError as exc:
                raise KeyMissing(f"Invalid key format for v1 from env: {exc}") from exc

        try:
            import keyring
        except ImportError as exc:
            raise KeyMissing("keyring package not installed. Install with: pip install keyring") from exc

        key_v1 = keyring.get_password("DeadlockBot", "DB_MASTER_KEY_V1")
        if not key_v1:
            raise KeyMissing(
                "DB_MASTER_KEY_V1 not found in Windows Credential Manager. "
                "Run scripts/generate_master_key.py to generate and store the key."
            )

        try:
            self._keys["v1"] = bytes.fromhex(key_v1)
            log.info("Loaded encryption key: v1")
        except ValueError as exc:
            raise KeyMissing(f"Invalid key format for v1: {exc}") from exc

        if len(self._keys["v1"]) != self.KEY_SIZE:
            raise KeyMissing(
                f"Key v1 has invalid size: {len(self._keys['v1'])} bytes (expected {self.KEY_SIZE})"
            )

    def encrypt_field(self, plaintext: str, aad: str, kid: str = "v1") -> bytes:
        if kid not in self._keys:
            raise KeyMissing(f"Encryption key '{kid}' not found")

        key = self._keys[kid]
        nonce = secrets.token_bytes(self.NONCE_SIZE)

        aesgcm = AESGCM(key)
        try:
            ciphertext = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), aad.encode("utf-8"))
        except Exception as exc:
            log.error("Encryption failed: %s", exc)
            raise CryptoError(f"Encryption failed: {exc}") from exc

        kid_bytes = kid.encode("ascii")
        kid_len = len(kid_bytes)
        if kid_len > 255:
            raise ValueError("Key ID too long (max 255 bytes)")

        return struct.pack("BB", self.VERSION, kid_len) + kid_bytes + nonce + ciphertext

    def decrypt_field(self, blob: bytes, aad: str) -> str:
        if not blob:
            raise InvalidPayload("Empty blob")
        if len(blob) < 15:
            raise InvalidPayload(f"Blob too short: {len(blob)} bytes")

        version, kid_len = struct.unpack("BB", blob[:2])
        if version != self.VERSION:
            raise InvalidPayload(f"Unknown version: {version} (expected {self.VERSION})")

        kid_start = 2
        kid_end = kid_start + kid_len
        if len(blob) < kid_end + self.NONCE_SIZE:
            raise InvalidPayload("Blob truncated (missing nonce)")

        try:
            kid = blob[kid_start:kid_end].decode("ascii")
        except UnicodeDecodeError as exc:
            raise InvalidPayload(f"Invalid key ID encoding: {exc}") from exc

        nonce_start = kid_end
        nonce_end = nonce_start + self.NONCE_SIZE
        nonce = blob[nonce_start:nonce_end]
        ciphertext = blob[nonce_end:]
        if not ciphertext:
            raise InvalidPayload("Blob truncated (missing ciphertext)")

        if kid not in self._keys:
            raise KeyMissing(f"Decryption key '{kid}' not found")
        key = self._keys[kid]

        aesgcm = AESGCM(key)
        try:
            plaintext_bytes = aesgcm.decrypt(nonce, ciphertext, aad.encode("utf-8"))
            return plaintext_bytes.decode("utf-8")
        except Exception as exc:
            safe_kid = kid.replace("\r", "\\r").replace("\n", "\\n")
            log.error("Decryption failed for kid=%s", safe_kid, exc_info=True)
            raise DecryptFailed(f"Decryption failed: {exc}") from exc


_crypto: FieldCrypto | None = None


def get_crypto() -> FieldCrypto:
    global _crypto
    if _crypto is None:
        _crypto = FieldCrypto()
    return _crypto


def reset_crypto() -> None:
    global _crypto
    _crypto = None
    log.info("Crypto singleton reset")
