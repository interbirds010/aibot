"""Interactively encrypt a Solana keypair for SOLANA_PRIVATE_KEY_ENCRYPTED."""

from __future__ import annotations

import getpass
import os

from cryptography.fernet import Fernet


def main() -> None:
    encryption_key = os.environ.get("SOLANA_KEY_ENCRYPTION_KEY", "").strip()
    if not encryption_key:
        raise SystemExit("Set SOLANA_KEY_ENCRYPTION_KEY in the OS environment first")
    private_key = getpass.getpass("Solana private key (base58 or JSON byte array): ").strip()
    if not private_key:
        raise SystemExit("Private key cannot be empty")
    ciphertext = Fernet(encryption_key.encode()).encrypt(private_key.encode()).decode()
    print("Copy this ciphertext into .env as SOLANA_PRIVATE_KEY_ENCRYPTED:")
    print(ciphertext)


if __name__ == "__main__":
    main()
