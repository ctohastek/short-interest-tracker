#!/usr/bin/env python3
"""Generate a PBKDF2 password hash for tracker_config.json.

Usage:
    python3 hash_password.py
    python3 hash_password.py <password>

Paste the output into tracker_config.json under auth.users.
"""

import hashlib
import os
import sys
import getpass


def hash_password(password):
    """Hash a password with PBKDF2-SHA256 + random salt."""
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 600_000)
    return f"pbkdf2:{salt.hex()}:{dk.hex()}"


def main():
    if len(sys.argv) > 1:
        pw = sys.argv[1]
    else:
        pw = getpass.getpass("Password: ")
        pw2 = getpass.getpass("Confirm:  ")
        if pw != pw2:
            print("Passwords don't match!")
            sys.exit(1)

    hashed = hash_password(pw)
    print(f"\nHashed password (paste into tracker_config.json):\n")
    print(f"  {hashed}")
    print()


if __name__ == "__main__":
    main()
