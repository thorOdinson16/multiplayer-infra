#!/bin/bash
# Generate JWT signing keys for auth service
set -e
KEYS_DIR="./services/auth/keys"
mkdir -p "$KEYS_DIR"
if [ ! -f "$KEYS_DIR/private.pem" ]; then
  openssl genrsa -out "$KEYS_DIR/private.pem" 2048
  openssl rsa -in "$KEYS_DIR/private.pem" -pubout -out "$KEYS_DIR/public.pem"
  echo "Generated JWT keys in $KEYS_DIR"
else
  echo "Keys already exist in $KEYS_DIR"
fi
