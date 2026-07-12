#!/bin/sh
set -e
CERT_DIR="/etc/nginx/certs"
mkdir -p "$CERT_DIR"

if [ ! -f "$CERT_DIR/server.key" ]; then
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout "$CERT_DIR/server.key" \
        -out "$CERT_DIR/server.crt" \
        -subj "/CN=localhost/O=Multiplayer Demo/C=US"
    echo "Self-signed certificate generated in $CERT_DIR"
fi
