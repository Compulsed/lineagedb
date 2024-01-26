#!/bin/bash

# Stop existing server
systemctl --user stop lineagedb.service

# Cleanup old version
rm lineagedb-aarch64-unknown-linux-gnu.tar.gz

# Download the latest 'release'
curl -s https://api.github.com/repos/Compulsed/lineagedb/releases/latest \
| grep "lineagedb-aarch64-unknown-linux-gnu.tar.gz" \
| cut -d : -f 2,3 \
| tr -d \" \
| wget -qi -

# Rm old version
rm -rf lineagedb

# Extract new version
tar zxvf lineagedb-aarch64-unknown-linux-gnu.tar.gz

# Start new server
systemctl --user start lineagedb.service