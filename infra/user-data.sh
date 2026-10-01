#!/bin/bash
# First-boot setup for the AirBreda VM (Amazon Linux 2023).
set -eux
dnf install -y docker cronie git
dnf install -y postgresql17 || dnf install -y postgresql16 || true
systemctl enable --now docker crond
usermod -aG docker ec2-user
# 1 GB RAM is tight for pip builds and three containers, so add 2 GB swap.
fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
echo '/swapfile swap swap defaults 0 0' >> /etc/fstab
