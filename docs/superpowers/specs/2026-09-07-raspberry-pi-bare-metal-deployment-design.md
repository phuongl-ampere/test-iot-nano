# Raspberry Pi Bare-Metal Deployment Guide Design

**Date:** 2026-09-07

## Goal

Document a repeatable, production-oriented installation of Rush IoT Nano on
Raspberry Pi OS or Debian ARM64 without Docker and without compiling on the
target machine. The operator receives release artifacts containing the three
Linux ARM64 executables: `iot-api`, `iot-ingest`, and `iot-admin-helper`.

The guide must support the two existing storage modes:

- SQLite stored locally on the Raspberry Pi.
- A remote TimescaleDB instance reached through `DATABASE_URL`.

## Decision

Create `docs/raspberry-pi-bare-metal.md` as a dedicated installation guide and
link to it from the Raspberry Pi section of `docs/operations.md`.

The guide will use explicit shell commands rather than the current
`scripts/install-raspberry-pi.sh`. That script builds `iot-admin-helper` from
source and therefore does not describe a binary-only deployment.

## Installation Flow

1. Verify the operating system is Debian-family ARM64 and validate the
   architecture and SHA-256 values of all three supplied binaries.
2. Install operating-system runtime prerequisites and NanoMQ. Create the
   `iot` and `nanomq` service accounts plus the durable state directories.
3. Install the executables under `/opt/rush-iot-nano`, preserving the
   root-only ownership and mode required by `iot-admin-helper`.
4. Install the existing NanoMQ configuration, systemd unit files, and API
   sudoers allowlist. Reload systemd.
5. Install MQTT TLS material and set distinct, non-placeholder NanoMQ HTTP
   authentication and webhook secrets.
6. Configure exactly one storage branch in both environment files:
   SQLite uses the shared absolute database path under
   `/var/lib/iot-ingest`; TimescaleDB uses a reachable `DATABASE_URL`.
7. Enable and start NanoMQ, ingest, and API. Verify unit status, logs,
   listening ports, and the ingest health endpoint.

## Document Structure

The new guide will contain these sections:

- Scope, supported operating systems, artifact contract, and security
  boundaries.
- Prerequisites and required runtime packages.
- Install NanoMQ, service accounts, directories, binaries, configuration, and
  systemd units.
- TLS certificates and secret generation rules.
- SQLite-local configuration and backup constraints.
- Remote-TimescaleDB configuration and connectivity requirements.
- Start-up and verification commands.
- Upgrade, rollback, and concise failure diagnosis.

`docs/operations.md` will retain operational procedures and link to the new
guide before its Raspberry Pi configuration material.

## Constraints

- No Docker commands or Docker dependencies appear in the installation path.
- The guide does not instruct users to compile Rust on the Pi.
- It must not print or commit real credentials, device tokens, database
  passwords, or TLS private keys.
- It will retain existing runtime locations and service identities so the
  current systemd units, NanoMQ configuration, and helper permissions remain
  valid.
- SQLite and TimescaleDB are mutually exclusive storage selections; the guide
  makes that selection explicit.

## Acceptance Criteria

An operator with a fresh supported Pi and verified binary artifacts can follow
the guide to:

- install all required services without using Docker;
- select either SQLite local storage or a remote TimescaleDB database;
- start `nanomq.service`, `iot-ingest.service`, and `iot-api.service`;
- distinguish a failed binary/service/configuration installation from a
  storage or TLS configuration failure; and
- upgrade or roll back the three application executables consistently.

## Out Of Scope

- Building or cross-compiling release artifacts.
- Provisioning, hardening, or backing up the remote TimescaleDB server.
- Raspberry Pi OS imaging, network provisioning, and firewall policy beyond
  the ports and connectivity required by the installed services.
