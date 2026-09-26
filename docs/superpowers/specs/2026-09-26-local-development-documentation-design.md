# Local Development Documentation Design

## Goal

Provide one safe, reproducible local path from an empty developer machine to a
seeded PowerMonitor UI backed by the local monolith, without duplicating the
production deployment runbook.

## Scope

The documentation change covers the current monolith topology only. It will:

- add one canonical local-development guide;
- reduce the README's local-seed section to a link and short summary;
- mark the former four-service operations document as historical and redirect
  readers to the monolith runbook;
- leave production deployment, Compose, runtime scripts, and application code
  unchanged.

## Canonical Local Guide

`docs/local-development.md` will describe this ordered workflow:

1. Install or verify the required command-line tools: Rust/Cargo, Node/npm,
   `curl`, `jq`, `lsof`, and OpenSSL.
2. Create the local runtime directory, generate a device-token vault key and a
   local MQTT certificate/key, and apply owner-only file permissions. These
   files are deliberately retained by reset/seed runs.
3. Copy and populate the ignored local seed configuration from
   `infra/monolith/local-platform-seed.env.example`.
4. Copy the PowerMonitor environment template to `.env.local`, set its local
   platform and OAuth values, then install its npm dependencies.
5. Run the guarded seed command. It creates the disposable SQLite platform,
   bootstraps the monolith, and creates the four documented demo users.
6. Start PowerMonitor on port 3002 in a second terminal, then verify backend
   health and the UI, and sign in with the credentials the developer put in the
   seed file.

The guide will distinguish disposable platform state from preserved local
secrets and call out that reset invalidates browser sessions. It will use the
actual default local listener addresses: public HTTP `127.0.0.1:18080`,
management `127.0.0.1:18081`, MQTT `127.0.0.1:18883`, MQTT TLS
`127.0.0.1:18884`, and PowerMonitor `http://localhost:3002`.

## Documentation Boundaries

`README.md` remains the repository entry point. It will state that the
repository deploys a single monolith and link separately to the local guide
and the production operations runbook.

`docs/operations-monolith.md` remains the production authority. The new local
guide does not repeat systemd, backup, upgrade, rollback, or production secret
handling procedures.

`docs/operations.md` will begin with an unambiguous historical notice. It will
say that its four-service process graph is no longer an operational procedure,
and point to the monolith documents. Its retained material is only historical
context for prior architecture and migrations.

## Validation

Documentation validation will confirm that every command mentioned in the new
guide matches existing scripts or package scripts; the local guide will be
checked for broken relative links; shell examples will be syntax-checked where
they are project scripts. No seed or runtime behavior changes are part of this
work.
