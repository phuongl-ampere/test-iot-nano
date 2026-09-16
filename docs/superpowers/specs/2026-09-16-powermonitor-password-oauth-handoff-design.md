# PowerMonitor Password OAuth Handoff Design

## Goal

Let a user sign in from the PowerMonitor login surface with a platform
username and password, then complete the existing PKCE OAuth flow
automatically and land on the PowerMonitor dashboard.

## Approved Flow

```text
PowerMonitor login form
  -> PowerMonitor BFF POST
  -> iot-api POST /api/auth/login
  -> server-only platform session
  -> iot-api /oauth/authorize
  -> existing PowerMonitor OAuth callback
  -> dashboard
```

There is no separate Platform Login Page in this flow.

## Boundaries

- The browser submits credentials only to the same-origin PowerMonitor BFF.
- The BFF sends credentials to `PLATFORM_AUTH_BASE_URL/api/auth/login` over
  server-side HTTPS in production.
- The BFF reads the platform session from the API response and forwards it
  only in its server-to-server authorization request. It never writes
  `iot_nano_session` to the browser.
- The existing OAuth callback continues to create the sealed
  `powermonitor_session` browser cookie. Browser API calls remain unchanged.
- Passwords, platform session IDs, client secrets, authorization codes, and
  access tokens must never appear in client JavaScript, URLs, logs, or UI
  errors.

## User Interface

The existing compact login gate becomes a username/password form with a
single `Sign in` command. Invalid credentials show a generic in-app error.
Platform or OAuth failures show a generic unavailable error. The form uses a
same-origin POST and the BFF rejects cross-origin requests.

## Configuration

`PLATFORM_AUTH_BASE_URL` is a server-only URL for the platform management
login endpoint. `PLATFORM_BASE_URL` remains the public OAuth and API base.

## Verification

- Unit-test successful password-to-PKCE handoff, invalid credentials, and a
  mismatched OAuth state.
- Test the login form controls and error display.
- Run the PowerMonitor suite and production build.
- Smoke-test the live browser flow against the demo iot-api.
