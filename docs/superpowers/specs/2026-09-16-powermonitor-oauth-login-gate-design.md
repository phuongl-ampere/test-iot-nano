# PowerMonitor OAuth Login Gate Design

## Goal

Give PowerMonitor a simple, explicit sign-in screen without duplicating
credential handling or weakening the existing OAuth BFF boundary.

## Scope

- Add a session-aware server-side gate for the PowerMonitor root, device, and
  asset pages.
- Render a compact login surface when the sealed PowerMonitor session cookie
  is missing or invalid.
- Send the user to the existing GET /api/auth/login OAuth initiation route.
- Preserve the existing callback, BFF proxy, PKCE state cookie, and dashboard
  behavior after a successful callback.

## Deliberate Non-Goals

- No username/password form in PowerMonitor.
- No client secret, access token, or platform database credential in browser
  code.
- No new platform API route, session format, or OAuth grant type.
- No marketing page or additional account-management flow.

## User Flow

1. A request reaches /, /devices/[deviceId], or /assets/[assetId].
2. The server reads the PowerMonitor session cookie using the existing
   readSession helper.
3. Without a valid session, it renders PowerMonitorLoginGate.
4. The only primary action is an anchor to /api/auth/login.
5. The existing BFF creates OAuth state plus S256 PKCE material and redirects
   to the monolith public OAuth endpoint.
6. After the existing callback exchanges the code and seals the app session,
   the user returns to the requested PowerMonitor route and sees the existing
   dashboard or detail view.

## UI

The gate is a small centered operational sign-in surface:

- Product mark and Power Monitor title.
- A short statement that platform access requires a session.
- One Continue to sign in action.
- A concise retry message only when the return URL carries an auth error.

It uses the existing PowerMonitor palette, square-to-slightly-rounded framed
surface, responsive spacing, and no decorative hero treatment.

## Implementation Shape

- Add a server helper that reads the cookie from Next cookies() and returns
  the existing OAuth token or null.
- Add a PowerMonitorLoginGate component and isolated CSS classes.
- Gate all three browser entry pages before rendering their existing content.
- Keep API route authentication unchanged; BFF requests remain a defense in
  depth for invalidated or expired upstream tokens.

## Tests

- Unauthenticated root, device, and asset page renders expose only the login
  action and do not render dashboard content.
- A valid sealed session renders the existing page content.
- The login action points exactly to /api/auth/login.
- Existing OAuth callback and browser API tests remain green.

## Acceptance

- No PowerMonitor page asks for a password.
- No browser bundle contains an OAuth client secret.
- Existing OAuth BFF callback still issues the same session cookie.
- Authenticated navigation and dashboard behavior remain unchanged.
