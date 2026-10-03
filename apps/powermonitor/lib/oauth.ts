import { createCipheriv, createDecipheriv, createHash, randomBytes } from "node:crypto";

export const sessionCookieName = "powermonitor_session";
export const oauthStateCookieName = "powermonitor_oauth_state";

export type OAuthConfig = {
  platformBaseUrl: string;
  clientId: string;
  clientSecret?: string;
  redirectUri: string;
  scope: string;
};

export type OAuthCookies = {
  get(name: string): { value: string } | undefined;
  set(name: string, value: string, options: Record<string, unknown>): void;
};

export type OAuthToken = {
  accessToken: string;
  refreshToken?: string;
  expiresIn?: number;
};

export type PasswordLoginError = "invalid_credentials" | "platform_unavailable";

export function readSession(value: string | undefined): OAuthToken | null {
  if (value === undefined) {
    return null;
  }
  try {
    const session = unseal<OAuthToken>(value);
    return typeof session.accessToken === "string" && session.accessToken.length > 0 ? session : null;
  } catch {
    return null;
  }
}

export async function createLoginHandler(input: {
  requestUrl: string;
  cookies: Pick<OAuthCookies, "set">;
  config: OAuthConfig;
  sealState?: (value: OAuthState) => string;
}): Promise<Response> {
  const authorization = createAuthorizationRequest(input.config);
  const sealState = input.sealState ?? seal;
  input.cookies.set(
    oauthStateCookieName,
    sealState({ state: authorization.state, verifier: authorization.verifier }),
    cookieOptions(300),
  );
  return Response.redirect(authorization.url, 307);
}

export async function createPasswordLoginHandler(input: {
  appBaseUrl: string;
  authBaseUrl: string;
  config: OAuthConfig;
  cookies: Pick<OAuthCookies, "set">;
  exchangeCode?: (input: {
    code: string;
    codeVerifier: string;
    redirectUri: string;
  }) => Promise<OAuthToken>;
  fetcher?: typeof fetch;
  password: string;
  sealSession?: (value: OAuthToken) => string;
  username: string;
}): Promise<Response> {
  const fetcher = input.fetcher ?? fetch;
  let loginResponse: Response;
  try {
    loginResponse = await fetcher(new URL("/api/v1/auth/login", input.authBaseUrl), {
      body: JSON.stringify({ username: input.username, password: input.password }),
      cache: "no-store",
      headers: { "content-type": "application/json" },
      method: "POST",
    });
  } catch {
    return passwordLoginError(input.appBaseUrl, "platform_unavailable");
  }

  if (loginResponse.status === 401) {
    return passwordLoginError(input.appBaseUrl, "invalid_credentials");
  }
  if (!loginResponse.ok) {
    return passwordLoginError(input.appBaseUrl, "platform_unavailable");
  }

  const platformSession = platformSessionFromSetCookie(loginResponse.headers.get("set-cookie"));
  if (platformSession === null) {
    return passwordLoginError(input.appBaseUrl, "platform_unavailable");
  }

  const authorization = createAuthorizationRequest(input.config);
  let authorizeResponse: Response;
  try {
    authorizeResponse = await fetcher(authorization.url, {
      cache: "no-store",
      headers: { cookie: `iot_nano_session=${platformSession}` },
      redirect: "manual",
    });
  } catch {
    return passwordLoginError(input.appBaseUrl, "platform_unavailable");
  }

  const authorizationCode = authorizationCodeFromResponse(
    authorizeResponse,
    input.config.redirectUri,
    authorization.state,
  );
  if (authorizationCode === null) {
    return passwordLoginError(input.appBaseUrl, "platform_unavailable");
  }

  const exchangeCode = input.exchangeCode ?? ((request) => exchangeAuthorizationCode(
    input.config,
    request,
    fetcher,
  ));
  let token: OAuthToken;
  try {
    token = await exchangeCode({
      code: authorizationCode,
      codeVerifier: authorization.verifier,
      redirectUri: input.config.redirectUri,
    });
  } catch {
    return passwordLoginError(input.appBaseUrl, "platform_unavailable");
  }

  const sealSession = input.sealSession ?? seal;
  input.cookies.set(sessionCookieName, sealSession(token), cookieOptions(60 * 60 * 8));
  return Response.redirect(new URL("/", input.appBaseUrl), 303);
}

export async function createCallbackHandler(input: {
  requestUrl: string;
  cookies: OAuthCookies;
  exchangeCode: (input: {
    code: string;
    codeVerifier: string;
    redirectUri: string;
  }) => Promise<OAuthToken>;
  redirectUri: string;
  unsealState?: (value: string) => OAuthState;
  sealSession?: (value: OAuthToken) => string;
}): Promise<Response> {
  const request = new URL(input.requestUrl);
  const redirectUri = new URL(input.redirectUri);
  const code = request.searchParams.get("code");
  const state = request.searchParams.get("state");
  const sealedState = input.cookies.get(oauthStateCookieName)?.value;
  if (code === null || state === null || sealedState === undefined) {
    return new Response("Invalid OAuth callback", { status: 400 });
  }

  const unsealState = input.unsealState ?? ((value: string) => unseal<OAuthState>(value));
  let oauthState: OAuthState;
  try {
    oauthState = unsealState(sealedState);
  } catch {
    return new Response("Invalid OAuth callback", { status: 400 });
  }

  if (oauthState.state !== state) {
    return new Response("Invalid OAuth callback", { status: 400 });
  }

  let token: OAuthToken;
  try {
    token = await input.exchangeCode({
      code,
      codeVerifier: oauthState.verifier,
      redirectUri: redirectUri.href,
    });
  } catch {
    return new Response("OAuth token exchange denied", { status: 502 });
  }
  const sealSession = input.sealSession ?? seal;
  input.cookies.set(sessionCookieName, sealSession(token), cookieOptions(60 * 60 * 8));
  input.cookies.set(oauthStateCookieName, "", { ...cookieOptions(0), maxAge: 0 });
  return Response.redirect(new URL("/", redirectUri), 307);
}

export async function exchangeAuthorizationCode(
  config: OAuthConfig,
  input: { code: string; codeVerifier: string; redirectUri: string },
  fetcher: typeof fetch = fetch,
): Promise<OAuthToken> {
  const body = new URLSearchParams({
    grant_type: "authorization_code",
    code: input.code,
    code_verifier: input.codeVerifier,
    redirect_uri: input.redirectUri,
    ...(config.clientSecret === undefined ? { client_id: config.clientId } : {}),
  });
  const headers = new Headers({ "content-type": "application/x-www-form-urlencoded" });
  if (config.clientSecret !== undefined) {
    const clientId = formEncode(config.clientId);
    const clientSecret = formEncode(config.clientSecret);
    headers.set("authorization", `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString("base64")}`);
  }

  const response = await fetcher(new URL("/oauth/token", config.platformBaseUrl), {
    method: "POST",
    headers,
    body,
    cache: "no-store",
  });
  if (!response.ok) {
    throw new Error("OAuth token exchange denied");
  }

  const payload = (await response.json()) as {
    access_token?: string;
    refresh_token?: string;
    expires_in?: number;
  };
  if (payload.access_token === undefined) {
    throw new Error("OAuth token response missing access token");
  }
  return {
    accessToken: payload.access_token,
    refreshToken: payload.refresh_token,
    expiresIn: payload.expires_in,
  };
}

export function oauthConfigFromEnvironment(): OAuthConfig {
  return {
    platformBaseUrl: required("PLATFORM_BASE_URL"),
    clientId: required("OAUTH_CLIENT_ID"),
    clientSecret: process.env.OAUTH_CLIENT_SECRET,
    redirectUri: required("OAUTH_REDIRECT_URI"),
    scope: process.env.OAUTH_SCOPE ?? "devices:read assets:read telemetry:read",
  };
}

export function platformAuthBaseUrlFromEnvironment(): string {
  return required("PLATFORM_AUTH_BASE_URL");
}

type OAuthState = { state: string; verifier: string };

function createAuthorizationRequest(config: OAuthConfig) {
  const verifier = randomBytes(32).toString("base64url");
  const state = randomBytes(32).toString("base64url");
  const challenge = createHash("sha256").update(verifier).digest("base64url");
  const url = new URL("/oauth/authorize", config.platformBaseUrl);

  url.search = new URLSearchParams({
    response_type: "code",
    client_id: config.clientId,
    redirect_uri: config.redirectUri,
    scope: config.scope,
    state,
    code_challenge: challenge,
    code_challenge_method: "S256",
  }).toString();

  return { state, url, verifier };
}

function platformSessionFromSetCookie(value: string | null): string | null {
  if (value === null) {
    return null;
  }
  const prefix = "iot_nano_session=";
  const cookie = value.split(";", 1)[0];
  if (cookie === undefined || !cookie.startsWith(prefix)) {
    return null;
  }
  const session = cookie.slice(prefix.length);
  return /^[A-Za-z0-9_-]+$/.test(session) ? session : null;
}

function authorizationCodeFromResponse(
  response: Response,
  redirectUri: string,
  expectedState: string,
): string | null {
  if (response.status < 300 || response.status >= 400) {
    return null;
  }
  const location = response.headers.get("location");
  if (location === null) {
    return null;
  }
  try {
    const callback = new URL(location);
    const expected = new URL(redirectUri);
    if (callback.origin !== expected.origin || callback.pathname !== expected.pathname) {
      return null;
    }
    const code = callback.searchParams.get("code");
    if (code === null || callback.searchParams.get("state") !== expectedState) {
      return null;
    }
    return code;
  } catch {
    return null;
  }
}

function passwordLoginError(appBaseUrl: string, error: PasswordLoginError): Response {
  const redirect = new URL("/", appBaseUrl);
  redirect.searchParams.set("login_error", error);
  return Response.redirect(redirect, 303);
}

function cookieOptions(maxAge: number) {
  return {
    httpOnly: true,
    secure: webHttpsEnabled(),
    sameSite: "lax" as const,
    path: "/",
    maxAge,
  };
}

function webHttpsEnabled(): boolean {
  return ["true", "1", "on"].includes(process.env.IOT_NANO_HTTPS_ENABLED?.toLowerCase() ?? "");
}

function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function formEncode(value: string): string {
  return new URLSearchParams({ value }).toString().slice("value=".length);
}

function seal(value: object): string {
  const key = encryptionKey();
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  const ciphertext = Buffer.concat([cipher.update(JSON.stringify(value), "utf8"), cipher.final()]);
  return [iv, cipher.getAuthTag(), ciphertext].map((part) => part.toString("base64url")).join(".");
}

function unseal<T>(value: string): T {
  const [encodedIv, encodedTag, encodedCiphertext] = value.split(".");
  if (encodedIv === undefined || encodedTag === undefined || encodedCiphertext === undefined) {
    throw new Error("Invalid sealed value");
  }
  const decipher = createDecipheriv(
    "aes-256-gcm",
    encryptionKey(),
    Buffer.from(encodedIv, "base64url"),
  );
  decipher.setAuthTag(Buffer.from(encodedTag, "base64url"));
  const plaintext = Buffer.concat([
    decipher.update(Buffer.from(encodedCiphertext, "base64url")),
    decipher.final(),
  ]);
  return JSON.parse(plaintext.toString("utf8")) as T;
}

function encryptionKey(): Buffer {
  const secret = process.env.SESSION_SECRET;
  if (secret === undefined || secret.length < 32) {
    throw new Error("SESSION_SECRET must be at least 32 characters");
  }
  return createHash("sha256").update(secret).digest();
}
