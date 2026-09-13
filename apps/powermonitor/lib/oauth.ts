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
  const verifier = randomBytes(32).toString("base64url");
  const state = randomBytes(32).toString("base64url");
  const challenge = createHash("sha256").update(verifier).digest("base64url");
  const sealState = input.sealState ?? seal;
  const authorizationUrl = new URL("/oauth/authorize", input.config.platformBaseUrl);

  authorizationUrl.search = new URLSearchParams({
    response_type: "code",
    client_id: input.config.clientId,
    redirect_uri: input.config.redirectUri,
    scope: input.config.scope,
    state,
    code_challenge: challenge,
    code_challenge_method: "S256",
  }).toString();

  input.cookies.set(oauthStateCookieName, sealState({ state, verifier }), cookieOptions(300));
  return Response.redirect(authorizationUrl, 307);
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
    client_id: config.clientId,
  });
  const headers = new Headers({ "content-type": "application/x-www-form-urlencoded" });
  if (config.clientSecret !== undefined) {
    headers.set("authorization", `Basic ${Buffer.from(`${config.clientId}:${config.clientSecret}`).toString("base64")}`);
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

type OAuthState = { state: string; verifier: string };

function cookieOptions(maxAge: number) {
  return {
    httpOnly: true,
    secure: true,
    sameSite: "lax" as const,
    path: "/",
    maxAge,
  };
}

function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    throw new Error(`${name} is required`);
  }
  return value;
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
