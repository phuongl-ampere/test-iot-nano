export type ServiceClientConfig = {
  clientId: string;
  clientSecret: string;
  platformBaseUrl: string;
  scope: string;
};

export type ServiceAccessToken = {
  accessToken: string;
  expiresIn?: number;
};

export async function exchangeServiceAccessToken(
  config: ServiceClientConfig,
  fetcher: typeof fetch = fetch,
): Promise<ServiceAccessToken> {
  const response = await fetcher(new URL("/oauth/token", config.platformBaseUrl), {
    body: new URLSearchParams({
      grant_type: "client_credentials",
      scope: config.scope,
    }),
    cache: "no-store",
    headers: {
      authorization: "Basic " + Buffer.from(config.clientId + ":" + config.clientSecret).toString("base64"),
      "content-type": "application/x-www-form-urlencoded",
    },
    method: "POST",
  });
  if (!response.ok) {
    throw new Error("Service token exchange denied");
  }

  const payload = (await response.json()) as { access_token?: string; expires_in?: number };
  if (payload.access_token === undefined || payload.access_token.length === 0) {
    throw new Error("Service token response missing access token");
  }
  return { accessToken: payload.access_token, expiresIn: payload.expires_in };
}

export function serviceClientConfigFromEnvironment(): ServiceClientConfig {
  return {
    clientId: required("OAUTH_SERVICE_CLIENT_ID"),
    clientSecret: required("OAUTH_SERVICE_CLIENT_SECRET"),
    platformBaseUrl: required("PLATFORM_BASE_URL"),
    scope: required("OAUTH_SERVICE_SCOPE"),
  };
}

function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    throw new Error(name + " is required");
  }
  return value;
}
