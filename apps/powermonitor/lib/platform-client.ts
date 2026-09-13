export type PlatformFetcher = typeof fetch;

export type PlatformSession = {
  accessToken: string;
  fetcher?: PlatformFetcher;
};

export class PlatformApiError extends Error {
  constructor(
    readonly status: number,
    message = "Platform request failed",
  ) {
    super(message);
    this.name = "PlatformApiError";
  }
}

export async function platformRequest(path: string, session: PlatformSession): Promise<Response> {
  if (!path.startsWith("/") || path.startsWith("//") || path.includes("://")) {
    throw new TypeError("Platform paths must be relative");
  }

  const baseUrl = requireEnvironment("PLATFORM_BASE_URL");
  const url = new URL(`/api/v1${path}`, baseUrl);
  if (url.pathname !== "/api/v1" && !url.pathname.startsWith("/api/v1/")) {
    throw new TypeError("Platform paths must remain under /api/v1");
  }
  const response = await (session.fetcher ?? fetch)(url, {
    headers: {
      accept: "application/json",
      authorization: `Bearer ${session.accessToken}`,
    },
    cache: "no-store",
  });

  if (!response.ok) {
    throw new PlatformApiError(response.status, "Platform request denied");
  }

  return response;
}

function requireEnvironment(name: "PLATFORM_BASE_URL"): string {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    throw new Error(`${name} is required`);
  }
  return value;
}
