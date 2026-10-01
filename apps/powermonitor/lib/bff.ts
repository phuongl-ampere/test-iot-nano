import {
  PlatformApiError,
  type PlatformRequestOptions,
  type PlatformSession,
} from "./platform-client";

type PlatformRequester = (
  path: string,
  session: PlatformSession,
  init?: PlatformRequestOptions,
) => Promise<Response>;

export async function createBffResponse(input: {
  platformRequest: PlatformRequester;
  request: Request;
  session: PlatformSession;
}): Promise<Response> {
  const requestUrl = new URL(input.request.url);
  if (!requestUrl.pathname.startsWith("/api/v1/")) {
    return errorResponse(404, "not_found", "Unknown BFF resource.");
  }
  if (isProfileAssignmentMutation(requestUrl.pathname, input.request.method)) {
    return errorResponse(
      405,
      "profile_read_only",
      "Profiles are assigned by the Tenant Account and are read-only in PowerMonitor.",
    );
  }
  if (isDeviceTokenPath(requestUrl.pathname)) {
    return errorResponse(
      405,
      "token_unavailable",
      "Device tokens are managed in Tenant Console and are unavailable in PowerMonitor.",
    );
  }

  const path = requestUrl.pathname.slice("/api/v1".length) + requestUrl.search;
  const headers = forwardableHeaders(input.request.headers);
  const body = input.request.method === "GET" || input.request.method === "HEAD"
    ? undefined
    : await input.request.text();
  if (isResourceProfileMutation(requestUrl.pathname, input.request.method, body)) {
    return errorResponse(
      405,
      "profile_read_only",
      "Profiles are assigned by the Tenant Account and are read-only in PowerMonitor.",
    );
  }

  try {
    const upstream = await input.platformRequest(path, input.session, {
      body: body === "" ? undefined : body,
      headers,
      method: input.request.method,
    });
    const responseHeaders = new Headers();
    const contentType = upstream.headers.get("content-type");
    if (contentType !== null) {
      responseHeaders.set("content-type", contentType);
    }
    return new Response(upstream.body, {
      headers: responseHeaders,
      status: upstream.status,
      statusText: upstream.statusText,
    });
  } catch (error) {
    if (error instanceof PlatformApiError) {
      if (error.status === 401) {
        return errorResponse(401, "unauthorized", "Sign in is required.");
      }
      if (error.status === 403) {
        return errorResponse(403, "forbidden", "The current scope cannot access this resource.");
      }
      return errorResponse(error.status, "platform_request_failed", "The platform request failed.");
    }
    return errorResponse(502, "platform_unavailable", "The platform could not be reached.");
  }
}

function isProfileAssignmentMutation(pathname: string, method: string): boolean {
  return method !== "GET"
    && method !== "HEAD"
    && /^\/api\/v1\/(?:devices|assets)\/[^/]+\/tenant-profile$/.test(pathname);
}

function isResourceProfileMutation(pathname: string, method: string, body: string | undefined): boolean {
  if ((method !== "PATCH" && method !== "PUT") || body === undefined) {
    return false;
  }
  const field = /^\/api\/v1\/devices\/[^/]+$/.test(pathname)
    ? "device_profile_id"
    : /^\/api\/v1\/assets\/[^/]+$/.test(pathname)
      ? "asset_profile_id"
      : null;
  if (field === null) return false;
  try {
    const payload: unknown = JSON.parse(body);
    return typeof payload === "object"
      && payload !== null
      && Object.prototype.hasOwnProperty.call(payload, field);
  } catch {
    return false;
  }
}

function isDeviceTokenPath(pathname: string): boolean {
  return /^\/api\/v1\/devices\/[^/]+\/token$/.test(pathname);
}

function forwardableHeaders(source: Headers): Headers {
  const headers = new Headers({ accept: "application/json" });
  for (const name of ["content-type", "idempotency-key"]) {
    const value = source.get(name);
    if (value !== null) {
      headers.set(name, value);
    }
  }
  return headers;
}

function errorResponse(status: number, code: string, message: string): Response {
  return Response.json({ code, message, request_id: "powermonitor-bff" }, { status });
}
