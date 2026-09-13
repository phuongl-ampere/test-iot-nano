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

  const path = requestUrl.pathname.slice("/api/v1".length) + requestUrl.search;
  const headers = forwardableHeaders(input.request.headers);
  const body = input.request.method === "GET" || input.request.method === "HEAD"
    ? undefined
    : await input.request.text();

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
