import { cookies } from "next/headers";

import { createBffResponse } from "../../../../lib/bff";
import { readSession, sessionCookieName } from "../../../../lib/oauth";
import { platformRequest } from "../../../../lib/platform-client";

export const runtime = "nodejs";

async function proxy(request: Request): Promise<Response> {
  const session = readSession((await cookies()).get(sessionCookieName)?.value);
  if (session === null) {
    return Response.json(
      {
        code: "unauthorized",
        message: "Sign in is required.",
        request_id: "powermonitor-bff",
      },
      { status: 401 },
    );
  }
  return createBffResponse({ platformRequest, request, session });
}

export const GET = proxy;
export const POST = proxy;
export const PUT = proxy;
export const PATCH = proxy;
export const DELETE = proxy;
