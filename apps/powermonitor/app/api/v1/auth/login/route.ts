import { cookies } from "next/headers";

import {
  createLoginHandler,
  createPasswordLoginHandler,
  oauthConfigFromEnvironment,
  platformAuthBaseUrlFromEnvironment,
} from "../../../../lib/oauth";

export async function GET(request: Request) {
  return createLoginHandler({
    requestUrl: request.url,
    cookies: await cookies(),
    config: oauthConfigFromEnvironment(),
  });
}

export async function POST(request: Request) {
  const config = oauthConfigFromEnvironment();
  const appBaseUrl = new URL(config.redirectUri).origin;
  if (request.headers.get("origin") !== appBaseUrl) {
    return new Response("Forbidden", { status: 403 });
  }

  const form = await request.formData();
  const username = form.get("username");
  const password = form.get("password");
  if (
    typeof username !== "string"
    || username.length === 0
    || typeof password !== "string"
    || password.length === 0
  ) {
    return Response.redirect(new URL("/?login_error=invalid_credentials", appBaseUrl), 303);
  }

  return createPasswordLoginHandler({
    appBaseUrl,
    authBaseUrl: platformAuthBaseUrlFromEnvironment(),
    config,
    cookies: await cookies(),
    password,
    username,
  });
}
