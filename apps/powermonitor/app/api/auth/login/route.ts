import { cookies } from "next/headers";

import { createLoginHandler, oauthConfigFromEnvironment } from "../../../../lib/oauth";

export async function GET(request: Request) {
  return createLoginHandler({
    requestUrl: request.url,
    cookies: await cookies(),
    config: oauthConfigFromEnvironment(),
  });
}
