import { cookies } from "next/headers";

import {
  createCallbackHandler,
  exchangeAuthorizationCode,
  oauthConfigFromEnvironment,
} from "../../../../../lib/oauth";

export async function GET(request: Request) {
  const config = oauthConfigFromEnvironment();
  return createCallbackHandler({
    requestUrl: request.url,
    cookies: await cookies(),
    exchangeCode: (input) => exchangeAuthorizationCode(config, input),
    redirectUri: config.redirectUri,
  });
}
