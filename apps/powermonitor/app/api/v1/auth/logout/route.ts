import { cookies } from "next/headers";

import { oauthStateCookieName, sessionCookieName } from "../../../../../lib/oauth";

export async function POST(request: Request) {
  const cookieStore = await cookies();
  cookieStore.delete(sessionCookieName);
  cookieStore.delete(oauthStateCookieName);
  return Response.redirect(new URL("/", request.url), 303);
}
