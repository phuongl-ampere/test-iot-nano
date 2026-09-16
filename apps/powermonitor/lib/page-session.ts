import { cookies } from "next/headers";

import { readSession, sessionCookieName } from "./oauth";

export async function hasPowerMonitorSession(): Promise<boolean> {
  const cookieStore = await cookies();
  return readSession(cookieStore.get(sessionCookieName)?.value) !== null;
}
