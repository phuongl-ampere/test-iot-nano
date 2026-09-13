"use client";

import { useCallback, useEffect, useState, type ReactNode } from "react";
import { usePathname, useRouter } from "next/navigation";

import {
  type ApiClient,
  type UserSession,
  UnauthorizedApiError,
  createApiClient,
  getCurrentUser,
} from "../lib/api";
import { canAccessPath, rootPath } from "../lib/routing";
import { LoginScreen } from "./login-screen";

const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:8080";
export const sessionStorageKey = "rush-iot-nano.session-id";

type ReadySession = {
  client: ApiClient;
  user: UserSession;
};

type PortalGateProps = {
  children(session: ReadySession, onUnauthorized: () => void): ReactNode;
};

function useSession() {
  const [session, setSession] = useState<ReadySession | null>(null);
  const [ready, setReady] = useState(false);
  const [sessionError, setSessionError] = useState<string | null>(null);

  const clear = useCallback(() => {
    window.sessionStorage.removeItem(sessionStorageKey);
    setSession(null);
    setSessionError(null);
  }, []);

  const restore = useCallback(async (client: ApiClient) => {
    setSessionError(null);
    try {
      const user = await getCurrentUser(client);
      setSession({ client, user });
    } catch (error) {
      if (error instanceof UnauthorizedApiError) {
        clear();
      } else {
        setSessionError(error instanceof Error ? error.message : "Unable to restore the current session.");
      }
    } finally {
      setReady(true);
    }
  }, [clear]);

  useEffect(() => {
    const sessionId = window.sessionStorage.getItem(sessionStorageKey);
    if (sessionId === null) {
      setReady(true);
      return;
    }
    void restore(createApiClient(apiBaseUrl, sessionId));
  }, [restore]);

  const authenticated = useCallback((client: ApiClient) => {
    window.sessionStorage.setItem(sessionStorageKey, client.sessionId);
    setReady(false);
    void restore(client);
  }, [restore]);

  return { authenticated, clear, ready, session, sessionError };
}

export function PortalHome() {
  const router = useRouter();
  const { authenticated, clear, ready, session, sessionError } = useSession();

  useEffect(() => {
    if (session !== null && session.user.accountClass !== "user") {
      router.replace(rootPath(session.user));
    }
  }, [router, session]);

  if (!ready || (session !== null && session.user.accountClass !== "user")) {
    return <main aria-busy="true" className="login-shell" />;
  }

  if (session !== null) {
    return (
      <main className="login-shell">
        <section className="login-card">
          <span className="eyebrow">Platform console</span>
          <h1>Operator access required</h1>
          <p>This console is reserved for platform operators.</p>
          <button onClick={clear} type="button">Sign out</button>
        </section>
      </main>
    );
  }

  return (
    <LoginScreen
      apiBaseUrl={apiBaseUrl}
      onAuthenticated={authenticated}
      sessionError={sessionError}
    />
  );
}

export function PortalGate({ children }: PortalGateProps) {
  const pathname = usePathname();
  const router = useRouter();
  const { authenticated, clear, ready, session, sessionError } = useSession();

  useEffect(() => {
    if (session !== null && !canAccessPath(session.user, pathname)) {
      router.replace(rootPath(session.user));
    }
  }, [pathname, router, session]);

  if (!ready || (session !== null && !canAccessPath(session.user, pathname))) {
    return <main aria-busy="true" className="login-shell" />;
  }

  if (session === null) {
    return (
      <LoginScreen
        apiBaseUrl={apiBaseUrl}
        onAuthenticated={authenticated}
        sessionError={sessionError}
      />
    );
  }

  return <>{children(session, clear)}</>;
}
