"use client";

import { Activity, KeyRound, LogIn } from "lucide-react";
import { FormEvent, useState } from "react";

import { type ApiClient, type Role, createApiClient, login } from "../lib/api";

type LoginScreenProps = {
  apiBaseUrl: string;
  onAuthenticated(client: ApiClient, role: Role): void;
  sessionError?: string | null;
};

export function LoginScreen({
  apiBaseUrl,
  onAuthenticated,
  sessionError = null,
}: LoginScreenProps) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      const session = await login(apiBaseUrl, username, password);
      onAuthenticated(createApiClient(apiBaseUrl, session.sessionId), session.role);
    } catch (loginError) {
      setError(loginError instanceof Error ? loginError.message : "Login failed.");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <main className="login-shell">
      <section className="login-surface" aria-label="Dashboard login">
        <div className="login-brand">
          <span className="brand-mark"><Activity aria-hidden="true" size={18} /></span>
          <div>
            <strong>Rush IoT Nano</strong>
            <span>Fleet monitor</span>
          </div>
        </div>
        <form className="login-form" onSubmit={(event) => void submit(event)}>
          <div className="login-heading">
            <KeyRound aria-hidden="true" size={18} />
            <h1>Access</h1>
          </div>
          <label>
            <span>Username</span>
            <input
              aria-label="Username"
              autoComplete="username"
              onChange={(event) => setUsername(event.target.value)}
              required
              spellCheck={false}
              value={username}
            />
          </label>
          <label>
            <span>Password</span>
            <input
              aria-label="Password"
              autoComplete="current-password"
              minLength={8}
              onChange={(event) => setPassword(event.target.value)}
              required
              spellCheck={false}
              type="password"
              value={password}
            />
          </label>
          {sessionError !== null && <p className="login-error" role="alert">{sessionError}</p>}
          {error !== null && <p className="login-error" role="alert">{error}</p>}
          <button className="login-submit" disabled={submitting} type="submit">
            <LogIn aria-hidden="true" size={16} />
            Sign in
          </button>
        </form>
      </section>
    </main>
  );
}
