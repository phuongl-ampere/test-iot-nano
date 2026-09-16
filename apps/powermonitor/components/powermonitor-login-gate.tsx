type PowerMonitorLoginGateProps = {
  error?: "invalid_credentials" | "platform_unavailable";
};

export function PowerMonitorLoginGate({ error }: PowerMonitorLoginGateProps) {
  return (
    <main className="powermonitor-login">
      <section aria-labelledby="powermonitor-login-title" className="powermonitor-login-panel">
        <span aria-hidden="true" className="brand-mark">P</span>
        <p className="eyebrow">Power Monitor</p>
        <h1 id="powermonitor-login-title">Sign in to view your energy operations</h1>
        <p>Use your platform account to continue.</p>
        <form action="/api/auth/login" aria-label="PowerMonitor sign in" className="powermonitor-login-form" method="post">
          <label className="powermonitor-login-field">
            <span>Username</span>
            <input autoComplete="username" name="username" required spellCheck={false} />
          </label>
          <label className="powermonitor-login-field">
            <span>Password</span>
            <input autoComplete="current-password" name="password" required type="password" />
          </label>
          {error === "invalid_credentials" && <p className="powermonitor-login-error" role="alert">Username or password is incorrect.</p>}
          {error === "platform_unavailable" && <p className="powermonitor-login-error" role="alert">Sign in is temporarily unavailable.</p>}
          <button className="powermonitor-login-action" type="submit">Sign in</button>
        </form>
      </section>
    </main>
  );
}
