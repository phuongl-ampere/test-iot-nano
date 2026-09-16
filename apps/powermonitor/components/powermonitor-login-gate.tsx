export function PowerMonitorLoginGate() {
  return (
    <main className="powermonitor-login">
      <section aria-labelledby="powermonitor-login-title" className="powermonitor-login-panel">
        <span aria-hidden="true" className="brand-mark">P</span>
        <p className="eyebrow">Power Monitor</p>
        <h1 id="powermonitor-login-title">Sign in to view your energy operations</h1>
        <p>Use your authorized platform session to continue.</p>
        <a className="powermonitor-login-action" href="/api/auth/login">Continue to sign in</a>
      </section>
    </main>
  );
}
