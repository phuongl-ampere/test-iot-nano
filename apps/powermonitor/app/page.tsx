import { PowerMonitorDashboard } from "../components/powermonitor-dashboard";
import { PowerMonitorLoginGate } from "../components/powermonitor-login-gate";
import { hasPowerMonitorSession } from "../lib/page-session";

export default async function PowerMonitorPage({
  searchParams,
}: {
  searchParams: Promise<{ login_error?: string }>;
}) {
  if (!(await hasPowerMonitorSession())) {
    const { login_error: loginError } = await searchParams;
    const error = loginError === "invalid_credentials" || loginError === "platform_unavailable"
      ? loginError
      : undefined;
    return <PowerMonitorLoginGate error={error} />;
  }

  return <PowerMonitorDashboard />;
}
