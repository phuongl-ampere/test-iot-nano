import { PowerMonitorDashboard } from "../components/powermonitor-dashboard";
import { PowerMonitorLoginGate } from "../components/powermonitor-login-gate";
import { hasPowerMonitorSession } from "../lib/page-session";

export default async function PowerMonitorPage() {
  if (!(await hasPowerMonitorSession())) {
    return <PowerMonitorLoginGate />;
  }

  return <PowerMonitorDashboard />;
}
