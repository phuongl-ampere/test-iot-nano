import { PowerMonitorDashboard } from "../../../components/powermonitor-dashboard";
import { PowerMonitorLoginGate } from "../../../components/powermonitor-login-gate";
import { hasPowerMonitorSession } from "../../../lib/page-session";

export default async function DeviceDetailPage({
  params,
}: {
  params: Promise<{ deviceId: string }>;
}) {
  const { deviceId } = await params;
  if (!(await hasPowerMonitorSession())) {
    return <PowerMonitorLoginGate />;
  }

  return <PowerMonitorDashboard initialDeviceId={deviceId} />;
}
