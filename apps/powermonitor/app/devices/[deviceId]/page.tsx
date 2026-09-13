import { PowerMonitorDashboard } from "../../../components/powermonitor-dashboard";

export default async function DeviceDetailPage({
  params,
}: {
  params: Promise<{ deviceId: string }>;
}) {
  const { deviceId } = await params;
  return <PowerMonitorDashboard initialDeviceId={deviceId} />;
}
