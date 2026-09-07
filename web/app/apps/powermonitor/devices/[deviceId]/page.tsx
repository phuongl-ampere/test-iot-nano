import { PowerMonitorApp } from "../../../../../components/powermonitor-app";

type DeviceDetailPageProps = {
  params: Promise<{ deviceId: string }>;
};

export default async function DeviceDetailPage({ params }: DeviceDetailPageProps) {
  const { deviceId } = await params;
  return <PowerMonitorApp initialDeviceId={deviceId} />;
}
