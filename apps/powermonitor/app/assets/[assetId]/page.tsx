import { PowerMonitorDashboard } from "../../../components/powermonitor-dashboard";

export default async function AssetDetailPage({
  params,
}: {
  params: Promise<{ assetId: string }>;
}) {
  const { assetId } = await params;
  return <PowerMonitorDashboard initialAssetId={assetId} />;
}
