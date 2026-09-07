import { PowerMonitorAssetDetail } from "../../../../../components/powermonitor-asset-detail";

type AssetDetailPageProps = {
  params: Promise<{ assetId: string }>;
};

export default async function AssetDetailPage({ params }: AssetDetailPageProps) {
  const { assetId } = await params;
  return <PowerMonitorAssetDetail assetId={assetId} />;
}
