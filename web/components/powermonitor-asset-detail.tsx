"use client";

import { ArrowLeft, RefreshCw, Zap } from "lucide-react";
import Link from "next/link";
import { useCallback, useEffect, useState } from "react";

import {
  type ApiClient,
  type PowerAsset,
  type PowerTelemetryPoint,
  type TimeRange,
  UnauthorizedApiError,
  fetchPowerAssetTelemetry,
  fetchPowerAssets,
} from "../lib/api";
import { PowerTelemetryChart } from "./power-telemetry-chart";
import { PortalGate } from "./portal-gate";
import { TimeRangeControl } from "./time-range-control";

type PowerMonitorAssetDetailProps = {
  assetId: string;
};

function AssetDetail({ assetId, client, onUnauthorized }: {
  assetId: string;
  client: ApiClient;
  onUnauthorized(): void;
}) {
  const [asset, setAsset] = useState<PowerAsset | null>(null);
  const [points, setPoints] = useState<PowerTelemetryPoint[]>([]);
  const [range, setRange] = useState<TimeRange>("1h");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [assets, telemetry] = await Promise.all([
        fetchPowerAssets(client),
        fetchPowerAssetTelemetry(client, assetId, range),
      ]);
      setAsset(assets.find((item) => item.id === assetId) ?? null);
      setPoints(telemetry);
    } catch (loadError) {
      if (loadError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setError(loadError instanceof Error ? loadError.message : "Asset telemetry request failed.");
    } finally {
      setLoading(false);
    }
  }, [assetId, client, onUnauthorized, range]);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <main className="system-shell">
      <section className="asset-detail">
        <header className="system-configuration-header">
          <div>
            <span className="eyebrow">Power Monitor asset</span>
            <h1>{asset?.name ?? "Asset not found"}</h1>
          </div>
          <div className="workspace-actions">
            <TimeRangeControl onChange={setRange} value={range} />
            <button aria-label="Refresh asset telemetry" className="icon-button" disabled={loading} onClick={() => void load()} title="Refresh asset telemetry" type="button">
              <RefreshCw aria-hidden="true" size={17} />
            </button>
            <Link aria-label="Back to Power Monitor" className="icon-button" href="/apps/powermonitor" title="Back to Power Monitor">
              <ArrowLeft aria-hidden="true" size={17} />
            </Link>
          </div>
        </header>
        {error !== null && <div className="error-banner" role="alert">{error}</div>}
        <dl className="reading-strip asset-reading-strip">
          <div><dt><Zap aria-hidden="true" size={16} /> Current power</dt><dd>{asset === null ? "--" : `${asset.total_power_w.toFixed(1)} W`}</dd></div>
          <div><dt>Energy</dt><dd>{asset === null ? "--" : `${asset.total_energy_kwh.toFixed(2)} kWh`}</dd></div>
          <div><dt>Devices</dt><dd>{asset?.device_count ?? 0}</dd></div>
        </dl>
        <section className="chart-section" aria-label="Asset power telemetry chart">
          <div className="section-heading"><div><span className="eyebrow">Aggregate</span><h2>Power trace</h2></div><span>{points.length} samples</span></div>
          <PowerTelemetryChart points={points} />
        </section>
      </section>
    </main>
  );
}

export function PowerMonitorAssetDetail({ assetId }: PowerMonitorAssetDetailProps) {
  return (
    <PortalGate>
      {(session, onUnauthorized) => (
        <AssetDetail assetId={assetId} client={session.client} onUnauthorized={onUnauthorized} />
      )}
    </PortalGate>
  );
}
