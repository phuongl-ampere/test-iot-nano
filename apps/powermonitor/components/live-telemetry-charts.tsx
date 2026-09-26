import type { LiveChart, TelemetryPoint, TimeRange } from "../lib/browser-api";

type LiveTelemetryChartsProps = {
  charts: LiveChart[];
  isAsset: boolean;
  points: TelemetryPoint[];
  range: TimeRange;
};

type LinePoint = {
  at: string;
  value: number;
};

export function LiveTelemetryCharts({
  charts,
  isAsset,
  points,
  range,
}: LiveTelemetryChartsProps) {
  if (charts.length === 0) {
    return null;
  }
  return (
    <div className="live-telemetry-charts">
      {charts.map((chart) => (
        <ProfileLineChart
          chart={chart}
          key={chart.metric}
          points={isAsset ? aggregateAssetMetric(points, chart, range) : deviceMetric(points, chart.metric)}
        />
      ))}
    </div>
  );
}

function ProfileLineChart({ chart, points }: { chart: LiveChart; points: LinePoint[] }) {
  if (points.length === 0) {
    return (
      <section className="profile-line-chart" aria-label={chart.label + " line chart"}>
        <header><h3>{chart.label}</h3><span>{chart.unit ?? chart.metric}</span></header>
        <p className="empty-state">No samples are available for this range.</p>
      </section>
    );
  }
  const values = points.map((point) => point.value);
  const minimum = Math.min(...values);
  const maximum = Math.max(...values);
  const span = maximum - minimum || 1;
  const polyline = points.map((point, index) => {
    const x = points.length === 1 ? 50 : (index / (points.length - 1)) * 100;
    const y = 88 - ((point.value - minimum) / span) * 76;
    return x.toFixed(2) + "," + y.toFixed(2);
  }).join(" ");
  const suffix = chart.unit === undefined ? "" : " " + chart.unit;

  return (
    <figure className="profile-line-chart" aria-label={chart.label + " line chart"}>
      <header><h3>{chart.label}</h3><span>{chart.aggregation}</span></header>
      <svg preserveAspectRatio="none" role="img" viewBox="0 0 100 100">
        <line x1="0" x2="100" y1="88" y2="88" />
        <line x1="0" x2="100" y1="50" y2="50" />
        <line x1="0" x2="100" y1="12" y2="12" />
        <polyline fill="none" points={polyline} stroke={chart.color ?? "#167b83"} />
      </svg>
      <figcaption>
        <span>{minimum.toFixed(1)}{suffix}</span>
        <span>{maximum.toFixed(1)}{suffix}</span>
      </figcaption>
    </figure>
  );
}

function deviceMetric(points: TelemetryPoint[], metric: string): LinePoint[] {
  return points
    .map((point) => ({ at: point.at, value: metricValue(point, metric) }))
    .filter((point): point is LinePoint => point.value !== null)
    .sort((left, right) => left.at.localeCompare(right.at));
}

function aggregateAssetMetric(
  points: TelemetryPoint[],
  chart: LiveChart,
  range: TimeRange,
): LinePoint[] {
  const buckets = new Map<number, LinePoint[]>();
  const size = bucketSize(range);
  for (const point of points) {
    const value = metricValue(point, chart.metric);
    const at = new Date(point.at).getTime();
    if (value === null || Number.isNaN(at)) {
      continue;
    }
    const bucket = Math.floor(at / size) * size;
    const rows = buckets.get(bucket) ?? [];
    rows.push({ at: point.at, value });
    buckets.set(bucket, rows);
  }
  return Array.from(buckets.entries())
    .sort(([left], [right]) => left - right)
    .map(([at, values]) => ({
      at: new Date(at).toISOString(),
      value: aggregate(values, chart.aggregation),
    }));
}

function metricValue(point: TelemetryPoint, metric: string): number | null {
  const value = point.measurements?.[metric] ?? topLevelMetric(point, metric);
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function topLevelMetric(point: TelemetryPoint, metric: string): number | null | undefined {
  if (metric === "power_w") return point.power_w;
  if (metric === "voltage_v") return point.voltage_v;
  if (metric === "current_a") return point.current_a;
  if (metric === "energy_kwh") return point.energy_kwh;
  return undefined;
}

function aggregate(points: LinePoint[], method: LiveChart["aggregation"]): number {
  if (method === "last") {
    return [...points].sort((left, right) => left.at.localeCompare(right.at)).at(-1)!.value;
  }
  const values = points.map((point) => point.value);
  if (method === "sum") return values.reduce((total, value) => total + value, 0);
  if (method === "avg") return values.reduce((total, value) => total + value, 0) / values.length;
  if (method === "min") return Math.min(...values);
  return Math.max(...values);
}

function bucketSize(range: TimeRange): number {
  if (range === "1h") return 60_000;
  if (range === "24h") return 15 * 60_000;
  return 2 * 60 * 60_000;
}
