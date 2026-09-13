import type { TelemetryPoint } from "../lib/browser-api";

export function PowerTelemetryChart({ points }: { points: TelemetryPoint[] }) {
  const values = points.map(readPower).filter((value): value is number => value !== null);
  if (values.length === 0) {
    return <p className="empty-state">No power samples are available for this range.</p>;
  }

  const minimum = Math.min(...values);
  const maximum = Math.max(...values);
  const span = maximum - minimum || 1;
  const polyline = points
    .map((point, index) => {
      const value = readPower(point);
      if (value === null) {
        return null;
      }
      const x = points.length === 1 ? 50 : (index / (points.length - 1)) * 100;
      const y = 88 - ((value - minimum) / span) * 76;
      return x.toFixed(2) + "," + y.toFixed(2);
    })
    .filter((value): value is string => value !== null)
    .join(" ");

  return (
    <figure aria-label="Power telemetry chart" className="telemetry-chart">
      <svg preserveAspectRatio="none" role="img" viewBox="0 0 100 100">
        <line x1="0" x2="100" y1="88" y2="88" />
        <line x1="0" x2="100" y1="50" y2="50" />
        <line x1="0" x2="100" y1="12" y2="12" />
        <polyline fill="none" points={polyline} />
      </svg>
      <figcaption>
        <span>{minimum.toFixed(1)} W</span>
        <span>{maximum.toFixed(1)} W</span>
      </figcaption>
    </figure>
  );
}

function readPower(point: TelemetryPoint): number | null {
  if (typeof point.power_w === "number") {
    return point.power_w;
  }
  const value = point.measurements?.power_w;
  return typeof value === "number" ? value : null;
}
