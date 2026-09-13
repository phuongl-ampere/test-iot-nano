import type { TelemetryPoint } from "../lib/browser-api";

export function PowerTelemetryTable({ points }: { points: TelemetryPoint[] }) {
  const keys = Array.from(
    new Set(points.flatMap((point) => Object.keys(point.measurements ?? {}))),
  ).sort();

  return (
    <div className="telemetry-table-wrap">
      <table>
        <thead>
          <tr>
            <th>Recorded</th>
            {keys.map((key) => <th key={key}>{key}</th>)}
          </tr>
        </thead>
        <tbody>
          {points.map((point) => (
            <tr key={point.at}>
              <td>{formatTimestamp(point.at)}</td>
              {keys.map((key) => <td key={key}>{formatValue(point.measurements?.[key])}</td>)}
            </tr>
          ))}
          {points.length === 0 && (
            <tr>
              <td className="table-empty" colSpan={Math.max(keys.length + 1, 1)}>
                No telemetry records in this range.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function formatTimestamp(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "—";
  }
  return typeof value === "object" ? JSON.stringify(value) : String(value);
}
