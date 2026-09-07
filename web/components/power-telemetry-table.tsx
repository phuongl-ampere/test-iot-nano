"use client";

import { useMemo } from "react";

import type { PowerTelemetryRecord } from "../lib/api";
import { browserDateTime } from "../lib/time";

export function PowerTelemetryTable({ records }: { records: PowerTelemetryRecord[] }) {
  const keys = useMemo(
    () => Array.from(new Set(records.flatMap((record) => Object.keys(record.measurements)))).sort(),
    [records],
  );

  return (
    <div className="readings-table-wrap">
      <table>
        <thead>
          <tr>
            <th>Time</th>
            {keys.map((key) => <th key={key}>{key}</th>)}
          </tr>
        </thead>
        <tbody>
          {records.map((record) => (
            <tr key={`${record.at}-${JSON.stringify(record.measurements)}`}>
              <td>{browserDateTime(record.at, "--")}</td>
              {keys.map((key) => <td key={key}>{formatValue(record.measurements[key])}</td>)}
            </tr>
          ))}
          {records.length === 0 && (
            <tr><td className="table-empty" colSpan={Math.max(keys.length + 1, 1)}>No telemetry records in this range.</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function formatValue(value: unknown): string {
  if (value === undefined || value === null) {
    return "--";
  }
  return typeof value === "string" ? value : JSON.stringify(value);
}
