"use client";

import type { TimeRange } from "../lib/browser-api";

export function TimeRangeControl({
  onChange,
  value,
}: {
  onChange(value: TimeRange): void;
  value: TimeRange;
}) {
  return (
    <div aria-label="Telemetry range" className="range-control">
      {(["1h", "24h", "7d"] as TimeRange[]).map((range) => (
        <button
          aria-pressed={value === range}
          key={range}
          onClick={() => onChange(range)}
          type="button"
        >
          {range}
        </button>
      ))}
    </div>
  );
}
