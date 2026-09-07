"use client";

import type { TimeRange } from "../lib/api";

interface TimeRangeControlProps {
  value: TimeRange;
  onChange(value: TimeRange): void;
}

const ranges: TimeRange[] = ["1h", "24h", "7d"];

export function TimeRangeControl({ value, onChange }: TimeRangeControlProps) {
  return (
    <div className="time-range-control" aria-label="Telemetry time range">
      {ranges.map((range) => (
        <button
          aria-pressed={value === range}
          className={value === range ? "is-active" : ""}
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
