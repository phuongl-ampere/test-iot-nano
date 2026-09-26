import type { LiveChart } from "./browser-api";

export type PowerProfilePresentation = {
  label: string;
  charts: LiveChart[];
};

const profiles: Record<string, PowerProfilePresentation> = {
  "asset:Power Farm": {
    label: "Power Farm",
    charts: [{
      aggregation: "sum",
      color: "#d69731",
      label: "Farm demand",
      metric: "power_w",
      unit: "W",
    }],
  },
  "asset:Power Zone": {
    label: "Power Zone",
    charts: [{
      aggregation: "sum",
      color: "#167b83",
      label: "Zone demand",
      metric: "power_w",
      unit: "W",
    }],
  },
  "device:Power Meter": {
    label: "Power Meter",
    charts: [{
      aggregation: "last",
      color: "#167b83",
      label: "Active power",
      metric: "power_w",
      unit: "W",
    }],
  },
};

export function powerProfilePresentation(
  kind: "asset" | "device",
  name: string | undefined,
): PowerProfilePresentation | null {
  return name === undefined ? null : profiles[kind + ":" + name] ?? null;
}

