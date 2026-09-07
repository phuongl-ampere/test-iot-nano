"use client";

import dynamic from "next/dynamic";
import type { ApexOptions } from "apexcharts";

import type { PowerTelemetryPoint } from "../lib/api";

const ApexChart = dynamic(() => import("react-apexcharts"), { ssr: false });

type PowerTelemetryChartProps = {
  points: PowerTelemetryPoint[];
};

export function PowerTelemetryChart({ points }: PowerTelemetryChartProps) {
  const options: ApexOptions = {
    chart: {
      animations: { enabled: false },
      background: "transparent",
      fontFamily: "var(--font-data)",
      toolbar: { show: false },
      zoom: { enabled: false },
    },
    colors: ["#007F71", "#2E6F9E"],
    dataLabels: { enabled: false },
    grid: { borderColor: "#D5E1E5", strokeDashArray: 3 },
    legend: {
      fontSize: "12px",
      horizontalAlign: "left",
      labels: { colors: "#52666D" },
      position: "top",
    },
    stroke: { curve: "straight", width: 2 },
    tooltip: { shared: true, x: { format: "dd MMM HH:mm" } },
    xaxis: {
      type: "datetime",
      axisBorder: { color: "#B8CCD2" },
      axisTicks: { color: "#B8CCD2" },
      labels: { style: { colors: "#52666D", fontSize: "11px" } },
    },
    yaxis: [
      {
        labels: { style: { colors: "#52666D", fontSize: "11px" } },
        title: { text: "Power W", style: { color: "#52666D", fontSize: "11px", fontWeight: 500 } },
      },
      {
        opposite: true,
        labels: { style: { colors: "#52666D", fontSize: "11px" } },
        title: { text: "Voltage V", style: { color: "#52666D", fontSize: "11px", fontWeight: 500 } },
      },
    ],
  };
  const series = [
    {
      name: "Power",
      data: points
        .filter((point) => point.power_w !== null)
        .map((point) => [Date.parse(point.at), point.power_w] as [number, number]),
    },
    {
      name: "Voltage",
      data: points
        .filter((point) => point.voltage_v !== null)
        .map((point) => [Date.parse(point.at), point.voltage_v] as [number, number]),
    },
  ];

  if (points.length === 0) {
    return <div className="chart-empty">No power readings in this time range.</div>;
  }

  return <ApexChart height={330} options={options} series={series} type="line" />;
}
