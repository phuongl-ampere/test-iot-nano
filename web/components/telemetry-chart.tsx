"use client";

import dynamic from "next/dynamic";
import type { ApexOptions } from "apexcharts";

import type { TelemetryPoint } from "../lib/api";

const ApexChart = dynamic(() => import("react-apexcharts"), { ssr: false });

interface TelemetryChartProps {
  points: TelemetryPoint[];
}

export function TelemetryChart({ points }: TelemetryChartProps) {
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
    grid: {
      borderColor: "#D5E1E5",
      strokeDashArray: 3,
    },
    legend: {
      fontSize: "12px",
      horizontalAlign: "left",
      labels: { colors: "#52666D" },
      position: "top",
    },
    stroke: {
      curve: "straight",
      width: 2,
    },
    tooltip: {
      shared: true,
      x: { format: "dd MMM HH:mm" },
    },
    xaxis: {
      type: "datetime",
      axisBorder: { color: "#B8CCD2" },
      axisTicks: { color: "#B8CCD2" },
      labels: { style: { colors: "#52666D", fontSize: "11px" } },
    },
    yaxis: [
      {
        labels: { style: { colors: "#52666D", fontSize: "11px" } },
        title: { text: "Temperature C", style: { color: "#52666D", fontSize: "11px", fontWeight: 500 } },
      },
      {
        opposite: true,
        labels: { style: { colors: "#52666D", fontSize: "11px" } },
        title: { text: "Humidity %", style: { color: "#52666D", fontSize: "11px", fontWeight: 500 } },
      },
    ],
  };
  const series = [
    {
      name: "Temperature",
      data: points
        .filter((point) => point.temperature_c !== null)
        .map((point) => [Date.parse(point.at), point.temperature_c] as [number, number]),
    },
    {
      name: "Humidity",
      data: points
        .filter((point) => point.humidity_pct !== null)
        .map((point) => [Date.parse(point.at), point.humidity_pct] as [number, number]),
    },
  ];

  if (points.length === 0) {
    return <div className="chart-empty">No telemetry in this time range.</div>;
  }

  return <ApexChart height={330} options={options} series={series} type="line" />;
}
