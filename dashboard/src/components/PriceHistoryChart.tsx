"use client";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  Legend,
} from "recharts";
import type { PriceHistoryPoint } from "../lib/types";

interface PriceHistoryChartProps {
  history: PriceHistoryPoint[];
}

export default function PriceHistoryChart({ history }: PriceHistoryChartProps) {
  if (history.length === 0) {
    return (
      <p className="text-gray-500 text-sm py-8 text-center">
        No price history available yet.
      </p>
    );
  }

  const data = history.map((h) => ({
    time: new Date(h.snapshot_at).toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    }),
    "Midpoint ROI": h.roi != null ? +(h.roi * 100).toFixed(2) : null,
    "Executable ROI":
      h.roi_executable != null ? +(h.roi_executable * 100).toFixed(2) : null,
    "YES Depth": h.buy_yes_depth,
    "NO Depth": h.buy_no_depth,
  }));

  return (
    <div className="h-72">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" stroke="#333" />
          <XAxis
            dataKey="time"
            tick={{ fill: "#888", fontSize: 11 }}
            interval="preserveStartEnd"
          />
          <YAxis
            tick={{ fill: "#888", fontSize: 11 }}
            tickFormatter={(v: number) => `${v}%`}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: "#1a1a1a",
              border: "1px solid #333",
              borderRadius: "8px",
              fontSize: "12px",
            }}
          />
          <Legend />
          <Line
            type="monotone"
            dataKey="Midpoint ROI"
            stroke="#6b7280"
            strokeWidth={1.5}
            dot={false}
            connectNulls
          />
          <Line
            type="monotone"
            dataKey="Executable ROI"
            stroke="#22c55e"
            strokeWidth={2}
            dot={false}
            connectNulls
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
