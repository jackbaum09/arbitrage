"use client";

import type { DashboardFilters } from "../lib/types";

const SPORTS = ["all", "nba", "cbb", "mlb", "nhl", "nfl"];
const STATUSES: DashboardFilters["status"][] = ["active", "expired", "all"];
const SORT_OPTIONS: { value: DashboardFilters["sortBy"]; label: string }[] = [
  { value: "roi", label: "ROI" },
  { value: "net_profit", label: "Net Profit" },
  { value: "detected_at", label: "Detected" },
];

interface FiltersProps {
  filters: DashboardFilters;
  onChange: (f: DashboardFilters) => void;
}

export default function Filters({ filters, onChange }: FiltersProps) {
  const set = (patch: Partial<DashboardFilters>) =>
    onChange({ ...filters, ...patch });

  return (
    <div className="flex flex-wrap items-center gap-3 mb-4">
      {/* Sport */}
      <select
        value={filters.sport}
        onChange={(e) => set({ sport: e.target.value })}
        className="rounded bg-gray-800 border border-gray-700 px-3 py-1.5 text-sm"
      >
        {SPORTS.map((s) => (
          <option key={s} value={s}>
            {s === "all" ? "All Sports" : s.toUpperCase()}
          </option>
        ))}
      </select>

      {/* Status toggle */}
      <div className="flex rounded overflow-hidden border border-gray-700">
        {STATUSES.map((s) => (
          <button
            key={s}
            onClick={() => set({ status: s })}
            className={`px-3 py-1.5 text-sm capitalize ${
              filters.status === s
                ? "bg-gray-600 text-white"
                : "bg-gray-800 text-gray-400 hover:bg-gray-700"
            }`}
          >
            {s}
          </button>
        ))}
      </div>

      {/* Verified only */}
      <label className="flex items-center gap-1.5 text-sm text-gray-300 cursor-pointer">
        <input
          type="checkbox"
          checked={filters.verifiedOnly}
          onChange={(e) => set({ verifiedOnly: e.target.checked })}
          className="rounded bg-gray-800 border-gray-600"
        />
        Verified only
      </label>

      {/* Sort */}
      <select
        value={filters.sortBy}
        onChange={(e) =>
          set({ sortBy: e.target.value as DashboardFilters["sortBy"] })
        }
        className="rounded bg-gray-800 border border-gray-700 px-3 py-1.5 text-sm ml-auto"
      >
        {SORT_OPTIONS.map((o) => (
          <option key={o.value} value={o.value}>
            Sort: {o.label}
          </option>
        ))}
      </select>

      <button
        onClick={() =>
          set({ sortDir: filters.sortDir === "desc" ? "asc" : "desc" })
        }
        className="rounded bg-gray-800 border border-gray-700 px-3 py-1.5 text-sm hover:bg-gray-700"
        title="Toggle sort direction"
      >
        {filters.sortDir === "desc" ? "\u2193" : "\u2191"}
      </button>
    </div>
  );
}
