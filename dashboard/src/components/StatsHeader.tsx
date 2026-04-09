"use client";

import type { ArbitrageOpportunity } from "../lib/types";

interface StatsHeaderProps {
  opportunities: Pick<ArbitrageOpportunity, "roi" | "capital_required" | "sport" | "liquidity_verified">[];
}

export default function StatsHeader({ opportunities }: StatsHeaderProps) {
  const count = opportunities.length;
  const avgRoi =
    count > 0
      ? opportunities.reduce((sum, o) => sum + Number(o.roi), 0) / count
      : 0;
  const totalCapital = opportunities.reduce(
    (sum, o) => sum + Number(o.capital_required),
    0
  );
  const verifiedCount = opportunities.filter((o) => o.liquidity_verified).length;

  const sportCounts: Record<string, number> = {};
  for (const o of opportunities) {
    sportCounts[o.sport] = (sportCounts[o.sport] || 0) + 1;
  }

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
      <Card label="Active Opportunities" value={String(count)} />
      <Card
        label="Avg ROI"
        value={`${(avgRoi * 100).toFixed(2)}%`}
        accent={avgRoi > 0.02 ? "green" : avgRoi > 0 ? "yellow" : "gray"}
      />
      <Card
        label="Total Capital Needed"
        value={`$${totalCapital.toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
      />
      <Card
        label="Verified"
        value={`${verifiedCount} / ${count}`}
        sub={Object.entries(sportCounts)
          .map(([s, n]) => `${s.toUpperCase()} (${n})`)
          .join(", ")}
      />
    </div>
  );
}

function Card({
  label,
  value,
  accent = "gray",
  sub,
}: {
  label: string;
  value: string;
  accent?: "green" | "yellow" | "gray";
  sub?: string;
}) {
  const accentColor =
    accent === "green"
      ? "text-green-400"
      : accent === "yellow"
        ? "text-yellow-400"
        : "text-gray-100";

  return (
    <div className="rounded-lg bg-gray-900 border border-gray-800 p-4">
      <p className="text-xs text-gray-400 uppercase tracking-wide mb-1">
        {label}
      </p>
      <p className={`text-2xl font-semibold ${accentColor}`}>{value}</p>
      {sub && <p className="text-xs text-gray-500 mt-1">{sub}</p>}
    </div>
  );
}
