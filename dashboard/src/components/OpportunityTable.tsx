"use client";

import { useRouter } from "next/navigation";
import type { ArbitrageOpportunity } from "../lib/types";

interface OpportunityTableProps {
  opportunities: ArbitrageOpportunity[];
}

export default function OpportunityTable({ opportunities }: OpportunityTableProps) {
  const router = useRouter();

  if (opportunities.length === 0) {
    return (
      <div className="text-center py-12 text-gray-500">
        No opportunities found. Adjust filters or check Supabase RLS policies.
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-gray-800 text-gray-400 text-left">
            <th className="py-2 pr-3 font-medium">Sport</th>
            <th className="py-2 pr-3 font-medium">Market</th>
            <th className="py-2 pr-3 font-medium">Outcome</th>
            <th className="py-2 pr-3 font-medium text-right">ROI</th>
            <th className="py-2 pr-3 font-medium text-right">Net Profit</th>
            <th className="py-2 pr-3 font-medium">Buy YES</th>
            <th className="py-2 pr-3 font-medium">Buy NO</th>
            <th className="py-2 pr-3 font-medium text-right">Depth</th>
            <th className="py-2 pr-3 font-medium">Status</th>
            <th className="py-2 font-medium text-center">Verified</th>
          </tr>
        </thead>
        <tbody>
          {opportunities.map((opp) => {
            const borderColor = getBorderColor(opp);
            const rowOpacity = opp.status === "expired" ? "opacity-50" : "";

            return (
              <tr
                key={opp.id}
                onClick={() => router.push(`/opportunity/${opp.id}`)}
                className={`border-b border-gray-800/50 hover:bg-gray-800/50 cursor-pointer border-l-4 ${borderColor} ${rowOpacity}`}
              >
                <td className="py-2.5 pr-3 uppercase font-medium text-xs">
                  {opp.sport}
                </td>
                <td className="py-2.5 pr-3 text-gray-300">
                  {opp.market_type}
                </td>
                <td className="py-2.5 pr-3 max-w-48 truncate" title={opp.outcome}>
                  {opp.outcome}
                </td>
                <td className={`py-2.5 pr-3 text-right font-mono font-semibold ${getRoiColor(opp.roi)}`}>
                  {(opp.roi * 100).toFixed(2)}%
                </td>
                <td className="py-2.5 pr-3 text-right font-mono">
                  ${(opp.net_profit * 100).toFixed(0)}
                </td>
                <td className="py-2.5 pr-3 text-gray-300">
                  <span className="capitalize">{opp.buy_yes_platform}</span>{" "}
                  <span className="font-mono">
                    @{opp.buy_yes_price.toFixed(2)}
                  </span>
                  {opp.buy_yes_midpoint != null && (
                    <span className="text-gray-500 text-xs ml-1">
                      (mid {opp.buy_yes_midpoint.toFixed(2)})
                    </span>
                  )}
                </td>
                <td className="py-2.5 pr-3 text-gray-300">
                  <span className="capitalize">{opp.buy_no_platform}</span>{" "}
                  <span className="font-mono">
                    @{opp.buy_no_price.toFixed(2)}
                  </span>
                  {opp.buy_no_midpoint != null && (
                    <span className="text-gray-500 text-xs ml-1">
                      (mid {opp.buy_no_midpoint.toFixed(2)})
                    </span>
                  )}
                </td>
                <td className="py-2.5 pr-3 text-right font-mono text-gray-400">
                  {opp.max_executable_size != null
                    ? `$${opp.max_executable_size.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
                    : "-"}
                </td>
                <td className="py-2.5 pr-3">
                  <StatusBadge status={opp.status} />
                </td>
                <td className="py-2.5 text-center">
                  {opp.liquidity_verified ? (
                    <span className="text-green-400" title="Liquidity verified">
                      &#10003;
                    </span>
                  ) : (
                    <span className="text-gray-600">-</span>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function getBorderColor(opp: ArbitrageOpportunity): string {
  if (opp.status === "expired") return "border-gray-700";
  if (opp.liquidity_verified && opp.roi > 0.02) return "border-green-500";
  if (opp.liquidity_verified) return "border-green-700";
  return "border-yellow-600";
}

function getRoiColor(roi: number): string {
  if (roi > 0.02) return "text-green-400";
  if (roi > 0.005) return "text-yellow-400";
  return "text-gray-300";
}

function StatusBadge({ status }: { status: string }) {
  const colors =
    status === "active"
      ? "bg-green-900/50 text-green-400 border-green-800"
      : "bg-gray-800 text-gray-500 border-gray-700";

  return (
    <span
      className={`inline-block px-2 py-0.5 rounded text-xs border ${colors}`}
    >
      {status}
    </span>
  );
}
