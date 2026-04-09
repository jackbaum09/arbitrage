"use client";

import { useState, useEffect } from "react";
import { useParams, useRouter } from "next/navigation";
import type { ArbitrageOpportunity, PriceHistoryPoint } from "../../../lib/types";
import { fetchOpportunityById, fetchPriceHistory } from "../../../lib/queries";
import PriceHistoryChart from "../../../components/PriceHistoryChart";

export default function OpportunityDetailPage() {
  const params = useParams();
  const router = useRouter();
  const id = Number(params.id);

  const [opp, setOpp] = useState<ArbitrageOpportunity | null>(null);
  const [history, setHistory] = useState<PriceHistoryPoint[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      setLoading(true);
      const data = await fetchOpportunityById(id);
      setOpp(data);
      if (data) {
        const hist = await fetchPriceHistory(data.opportunity_key);
        setHistory(hist);
      }
      setLoading(false);
    }
    if (id) load();
  }, [id]);

  if (loading) {
    return <p className="text-gray-500 py-12 text-center">Loading...</p>;
  }

  if (!opp) {
    return <p className="text-gray-500 py-12 text-center">Opportunity not found.</p>;
  }

  return (
    <div className="max-w-4xl mx-auto">
      {/* Back + Title */}
      <button
        onClick={() => router.push("/")}
        className="text-sm text-gray-400 hover:text-gray-200 mb-4 inline-block"
      >
        &larr; Back to dashboard
      </button>

      <div className="flex items-start justify-between mb-6">
        <div>
          <h2 className="text-xl font-semibold">{opp.outcome}</h2>
          <p className="text-gray-400 text-sm mt-1">
            {opp.sport.toUpperCase()} &middot; {opp.market_type}
          </p>
        </div>
        <div className="flex gap-2">
          <StatusBadge status={opp.status} />
          {opp.liquidity_verified && (
            <span className="inline-block px-2 py-0.5 rounded text-xs border bg-green-900/50 text-green-400 border-green-800">
              Verified
            </span>
          )}
        </div>
      </div>

      {/* Platform comparison */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
        <PlatformCard
          side="Buy YES"
          platform={opp.buy_yes_platform}
          price={opp.buy_yes_price}
          midpoint={opp.buy_yes_midpoint}
          executable={opp.buy_yes_executable_price}
          depth={opp.buy_yes_depth}
        />
        <PlatformCard
          side="Buy NO"
          platform={opp.buy_no_platform}
          price={opp.buy_no_price}
          midpoint={opp.buy_no_midpoint}
          executable={opp.buy_no_executable_price}
          depth={opp.buy_no_depth}
        />
      </div>

      {/* Fee breakdown */}
      <div className="rounded-lg bg-gray-900 border border-gray-800 p-5 mb-6">
        <h3 className="text-sm font-medium text-gray-400 uppercase tracking-wide mb-3">
          Fee Breakdown
        </h3>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <Stat label="Total Cost" value={`$${(opp.total_cost * 100).toFixed(2)}`} />
          <Stat label="Gross Profit" value={`$${(opp.gross_profit * 100).toFixed(2)}`} />
          <Stat label="Fees" value={`$${(opp.fees * 100).toFixed(2)}`} />
          <Stat
            label="Net Profit"
            value={`$${(opp.net_profit * 100).toFixed(2)}`}
            accent="green"
          />
          <Stat
            label="ROI"
            value={`${(opp.roi * 100).toFixed(2)}%`}
            accent={opp.roi > 0.02 ? "green" : "yellow"}
          />
          <Stat label="Capital Required" value={`$${opp.capital_required.toFixed(2)}`} />
          <Stat
            label="Max Executable Size"
            value={
              opp.max_executable_size != null
                ? `$${opp.max_executable_size.toLocaleString(undefined, { maximumFractionDigits: 0 })}`
                : "N/A"
            }
          />
          <Stat
            label="Detected"
            value={new Date(opp.detected_at).toLocaleString(undefined, {
              month: "short",
              day: "numeric",
              hour: "2-digit",
              minute: "2-digit",
            })}
          />
        </div>
      </div>

      {/* Price History */}
      <div className="rounded-lg bg-gray-900 border border-gray-800 p-5">
        <h3 className="text-sm font-medium text-gray-400 uppercase tracking-wide mb-3">
          Price History (Last 7 Days)
        </h3>
        <PriceHistoryChart history={history} />
      </div>
    </div>
  );
}

function PlatformCard({
  side,
  platform,
  price,
  midpoint,
  executable,
  depth,
}: {
  side: string;
  platform: string;
  price: number;
  midpoint: number | null;
  executable: number | null;
  depth: number | null;
}) {
  const execWorse = executable != null && midpoint != null && executable > midpoint;

  return (
    <div className="rounded-lg bg-gray-900 border border-gray-800 p-5">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-gray-400 uppercase tracking-wide">
          {side}
        </h3>
        <span className="capitalize text-sm font-medium">{platform}</span>
      </div>
      <div className="space-y-2 text-sm">
        <div className="flex justify-between">
          <span className="text-gray-400">Current Price</span>
          <span className="font-mono font-semibold">${price.toFixed(4)}</span>
        </div>
        {midpoint != null && (
          <div className="flex justify-between">
            <span className="text-gray-400">Midpoint</span>
            <span className="font-mono text-gray-300">
              ${midpoint.toFixed(4)}
            </span>
          </div>
        )}
        {executable != null && (
          <div className="flex justify-between">
            <span className="text-gray-400">Executable (VWAP)</span>
            <span
              className={`font-mono ${execWorse ? "text-red-400" : "text-green-400"}`}
            >
              ${executable.toFixed(4)}
              {execWorse && (
                <span className="text-xs ml-1">(+slippage)</span>
              )}
            </span>
          </div>
        )}
        {depth != null && (
          <div className="flex justify-between">
            <span className="text-gray-400">Depth</span>
            <span className="font-mono text-gray-300">
              ${depth.toLocaleString(undefined, { maximumFractionDigits: 0 })}
            </span>
          </div>
        )}
      </div>
    </div>
  );
}

function Stat({
  label,
  value,
  accent,
}: {
  label: string;
  value: string;
  accent?: "green" | "yellow";
}) {
  const color =
    accent === "green"
      ? "text-green-400"
      : accent === "yellow"
        ? "text-yellow-400"
        : "text-gray-100";

  return (
    <div>
      <p className="text-xs text-gray-500">{label}</p>
      <p className={`font-mono font-medium ${color}`}>{value}</p>
    </div>
  );
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
