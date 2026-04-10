"use client";

import { useState, useEffect, useCallback } from "react";
import type { ArbitrageOpportunity, DashboardFilters, ScannerRun } from "../lib/types";
import { fetchOpportunities, fetchStats, fetchLatestScanRun } from "../lib/queries";
import StatsHeader from "../components/StatsHeader";
import Filters from "../components/Filters";
import OpportunityTable from "../components/OpportunityTable";
import ScannerStatusBanner from "../components/ScannerStatusBanner";

const DEFAULT_FILTERS: DashboardFilters = {
  sport: "all",
  status: "active",
  verifiedOnly: false,
  sortBy: "roi",
  sortDir: "desc",
};

export default function DashboardPage() {
  const [filters, setFilters] = useState<DashboardFilters>(DEFAULT_FILTERS);
  const [opportunities, setOpportunities] = useState<ArbitrageOpportunity[]>([]);
  const [stats, setStats] = useState<Pick<ArbitrageOpportunity, "roi" | "capital_required" | "sport" | "liquidity_verified">[]>([]);
  const [latestRun, setLatestRun] = useState<ScannerRun | null>(null);
  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);

  const loadData = useCallback(async () => {
    const [opps, statsData, run] = await Promise.all([
      fetchOpportunities(filters),
      fetchStats(),
      fetchLatestScanRun(),
    ]);
    setOpportunities(opps);
    setStats(statsData);
    setLatestRun(run);
    setLastRefresh(new Date());
    setLoading(false);
  }, [filters]);

  // React 19 introduces react-hooks/set-state-in-effect, which statically
  // flags setState reachable from an effect body. The canonical Next 16
  // alternative is a Server Component + router.refresh() for polling, but
  // we keep this client-side pattern because the page's filter state lives
  // here. The setState calls all land after awaits (microtask), so the
  // cascading-render concern the rule protects against doesn't apply.
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    loadData();
  }, [loadData]);

  // Auto-refresh every 60 seconds
  useEffect(() => {
    const interval = setInterval(loadData, 60_000);
    return () => clearInterval(interval);
  }, [loadData]);

  return (
    <div>
      <ScannerStatusBanner latestRun={latestRun} />
      <StatsHeader opportunities={stats} />
      <Filters filters={filters} onChange={setFilters} />

      <div className="flex items-center justify-between mb-2">
        <p className="text-xs text-gray-500">
          {opportunities.length} result{opportunities.length !== 1 ? "s" : ""}
          {loading && " \u2022 loading..."}
        </p>
        {lastRefresh && (
          <p className="text-xs text-gray-600">
            Last refresh: {lastRefresh.toLocaleTimeString()}
          </p>
        )}
      </div>

      <OpportunityTable opportunities={opportunities} />
    </div>
  );
}
