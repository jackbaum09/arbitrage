"use client";

import { useState, useEffect, useCallback } from "react";
import type { ArbitrageOpportunity, DashboardFilters } from "../lib/types";
import { fetchOpportunities, fetchStats } from "../lib/queries";
import StatsHeader from "../components/StatsHeader";
import Filters from "../components/Filters";
import OpportunityTable from "../components/OpportunityTable";

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
  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);

  const loadData = useCallback(async () => {
    setLoading(true);
    const [opps, statsData] = await Promise.all([
      fetchOpportunities(filters),
      fetchStats(),
    ]);
    setOpportunities(opps);
    setStats(statsData);
    setLastRefresh(new Date());
    setLoading(false);
  }, [filters]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  // Auto-refresh every 60 seconds
  useEffect(() => {
    const interval = setInterval(loadData, 60_000);
    return () => clearInterval(interval);
  }, [loadData]);

  return (
    <div>
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
