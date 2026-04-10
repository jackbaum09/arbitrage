import { supabase } from "./supabase";
import type {
  ArbitrageOpportunity,
  PriceHistoryPoint,
  DashboardFilters,
  ScannerRun,
} from "./types";

function numericRow(row: Record<string, unknown>): Record<string, unknown> {
  const numericFields = [
    "buy_yes_price", "buy_no_price", "total_cost", "gross_profit",
    "fees", "net_profit", "roi", "kalshi_volume", "polymarket_liquidity",
    "capital_required", "buy_yes_executable_price", "buy_no_executable_price",
    "buy_yes_midpoint", "buy_no_midpoint", "buy_yes_depth", "buy_no_depth",
    "max_executable_size",
  ];
  const out = { ...row };
  for (const f of numericFields) {
    if (out[f] !== null && out[f] !== undefined) {
      out[f] = Number(out[f]);
    }
  }
  return out;
}

export async function fetchOpportunities(
  filters: DashboardFilters
): Promise<ArbitrageOpportunity[]> {
  let query = supabase
    .from("arbitrage_opportunities")
    .select("*")
    .order(filters.sortBy, { ascending: filters.sortDir === "asc" })
    .limit(200);

  if (filters.status !== "all") {
    query = query.eq("status", filters.status);
  }
  if (filters.sport !== "all") {
    query = query.eq("sport", filters.sport);
  }
  if (filters.verifiedOnly) {
    query = query.eq("liquidity_verified", true);
  }

  const { data, error } = await query;
  if (error) {
    console.error("Failed to fetch opportunities:", error);
    return [];
  }
  return (data || []).map((r) => numericRow(r) as unknown as ArbitrageOpportunity);
}

export async function fetchStats(): Promise<ArbitrageOpportunity[]> {
  const { data, error } = await supabase
    .from("arbitrage_opportunities")
    .select("roi, capital_required, sport, liquidity_verified")
    .eq("status", "active");

  if (error) {
    console.error("Failed to fetch stats:", error);
    return [];
  }
  return (data || []).map((r) => numericRow(r) as unknown as ArbitrageOpportunity);
}

export async function fetchOpportunityById(
  id: number
): Promise<ArbitrageOpportunity | null> {
  const { data, error } = await supabase
    .from("arbitrage_opportunities")
    .select("*")
    .eq("id", id)
    .single();

  if (error) {
    console.error("Failed to fetch opportunity:", error);
    return null;
  }
  return numericRow(data) as unknown as ArbitrageOpportunity;
}

export async function fetchPriceHistory(
  opportunityKey: string
): Promise<PriceHistoryPoint[]> {
  const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();

  const { data, error } = await supabase
    .from("opportunity_price_history")
    .select("*")
    .eq("opportunity_key", opportunityKey)
    .gte("snapshot_at", sevenDaysAgo)
    .order("snapshot_at", { ascending: true })
    .limit(500);

  if (error) {
    console.error("Failed to fetch price history:", error);
    return [];
  }

  return (data || []).map((r) => {
    const out = { ...r };
    for (const f of ["buy_yes_price", "buy_no_price", "buy_yes_executable", "buy_no_executable", "buy_yes_depth", "buy_no_depth", "roi", "roi_executable"]) {
      if (out[f] !== null && out[f] !== undefined) {
        out[f] = Number(out[f]);
      }
    }
    return out as PriceHistoryPoint;
  });
}

export async function fetchLatestScanRun(): Promise<ScannerRun | null> {
  const { data, error } = await supabase
    .from("scanner_runs")
    .select("*")
    .order("finished_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    console.error("Failed to fetch latest scan run:", error);
    return null;
  }
  if (!data) return null;

  const out = { ...data };
  for (const f of ["duration_seconds", "max_roi"]) {
    if (out[f] !== null && out[f] !== undefined) {
      out[f] = Number(out[f]);
    }
  }
  return out as ScannerRun;
}

export async function fetchRecentScanRuns(limit = 20): Promise<ScannerRun[]> {
  const { data, error } = await supabase
    .from("scanner_runs")
    .select("*")
    .order("finished_at", { ascending: false })
    .limit(limit);

  if (error) {
    console.error("Failed to fetch recent scan runs:", error);
    return [];
  }
  return (data || []).map((r) => {
    const out = { ...r };
    for (const f of ["duration_seconds", "max_roi"]) {
      if (out[f] !== null && out[f] !== undefined) {
        out[f] = Number(out[f]);
      }
    }
    return out as ScannerRun;
  });
}
