export interface ArbitrageOpportunity {
  id: number;
  opportunity_key: string;
  sport: string;
  market_type: string;
  outcome: string;
  buy_yes_platform: string;
  buy_yes_price: number;
  buy_no_platform: string;
  buy_no_price: number;
  total_cost: number;
  gross_profit: number;
  fees: number;
  net_profit: number;
  roi: number;
  kalshi_volume: number | null;
  polymarket_liquidity: number | null;
  capital_required: number;
  source_table: string;
  detected_at: string;
  expired_at: string | null;
  status: "active" | "expired";
  buy_yes_executable_price: number | null;
  buy_no_executable_price: number | null;
  buy_yes_midpoint: number | null;
  buy_no_midpoint: number | null;
  buy_yes_depth: number | null;
  buy_no_depth: number | null;
  max_executable_size: number | null;
  liquidity_verified: boolean;
  kalshi_market_id: string | null;
  polymarket_market_id: string | null;
}

export interface PriceHistoryPoint {
  id: number;
  opportunity_key: string;
  snapshot_at: string;
  buy_yes_price: number | null;
  buy_no_price: number | null;
  buy_yes_executable: number | null;
  buy_no_executable: number | null;
  buy_yes_depth: number | null;
  buy_no_depth: number | null;
  roi: number | null;
  roi_executable: number | null;
}

export interface DashboardFilters {
  sport: string;
  status: "active" | "expired" | "all";
  verifiedOnly: boolean;
  sortBy: "roi" | "net_profit" | "detected_at";
  sortDir: "asc" | "desc";
}
