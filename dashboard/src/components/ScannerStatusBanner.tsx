"use client";

import type { ScannerRun } from "../lib/types";

interface ScannerStatusBannerProps {
  latestRun: ScannerRun | null;
}

function minutesAgo(iso: string): number {
  return Math.round((Date.now() - new Date(iso).getTime()) / 60_000);
}

export default function ScannerStatusBanner({ latestRun }: ScannerStatusBannerProps) {
  if (!latestRun) {
    return (
      <div className="mb-4 rounded-lg border border-gray-700 bg-gray-900 px-4 py-3">
        <p className="text-sm text-gray-400">
          Scanner status: <span className="text-gray-300">no runs recorded yet</span>
        </p>
      </div>
    );
  }

  const age = minutesAgo(latestRun.finished_at);
  // Scanner runs every 5 min during active hours. Warn if >10 min,
  // critical if >30 min AND within active hours.
  const stale = age > 10;
  const critical = age > 30;
  const errored = latestRun.status === "error";

  let barColor = "border-green-800 bg-green-950/40";
  let dotColor = "bg-green-400";
  let label = "healthy";
  if (errored) {
    barColor = "border-red-800 bg-red-950/40";
    dotColor = "bg-red-400";
    label = "error";
  } else if (critical) {
    barColor = "border-red-800 bg-red-950/40";
    dotColor = "bg-red-400";
    label = "stale";
  } else if (stale) {
    barColor = "border-yellow-800 bg-yellow-950/40";
    dotColor = "bg-yellow-400";
    label = "slow";
  }

  return (
    <div className={`mb-4 rounded-lg border px-4 py-3 ${barColor}`}>
      <div className="flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-3">
          <span className={`inline-block h-2 w-2 rounded-full ${dotColor}`} />
          <p className="text-sm text-gray-200">
            Scanner <span className="font-semibold">{label}</span>
            <span className="text-gray-400">
              {" "}
              · last run {age === 0 ? "just now" : `${age} min ago`}
            </span>
          </p>
        </div>
        <div className="flex items-center gap-4 text-xs text-gray-400">
          <span>
            {latestRun.opportunities_found} opps
            {latestRun.opportunities_verified > 0 && (
              <span className="text-green-400">
                {" "}
                ({latestRun.opportunities_verified} verified)
              </span>
            )}
          </span>
          {latestRun.max_roi !== null && latestRun.opportunities_found > 0 && (
            <span>max ROI {(latestRun.max_roi * 100).toFixed(2)}%</span>
          )}
          {latestRun.duration_seconds !== null && (
            <span>{latestRun.duration_seconds.toFixed(1)}s</span>
          )}
        </div>
      </div>
      {errored && latestRun.error_message && (
        <p className="mt-2 text-xs text-red-300 font-mono truncate">
          {latestRun.error_message}
        </p>
      )}
    </div>
  );
}
