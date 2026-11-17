/**
 * GitHub recipes for the SDK.
 *
 * @example
 * import { prMonitor, prMonitorNotify } from '@clawdbot/lobster/recipes/github';
 *
 * const result = await prMonitor({ repo: 'owner/repo', pr: 123 }).run();
 */

export { prMonitor, prMonitorNotify } from "./pr_monitor.js";
export { ghPrView } from "./pr_view.js";
