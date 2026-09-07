import type { UserSession } from "./api";

export const powerMonitorPath = "/apps/powermonitor";

export function defaultAppPath(user: UserSession): string {
  if (
    user.defaultApp === powerMonitorPath
    && user.grantedApps.includes("powermonitor")
  ) {
    return powerMonitorPath;
  }
  return powerMonitorPath;
}

export function rootPath(user: UserSession): string {
  return user.role === "admin" ? "/management" : defaultAppPath(user);
}

export function canAccessPath(user: UserSession, path: string): boolean {
  if (path === "/management" || path.startsWith("/management/")) {
    return user.role === "admin";
  }
  if (path === powerMonitorPath || path.startsWith(`${powerMonitorPath}/`)) {
    return user.grantedApps.includes("powermonitor");
  }
  return true;
}
