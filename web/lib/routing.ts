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
  if (user.accountClass === "system") {
    return "/management/settings";
  }
  if (user.accountClass === "admin") {
    return "/management";
  }
  return defaultAppPath(user);
}

export function canAccessPath(user: UserSession, path: string): boolean {
  if (path === "/management" || path.startsWith("/management/")) {
    if (path === "/management/settings") {
      return user.accountClass === "system";
    }
    return user.accountClass === "admin";
  }
  if (path === powerMonitorPath || path.startsWith(`${powerMonitorPath}/`)) {
    return user.grantedApps.includes("powermonitor");
  }
  return true;
}
