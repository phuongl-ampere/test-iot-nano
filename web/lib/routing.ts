import type { UserSession } from "./api";

export function rootPath(user: UserSession): string {
  if (user.accountClass === "system") {
    return "/management/settings";
  }
  if (user.accountClass === "admin") {
    return "/management";
  }
  return "/";
}

export function canAccessPath(user: UserSession, path: string): boolean {
  if (path === "/management" || path.startsWith("/management/")) {
    if (path === "/management/settings") {
      return user.accountClass === "system";
    }
    return user.accountClass === "admin";
  }
  if (path === "/apps" || path.startsWith("/apps/")) {
    return false;
  }
  return true;
}
