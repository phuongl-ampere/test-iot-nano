"use client";

import { AppWindow, BellRing, Boxes, Cpu, LayoutDashboard, Settings2, Users } from "lucide-react";
import Link from "next/link";
import { useState } from "react";
import { useRouter } from "next/navigation";

import { logout } from "../lib/api";
import { ManagementPanel } from "./management-panels";
import { ManagementAlerts } from "./management-alerts";
import { PortalGate } from "./portal-gate";
import { ProfileMenu } from "./profile-menu";
import { SystemConfigurationPanel } from "./system-configuration";
import { UserProfilePanel } from "./user-profile";

export type ManagementSection =
  | "overview"
  | "settings"
  | "devices"
  | "assets"
  | "apps"
  | "alerts"
  | "users"
  | "device-profiles"
  | "asset-profiles";

type ManagementAppProps = {
  section: ManagementSection;
};

export const managementNavigation: Array<{ href: string; label: string; section: ManagementSection; icon: typeof LayoutDashboard }> = [
  { href: "/management", label: "Overview", section: "overview", icon: LayoutDashboard },
  { href: "/management/settings", label: "Settings", section: "settings", icon: Settings2 },
  { href: "/management/entities/devices", label: "Devices", section: "devices", icon: Cpu },
  { href: "/management/entities/assets", label: "Assets", section: "assets", icon: Boxes },
  { href: "/management/apps", label: "Apps", section: "apps", icon: AppWindow },
  { href: "/management/alerts", label: "Alerts & Notifications", section: "alerts", icon: BellRing },
  { href: "/management/users", label: "Users", section: "users", icon: Users },
  { href: "/management/profiles/device-profiles", label: "Device profiles", section: "device-profiles", icon: Cpu },
  { href: "/management/profiles/asset-profiles", label: "Asset profiles", section: "asset-profiles", icon: Boxes },
];

export function ManagementApp({ section }: ManagementAppProps) {
  const router = useRouter();
  const [profileOpen, setProfileOpen] = useState(false);

  return (
    <PortalGate>
      {(session, onUnauthorized) => {
        const signOut = () => {
          void logout(session.client).catch(() => undefined);
          onUnauthorized();
          router.replace("/");
        };
        if (profileOpen) {
          return (
            <main className="system-shell">
              <UserProfilePanel
                client={session.client}
                onBack={() => setProfileOpen(false)}
                onPasswordChanged={() => {
                  onUnauthorized();
                  router.replace("/");
                }}
                onUnauthorized={onUnauthorized}
                role={session.user.role}
              />
            </main>
          );
        }
        return (
          <main className="management-shell">
            <aside className="management-nav">
              <div className="brand-lockup">
                <span className="brand-mark"><Settings2 aria-hidden="true" size={18} /></span>
                <span><strong>Rush IoT Nano</strong><small>Management</small></span>
              </div>
              <nav aria-label="Management navigation">
                {managementNavigation
                  .filter((item) =>
                    session.user.accountClass === "system"
                      ? item.section === "settings"
                      : item.section !== "settings",
                  )
                  .map(({ href, icon: Icon, label, section: target }) => (
                  <Link className={target === section ? "is-active" : ""} href={href} key={href}>
                    <Icon aria-hidden="true" size={16} />
                    {label}
                  </Link>
                  ))}
              </nav>
            </aside>
            <section className="management-workspace">
              <header className="workspace-header management-header">
                <div><span className="eyebrow">Platform management</span><h1>{managementNavigation.find((item) => item.section === section)?.label}</h1></div>
                <ProfileMenu
                  onLogout={signOut}
                  onOpenProfile={() => setProfileOpen(true)}
                  onOpenSystemConfiguration={() => router.push("/management/settings")}
                  accountClass={session.user.accountClass}
                  role={session.user.role}
                />
              </header>
              {section === "settings" ? (
                <SystemConfigurationPanel
                  client={session.client}
                  onBack={() => router.push("/management")}
                  onUnauthorized={onUnauthorized}
                />
              ) : section === "alerts" ? (
                <ManagementAlerts client={session.client} onUnauthorized={onUnauthorized} />
              ) : (
                <ManagementPanel client={session.client} onUnauthorized={onUnauthorized} section={section} />
              )}
            </section>
          </main>
        );
      }}
    </PortalGate>
  );
}
