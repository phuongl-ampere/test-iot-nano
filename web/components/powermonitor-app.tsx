"use client";

import { PowerMonitorDashboard } from "./powermonitor-dashboard";
import { PortalGate } from "./portal-gate";

type PowerMonitorAppProps = {
  initialDeviceId?: string;
};

export function PowerMonitorApp({ initialDeviceId }: PowerMonitorAppProps) {
  return (
    <PortalGate>
      {(session, onUnauthorized) => (
        <PowerMonitorDashboard
          client={session.client}
          initialDeviceId={initialDeviceId}
          onUnauthorized={onUnauthorized}
          accountClass={session.user.accountClass}
          role={session.user.role}
        />
      )}
    </PortalGate>
  );
}
