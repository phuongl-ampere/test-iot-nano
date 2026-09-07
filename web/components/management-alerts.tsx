"use client";

import { BellRing, Settings2 } from "lucide-react";
import Link from "next/link";
import { useCallback, useEffect, useState } from "react";

import {
  type AlertIncident,
  type AlertRule,
  type ApiClient,
  UnauthorizedApiError,
  fetchAlertIncidents,
  fetchAlertRules,
} from "../lib/api";
import { AlertPanel } from "./alert-panel";

export function ManagementAlerts({
  client,
  onUnauthorized,
}: {
  client: ApiClient;
  onUnauthorized(): void;
}) {
  const [rules, setRules] = useState<AlertRule[]>([]);
  const [incidents, setIncidents] = useState<AlertIncident[]>([]);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(() => {
    setError(null);
    void Promise.all([fetchAlertRules(client), fetchAlertIncidents(client)])
      .then(([nextRules, nextIncidents]) => {
        setRules(nextRules);
        setIncidents(nextIncidents);
      })
      .catch((reason) => {
        if (reason instanceof UnauthorizedApiError) {
          onUnauthorized();
          return;
        }
        setError(reason instanceof Error ? reason.message : "Alert request failed.");
      });
  }, [client, onUnauthorized]);

  useEffect(refresh, [refresh]);

  return (
    <section className="management-alerts">
      <header className="management-alerts-header">
        <div>
          <BellRing aria-hidden="true" size={17} />
          <span>{incidents.filter((incident) => incident.status === "open").length} open</span>
        </div>
        <Link href="/management/settings">
          <Settings2 aria-hidden="true" size={15} />
          Notification settings
        </Link>
      </header>
      {error !== null && <p className="system-error" role="alert">{error}</p>}
      <AlertPanel
        client={client}
        incidents={incidents}
        onRefresh={refresh}
        onUnauthorized={onUnauthorized}
        role="admin"
        rules={rules}
      />
    </section>
  );
}
