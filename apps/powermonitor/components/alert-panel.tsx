import type { Alert } from "../lib/browser-api";

type AlertPanelProps = {
  alerts: Alert[];
  onAcknowledge(alertId: string): void;
  workingId: string | null;
};

export function AlertPanel({ alerts, onAcknowledge, workingId }: AlertPanelProps) {
  return (
    <section aria-label="Alerts" className="alert-panel">
      <header className="section-heading">
        <div>
          <span className="eyebrow">Attention required</span>
          <h2>Alerts</h2>
        </div>
        <span>{alerts.length} active</span>
      </header>
      {alerts.length === 0 ? (
        <p className="empty-state">No active alerts.</p>
      ) : (
        <ul className="alert-list">
          {alerts.map((alert) => (
            <li key={alert.id}>
              <div>
                <strong>{alert.message}</strong>
                <span>{alert.device_id ?? "Fleet"} · {alert.severity ?? "info"}</span>
              </div>
              {alert.status !== "acknowledged" && alert.status !== "resolved" && (
                <button
                  aria-label={"Acknowledge " + alert.message}
                  disabled={workingId === alert.id}
                  onClick={() => onAcknowledge(alert.id)}
                  type="button"
                >
                  {workingId === alert.id ? "Acknowledging" : "Acknowledge"}
                </button>
              )}
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
