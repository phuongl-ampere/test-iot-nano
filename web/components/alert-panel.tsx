"use client";

import { BellRing, Check, Pencil, Plus, Trash2, X } from "lucide-react";
import { FormEvent, useState } from "react";

import {
  type AlertComparison,
  type AlertIncident,
  type AlertRule,
  type AlertRuleType,
  type AlertSeverity,
  type ApiClient,
  type CreateAlertRule,
  type Role,
  UnauthorizedApiError,
  acknowledgeAlertIncident,
  archiveAlertRule,
  createAlertRule,
  toggleAlertRule,
  updateAlertRule,
} from "../lib/api";
import { browserDateTime } from "../lib/time";

type AlertPanelProps = {
  client: ApiClient;
  incidents: AlertIncident[];
  onRefresh: () => void;
  onUnauthorized(): void;
  role: Role;
  rules: AlertRule[];
};

type RuleForm = {
  comparison: AlertComparison;
  deviceId: string;
  forSeconds: number;
  hysteresis: number | undefined;
  metricKey: string;
  name: string;
  reminderIntervalSeconds: number;
  reopenGraceSeconds: number;
  resolveAfterSeconds: number;
  ruleType: AlertRuleType;
  severity: AlertSeverity;
  threshold: string;
  windowSeconds: string;
};

const initialForm: RuleForm = {
  comparison: "gt",
  deviceId: "",
  forSeconds: 300,
  hysteresis: undefined,
  metricKey: "temperature_c",
  name: "",
  reminderIntervalSeconds: 86400,
  reopenGraceSeconds: 3600,
  resolveAfterSeconds: 300,
  ruleType: "event_threshold",
  severity: "warning",
  threshold: "40",
  windowSeconds: "300",
};

function formFromRule(rule: AlertRule): RuleForm {
  return {
    comparison: rule.comparison,
    deviceId: rule.device_id ?? "",
    forSeconds: rule.for_seconds,
    hysteresis: rule.hysteresis ?? undefined,
    metricKey: rule.metric_key,
    name: rule.name,
    reminderIntervalSeconds: rule.reminder_interval_seconds,
    reopenGraceSeconds: rule.reopen_grace_seconds,
    resolveAfterSeconds: rule.resolve_after_seconds,
    ruleType: rule.rule_type,
    severity: rule.severity,
    threshold: String(rule.threshold),
    windowSeconds: String(rule.window_seconds ?? 300),
  };
}

function payloadFromForm(form: RuleForm): CreateAlertRule {
  const threshold = Number(form.threshold);
  const windowSeconds = Number(form.windowSeconds);
  return {
    name: form.name,
    device_id: form.deviceId.trim() || undefined,
    metric_key: form.metricKey,
    rule_type: form.ruleType,
    comparison: form.comparison,
    threshold,
    window_seconds: form.ruleType === "window_average" ? windowSeconds : undefined,
    for_seconds: form.forSeconds,
    resolve_after_seconds: form.resolveAfterSeconds,
    reopen_grace_seconds: form.reopenGraceSeconds,
    hysteresis: form.hysteresis,
    severity: form.severity,
    reminder_interval_seconds: form.reminderIntervalSeconds,
  };
}

function shortDate(value: string | null) {
  return browserDateTime(value);
}

export function AlertPanel({
  client,
  incidents,
  onRefresh,
  onUnauthorized,
  role,
  rules,
}: AlertPanelProps) {
  const [form, setForm] = useState<RuleForm>(initialForm);
  const [submitting, setSubmitting] = useState(false);
  const [actionId, setActionId] = useState<string | null>(null);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const canWrite = role === "admin";

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const threshold = Number(form.threshold);
    const windowSeconds = Number(form.windowSeconds);
    if (!Number.isFinite(threshold)) {
      setError("Threshold must be a number.");
      return;
    }
    if (form.ruleType === "window_average" && (!Number.isInteger(windowSeconds) || windowSeconds < 60)) {
      setError("Window must be at least 60 seconds.");
      return;
    }

    setSubmitting(true);
    setError(null);
    try {
      const payload = payloadFromForm(form);
      if (editingId === null) {
        await createAlertRule(client, payload);
      } else {
        await updateAlertRule(client, editingId, payload);
      }
      setForm(initialForm);
      setEditingId(null);
      onRefresh();
    } catch (actionError) {
      if (actionError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setError(actionError instanceof Error ? actionError.message : "Rule update failed.");
    } finally {
      setSubmitting(false);
    }
  };

  const beginEdit = (rule: AlertRule) => {
    setEditingId(rule.id);
    setError(null);
    setForm(formFromRule(rule));
  };

  const cancelEdit = () => {
    setEditingId(null);
    setError(null);
    setForm(initialForm);
  };

  const toggle = async (rule: AlertRule) => {
    setActionId(rule.id);
    setError(null);
    try {
      await toggleAlertRule(client, rule.id, !rule.enabled);
      onRefresh();
    } catch (actionError) {
      if (actionError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setError(actionError instanceof Error ? actionError.message : "Rule update failed.");
    } finally {
      setActionId(null);
    }
  };

  const archive = async (rule: AlertRule) => {
    if (!window.confirm(`Archive ${rule.name}? Active incidents will resolve.`)) {
      return;
    }
    setActionId(rule.id);
    setError(null);
    try {
      await archiveAlertRule(client, rule.id);
      if (editingId === rule.id) {
        cancelEdit();
      }
      onRefresh();
    } catch (actionError) {
      if (actionError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setError(actionError instanceof Error ? actionError.message : "Rule archive failed.");
    } finally {
      setActionId(null);
    }
  };

  const acknowledge = async (incident: AlertIncident) => {
    setActionId(incident.id);
    setError(null);
    try {
      await acknowledgeAlertIncident(client, incident.id);
      onRefresh();
    } catch (actionError) {
      if (actionError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setError(actionError instanceof Error ? actionError.message : "Incident acknowledge failed.");
    } finally {
      setActionId(null);
    }
  };

  return (
    <section className="alert-section" aria-label="Alert rules and incidents">
      <div className="section-heading">
        <div>
          <span className="eyebrow">Signal policy</span>
          <h2>Alerts</h2>
        </div>
        <span>{incidents.filter((incident) => incident.status === "open").length} open</span>
      </div>

      {canWrite && (
        <form className="alert-form" onSubmit={(event) => void submit(event)}>
          <label>
            <span>Rule name</span>
            <input
              aria-label="Rule name"
              onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))}
              required
              value={form.name}
            />
          </label>
          <label>
            <span>Metric</span>
            <input
              aria-label="Metric"
              onChange={(event) => setForm((current) => ({ ...current, metricKey: event.target.value }))}
              value={form.metricKey}
            />
          </label>
          <label>
            <span>Condition</span>
            <select
              aria-label="Condition"
              onChange={(event) =>
                setForm((current) => ({ ...current, comparison: event.target.value as AlertComparison }))
              }
              value={form.comparison}
            >
              <option value="gt">greater than</option>
              <option value="gte">at least</option>
              <option value="lt">less than</option>
              <option value="lte">at most</option>
            </select>
          </label>
          <label>
            <span>Threshold</span>
            <input
              aria-label="Threshold"
              inputMode="decimal"
              onChange={(event) => setForm((current) => ({ ...current, threshold: event.target.value }))}
              value={form.threshold}
            />
          </label>
          <label>
            <span>Evaluation</span>
            <select
              aria-label="Evaluation"
              onChange={(event) =>
                setForm((current) => ({ ...current, ruleType: event.target.value as AlertRuleType }))
              }
              value={form.ruleType}
            >
              <option value="event_threshold">each event</option>
              <option value="window_average">window average</option>
            </select>
          </label>
          {form.ruleType === "window_average" && (
            <label>
              <span>Window seconds</span>
              <input
                aria-label="Window seconds"
                inputMode="numeric"
                onChange={(event) => setForm((current) => ({ ...current, windowSeconds: event.target.value }))}
                value={form.windowSeconds}
              />
            </label>
          )}
          <label>
            <span>Severity</span>
            <select
              aria-label="Severity"
              onChange={(event) =>
                setForm((current) => ({ ...current, severity: event.target.value as AlertSeverity }))
              }
              value={form.severity}
            >
              <option value="info">info</option>
              <option value="warning">warning</option>
              <option value="critical">critical</option>
            </select>
          </label>
          <button className="alert-create" disabled={submitting} type="submit">
            {editingId === null ? <Plus aria-hidden="true" size={15} /> : <Check aria-hidden="true" size={15} />}
            {editingId === null ? "Create rule" : "Save changes"}
          </button>
          {editingId !== null && (
            <button
              aria-label="Cancel rule edit"
              className="alert-cancel"
              onClick={cancelEdit}
              title="Cancel rule edit"
              type="button"
            >
              <X aria-hidden="true" size={15} />
            </button>
          )}
        </form>
      )}

      {error !== null && <p className="alert-action-error" role="alert">{error}</p>}

      <div className="alert-grid">
        <section aria-label="Alert rules">
          <div className="alert-subheading">
            <BellRing aria-hidden="true" size={15} />
            <strong>Rules</strong>
          </div>
          <div className="alert-table-wrap">
            <table className="alert-table">
              <thead>
                <tr>
                  <th>Enabled</th>
                  <th>Rule</th>
                  <th>Condition</th>
                  <th>Scope</th>
                  <th>Severity</th>
                  {canWrite && <th />}
                </tr>
              </thead>
              <tbody>
                {rules.map((rule) => (
                  <tr key={rule.id}>
                    <td>
                      {canWrite ? (
                        <input
                          aria-label={`Enable ${rule.name}`}
                          checked={rule.enabled}
                          disabled={actionId === rule.id}
                          onChange={() => void toggle(rule)}
                          type="checkbox"
                        />
                      ) : (
                        <span>{rule.enabled ? "enabled" : "disabled"}</span>
                      )}
                    </td>
                    <td>{rule.name}</td>
                    <td>
                      {rule.metric_key} {rule.comparison} {rule.threshold}
                      {rule.rule_type === "window_average" ? ` / ${rule.window_seconds}s avg` : ""}
                    </td>
                    <td>{rule.device_id ?? "Fleet"}</td>
                    <td><span className={`severity severity-${rule.severity}`}>{rule.severity}</span></td>
                    {canWrite && (
                      <td className="alert-rule-actions">
                        <button
                          aria-label={`Edit ${rule.name}`}
                          className="alert-icon-action"
                          disabled={actionId === rule.id}
                          onClick={() => beginEdit(rule)}
                          title="Edit rule"
                          type="button"
                        >
                          <Pencil aria-hidden="true" size={14} />
                        </button>
                        <button
                          aria-label={`Delete ${rule.name}`}
                          className="alert-icon-action alert-delete"
                          disabled={actionId === rule.id}
                          onClick={() => void archive(rule)}
                          title="Archive rule"
                          type="button"
                        >
                          <Trash2 aria-hidden="true" size={14} />
                        </button>
                      </td>
                    )}
                  </tr>
                ))}
                {rules.length === 0 && (
                  <tr><td className="table-empty" colSpan={canWrite ? 6 : 5}>No alert rules.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section aria-label="Alert incidents">
          <div className="alert-subheading">
            <BellRing aria-hidden="true" size={15} />
            <strong>Incidents</strong>
          </div>
          <div className="alert-table-wrap">
            <table className="alert-table">
              <thead>
                <tr>
                  <th>Status</th>
                  <th>Rule</th>
                  <th>Device</th>
                  <th>Value</th>
                  <th>Updated</th>
                  {canWrite && <th />}
                </tr>
              </thead>
              <tbody>
                {incidents.map((incident) => (
                  <tr key={incident.id}>
                    <td><span className={`incident-status incident-${incident.status}`}>{incident.status}</span></td>
                    <td>{incident.rule_name}</td>
                    <td>{incident.device_id}</td>
                    <td>{incident.last_value ?? "—"}</td>
                    <td>{shortDate(incident.updated_at)}</td>
                    {canWrite && (
                      <td>
                        {incident.status === "open" && incident.acknowledged_at === null && (
                          <button
                            aria-label={`Acknowledge ${incident.rule_name}`}
                            className="alert-acknowledge"
                            disabled={actionId === incident.id}
                            onClick={() => void acknowledge(incident)}
                            title="Acknowledge incident"
                            type="button"
                          >
                            <Check aria-hidden="true" size={15} />
                          </button>
                        )}
                      </td>
                    )}
                  </tr>
                ))}
                {incidents.length === 0 && (
                  <tr><td className="table-empty" colSpan={canWrite ? 6 : 5}>No incidents.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </section>
  );
}
