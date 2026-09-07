"use client";

import { ArrowLeft, Save, Settings2 } from "lucide-react";
import { FormEvent, useEffect, useState } from "react";

import {
  type ApiClient,
  type IngestTuning,
  type MqttConfigurationUpdate,
  type SmtpConfigurationUpdate,
  type SystemConfiguration,
  type SystemConfigurationUpdate,
  UnauthorizedApiError,
  fetchSystemConfiguration,
  updateSystemConfiguration,
} from "../lib/api";

type SystemConfigurationPanelProps = {
  client: ApiClient;
  onBack(): void;
  onUnauthorized(): void;
};

function updateFromConfiguration(configuration: SystemConfiguration): SystemConfigurationUpdate {
  return {
    mqtt: configuration.mqtt ?? { host: "127.0.0.1", port: 1883 },
    smtp: {
      enabled: configuration.smtp.enabled,
      host: configuration.smtp.host,
      port: configuration.smtp.port,
      username: configuration.smtp.username,
      from: configuration.smtp.from,
      to: configuration.smtp.to,
      timeout_seconds: configuration.smtp.timeout_seconds,
    },
    tuning: configuration.tuning,
  };
}

function sameMqtt(left: MqttConfigurationUpdate, right: MqttConfigurationUpdate): boolean {
  return left.host === right.host && left.port === right.port;
}

function sameTuning(left: IngestTuning, right: IngestTuning): boolean {
  return (Object.keys(left) as Array<keyof IngestTuning>)
    .every((key) => left[key] === right[key]);
}

function sameSmtp(left: SmtpConfigurationUpdate, right: SmtpConfigurationUpdate): boolean {
  return left.enabled === right.enabled
    && left.host === right.host
    && left.port === right.port
    && left.username === right.username
    && left.from === right.from
    && left.to === right.to
    && left.timeout_seconds === right.timeout_seconds;
}

export function SystemConfigurationPanel({
  client,
  onBack,
  onUnauthorized,
}: SystemConfigurationPanelProps) {
  const [configuration, setConfiguration] = useState<SystemConfigurationUpdate | null>(null);
  const [appliedConfiguration, setAppliedConfiguration] =
    useState<SystemConfigurationUpdate | null>(null);
  const [passwordConfigured, setPasswordConfigured] = useState(false);
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [restartRequired, setRestartRequired] = useState(false);
  const [smtpAppliedLive, setSmtpAppliedLive] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    void fetchSystemConfiguration(client)
      .then((next) => {
        const nextConfiguration = updateFromConfiguration(next);
        setConfiguration(nextConfiguration);
        setAppliedConfiguration(nextConfiguration);
        setPasswordConfigured(next.smtp.password_configured);
        setRestartRequired(false);
        setSmtpAppliedLive(false);
      })
      .catch((loadError) => {
        if (loadError instanceof UnauthorizedApiError) {
          onUnauthorized();
          return;
        }
        setError(loadError instanceof Error ? loadError.message : "System configuration request failed.");
      })
      .finally(() => setLoading(false));
  }, [client, onUnauthorized]);

  const updateSmtp = <Key extends keyof SmtpConfigurationUpdate>(
    key: Key,
    value: SmtpConfigurationUpdate[Key],
  ) => {
    setConfiguration((current) => current === null
      ? current
      : { ...current, smtp: { ...current.smtp, [key]: value } });
  };

  const updateMqtt = <Key extends keyof MqttConfigurationUpdate>(
    key: Key,
    value: MqttConfigurationUpdate[Key],
  ) => {
    setConfiguration((current) => current === null
      ? current
      : { ...current, mqtt: { ...current.mqtt, [key]: value } });
  };

  const updateTuning = <Key extends keyof IngestTuning>(key: Key, value: number) => {
    setConfiguration((current) => current === null
      ? current
      : { ...current, tuning: { ...current.tuning, [key]: value } });
  };

  const save = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (configuration === null) {
      return;
    }
    setSaving(true);
    setError(null);
    try {
      const update: SystemConfigurationUpdate = {
        ...configuration,
        smtp: {
          ...configuration.smtp,
          ...(password.trim() === "" ? {} : { password }),
        },
      };
      const tuningChanged = appliedConfiguration === null
        || !sameTuning(update.tuning, appliedConfiguration.tuning);
      const mqttChanged = appliedConfiguration === null
        || !sameMqtt(update.mqtt, appliedConfiguration.mqtt);
      const smtpChanged = password.trim() !== ""
        || appliedConfiguration === null
        || !sameSmtp(update.smtp, appliedConfiguration.smtp);
      const saved = await updateSystemConfiguration(client, update);
      const savedConfiguration = updateFromConfiguration(saved);
      setConfiguration(savedConfiguration);
      setAppliedConfiguration((current) => ({
        mqtt: savedConfiguration.mqtt,
        smtp: savedConfiguration.smtp,
        tuning: current?.tuning ?? savedConfiguration.tuning,
      }));
      setPasswordConfigured(saved.smtp.password_configured);
      setPassword("");
      setRestartRequired(mqttChanged || tuningChanged);
      setSmtpAppliedLive(smtpChanged);
    } catch (saveError) {
      if (saveError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setError(saveError instanceof Error ? saveError.message : "System configuration update failed.");
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return <section className="system-configuration" aria-busy="true" />;
  }

  if (configuration === null) {
    return (
      <section className="system-configuration" aria-label="System Configuration">
        <button aria-label="Back to dashboard" className="icon-button" onClick={onBack} type="button">
          <ArrowLeft aria-hidden="true" size={17} />
        </button>
        {error !== null && <p className="system-error" role="alert">{error}</p>}
      </section>
    );
  }

  return (
    <section className="system-configuration" aria-label="System Configuration">
      <header className="system-configuration-header">
        <div>
          <span className="eyebrow">Administration</span>
          <h1>System Configuration</h1>
        </div>
        <button
          aria-label="Back to dashboard"
          className="icon-button"
          onClick={onBack}
          title="Back to dashboard"
          type="button"
        >
          <ArrowLeft aria-hidden="true" size={17} />
        </button>
      </header>

      <form onSubmit={(event) => void save(event)}>
        <section className="system-configuration-section" aria-label="SMTP settings">
          <div className="system-configuration-heading">
            <Settings2 aria-hidden="true" size={16} />
            <h2>SMTP</h2>
          </div>
          <label className="system-toggle">
            <input
              aria-label="Enable SMTP"
              checked={configuration.smtp.enabled}
              onChange={(event) => updateSmtp("enabled", event.target.checked)}
              type="checkbox"
            />
            <span>Enable SMTP</span>
          </label>
          {configuration.smtp.enabled && (
            <div className="system-configuration-grid">
              <label>
                <span>SMTP host</span>
                <input
                  aria-label="SMTP host"
                  onChange={(event) => updateSmtp("host", event.target.value || null)}
                  required
                  value={configuration.smtp.host ?? ""}
                />
              </label>
              <label>
                <span>SMTP port</span>
                <input
                  aria-label="SMTP port"
                  inputMode="numeric"
                  min="1"
                  onChange={(event) => updateSmtp("port", Number(event.target.value))}
                  required
                  value={configuration.smtp.port}
                />
              </label>
              <label>
                <span>SMTP username</span>
                <input
                  aria-label="SMTP username"
                  onChange={(event) => updateSmtp("username", event.target.value || null)}
                  required
                  value={configuration.smtp.username ?? ""}
                />
              </label>
              <label>
                <span>SMTP password</span>
                <input
                  aria-label="SMTP password"
                  autoComplete="new-password"
                  onChange={(event) => setPassword(event.target.value)}
                  placeholder={passwordConfigured ? "Configured" : "Required"}
                  type="password"
                  value={password}
                />
              </label>
              <label>
                <span>Sender</span>
                <input
                  aria-label="SMTP sender"
                  onChange={(event) => updateSmtp("from", event.target.value || null)}
                  required
                  value={configuration.smtp.from ?? ""}
                />
              </label>
              <label>
                <span>Recipient</span>
                <input
                  aria-label="SMTP recipient"
                  onChange={(event) => updateSmtp("to", event.target.value || null)}
                  required
                  value={configuration.smtp.to ?? ""}
                />
              </label>
              <label>
                <span>Send timeout seconds</span>
                <input
                  aria-label="SMTP timeout seconds"
                  inputMode="numeric"
                  min="1"
                  onChange={(event) => updateSmtp("timeout_seconds", Number(event.target.value))}
                  required
                  value={configuration.smtp.timeout_seconds}
                />
              </label>
            </div>
          )}
        </section>

        <section className="system-configuration-section" aria-label="MQTT settings">
          <div className="system-configuration-heading">
            <Settings2 aria-hidden="true" size={16} />
            <h2>MQTT</h2>
          </div>
          <div className="system-configuration-grid">
            <label>
              <span>Broker host</span>
              <input
                aria-label="MQTT broker host"
                onChange={(event) => updateMqtt("host", event.target.value)}
                required
                value={configuration.mqtt.host}
              />
            </label>
            <label>
              <span>Broker port</span>
              <input
                aria-label="MQTT broker port"
                inputMode="numeric"
                min="1"
                onChange={(event) => updateMqtt("port", Number(event.target.value))}
                required
                value={configuration.mqtt.port}
              />
            </label>
          </div>
        </section>

        <section className="system-configuration-section" aria-label="Stream settings">
          <div className="system-configuration-heading">
            <Settings2 aria-hidden="true" size={16} />
            <h2>Stream</h2>
          </div>
          <div className="system-configuration-grid">
            <NumberField label="Retention bytes" value={configuration.tuning.retention_bytes} onChange={(value) => updateTuning("retention_bytes", value)} />
            <NumberField label="Retention seconds" value={configuration.tuning.retention_seconds} onChange={(value) => updateTuning("retention_seconds", value)} />
            <NumberField label="Segment bytes" value={configuration.tuning.segment_bytes} onChange={(value) => updateTuning("segment_bytes", value)} />
            <NumberField label="Max record bytes" value={configuration.tuning.max_record_bytes} onChange={(value) => updateTuning("max_record_bytes", value)} />
          </div>
        </section>

        <section className="system-configuration-section" aria-label="Worker tuning">
          <div className="system-configuration-heading">
            <Settings2 aria-hidden="true" size={16} />
            <h2>Workers</h2>
          </div>
          <div className="system-configuration-grid">
            <NumberField label="Writer batch size" value={configuration.tuning.writer_batch_size} onChange={(value) => updateTuning("writer_batch_size", value)} />
            <NumberField label="Alert batch size" value={configuration.tuning.alert_batch_size} onChange={(value) => updateTuning("alert_batch_size", value)} />
            <NumberField label="Notification batch size" value={configuration.tuning.notification_batch_size} onChange={(value) => updateTuning("notification_batch_size", value)} />
            <NumberField label="Writer flush seconds" value={configuration.tuning.writer_flush_seconds} onChange={(value) => updateTuning("writer_flush_seconds", value)} />
            <NumberField label="Alert event interval milliseconds" value={configuration.tuning.alert_event_interval_milliseconds} onChange={(value) => updateTuning("alert_event_interval_milliseconds", value)} />
            <NumberField label="Alert window interval seconds" value={configuration.tuning.alert_window_interval_seconds} onChange={(value) => updateTuning("alert_window_interval_seconds", value)} />
            <NumberField label="Notification interval seconds" value={configuration.tuning.notification_interval_seconds} onChange={(value) => updateTuning("notification_interval_seconds", value)} />
            <NumberField label="Retention interval seconds" value={configuration.tuning.retention_interval_seconds} onChange={(value) => updateTuning("retention_interval_seconds", value)} />
            <NumberField label="Notification lease seconds" value={configuration.tuning.notification_lease_seconds} onChange={(value) => updateTuning("notification_lease_seconds", value)} />
            <NumberField label="Retry base seconds" value={configuration.tuning.notification_retry_base_seconds} onChange={(value) => updateTuning("notification_retry_base_seconds", value)} />
            <NumberField label="Retry max seconds" value={configuration.tuning.notification_retry_max_seconds} onChange={(value) => updateTuning("notification_retry_max_seconds", value)} />
          </div>
        </section>

        {error !== null && <p className="system-error" role="alert">{error}</p>}
        <div className="system-configuration-actions">
          <button className="system-save" disabled={saving} type="submit">
            <Save aria-hidden="true" size={16} />
            Save configuration
          </button>
          {smtpAppliedLive && (
            <span className="system-save-status system-save-status-live">SMTP changes apply live</span>
          )}
          {restartRequired && (
            <span className="system-save-status system-save-status-restart">
              Restart required for MQTT or tuning changes
            </span>
          )}
        </div>
      </form>
    </section>
  );
}

function NumberField({
  label,
  onChange,
  value,
}: {
  label: string;
  onChange(value: number): void;
  value: number;
}) {
  return (
    <label>
      <span>{label}</span>
      <input
        aria-label={label}
        inputMode="numeric"
        min="0"
        onChange={(event) => onChange(Number(event.target.value))}
        required
        value={value}
      />
    </label>
  );
}
