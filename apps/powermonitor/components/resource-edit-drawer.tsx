"use client";

import { useEffect, useMemo, useState, type FormEvent } from "react";

import {
  BffApiError,
  archiveDeviceAlertRule,
  createAssetResourceInvitation,
  createDeviceAlertRule,
  createDeviceResourceInvitation,
  listResourceProfiles,
  listDeviceAlertRules,
  regenerateDeviceToken,
  revealDeviceToken,
  updateAsset,
  updateDevice,
  updateDeviceAlertRule,
  type ResourceProfile,
  type Asset,
  type DeviceAlertRule,
  type DeviceAlertRuleInput,
  type Permission,
} from "../lib/browser-api";

type EditableResource = {
  id: string;
  kind: "asset" | "device";
  name?: string;
  permission?: Permission;
  asset_id?: string | null;
  asset_profile_id?: string | null;
  device_profile_id?: string | null;
  parent_id?: string | null;
};

type ResourceEditDrawerProps = {
  assets: Asset[];
  onClose(): void;
  onSaved(): void | Promise<void>;
  resource: EditableResource;
};

const defaultAlertRule = {
  comparison: "gt" as const,
  enabled: true,
  for_seconds: 0,
  metric_key: "power_w",
  name: "High active power",
  reopen_grace_seconds: 3_600,
  reminder_interval_seconds: 86_400,
  resolve_after_seconds: 300,
  rule_type: "event_threshold" as const,
  severity: "warning" as const,
  threshold: 500,
  window_seconds: null,
};

export function ResourceEditDrawer({
  assets,
  onClose,
  onSaved,
  resource,
}: ResourceEditDrawerProps) {
  const isDevice = resource.kind === "device";
  const canManage = resource.permission === "manager" || resource.permission === "owner";
  const canShare = resource.permission === "owner";
  const [name, setName] = useState(resource.name ?? resource.id);
  const [assignmentId, setAssignmentId] = useState(
    isDevice ? resource.asset_id ?? "" : resource.parent_id ?? "",
  );
  const [profiles, setProfiles] = useState<ResourceProfile[]>([]);
  const [profileId, setProfileId] = useState(
    isDevice ? resource.device_profile_id ?? "" : resource.asset_profile_id ?? "",
  );
  const [profilesLoading, setProfilesLoading] = useState(canManage);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [tokenLoading, setTokenLoading] = useState(isDevice && canShare);
  const [tokenBusy, setTokenBusy] = useState(false);
  const [shareUsername, setShareUsername] = useState("");
  const [sharePermission, setSharePermission] = useState<"viewer" | "manager">("viewer");
  const [sharing, setSharing] = useState(false);
  const [rules, setRules] = useState<DeviceAlertRule[]>([]);
  const [rulesLoading, setRulesLoading] = useState(isDevice && canManage);
  const [ruleBusy, setRuleBusy] = useState(false);
  const [editingRuleId, setEditingRuleId] = useState<string | null>(null);
  const [rule, setRule] = useState<DeviceAlertRuleInput>(defaultAlertRule);

  const assignmentChoices = useMemo(
    () => isDevice ? assets : assets.filter((asset) => asset.id !== resource.id),
    [assets, isDevice, resource.id],
  );

  useEffect(() => {
    let active = true;
    if (!canManage) {
      setProfilesLoading(false);
      return () => {
        active = false;
      };
    }
    void listResourceProfiles(resource.kind)
      .then((items) => {
        if (!active) return;
        setProfiles(items);
      })
      .catch((reason) => {
        if (active) setError(messageFor(reason));
      })
      .finally(() => {
        if (active) setProfilesLoading(false);
      });
    return () => {
      active = false;
    };
  }, [canManage, resource.kind]);

  useEffect(() => {
    let active = true;
    if (!isDevice || !canShare) {
      setTokenLoading(false);
      return () => {
        active = false;
      };
    }
    void revealDeviceToken(resource.id)
      .then((response) => {
        if (active) setToken(response.token ?? null);
      })
      .catch((reason) => {
        if (active) setError(messageFor(reason));
      })
      .finally(() => {
        if (active) setTokenLoading(false);
      });
    return () => {
      active = false;
    };
  }, [canShare, isDevice, resource.id]);

  useEffect(() => {
    let active = true;
    if (!isDevice || !canManage) {
      setRulesLoading(false);
      return () => {
        active = false;
      };
    }
    void listDeviceAlertRules(resource.id)
      .then((items) => {
        if (active) setRules(items);
      })
      .catch((reason) => {
        if (active) setError(messageFor(reason));
      })
      .finally(() => {
        if (active) setRulesLoading(false);
      });
    return () => {
      active = false;
    };
  }, [canManage, isDevice, resource.id]);

  const saveResource = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!canManage) return;
    setSaving(true);
    setError(null);
    setNotice(null);
    try {
      if (isDevice) {
        await updateDevice(resource.id, {
          asset_id: assignmentId === "" ? null : assignmentId,
          device_profile_id: profileId === "" ? null : profileId,
          display_name: name,
        });
      } else {
        await updateAsset(resource.id, {
          asset_profile_id: profileId === "" ? null : profileId,
          name,
          parent_asset_id: assignmentId === "" ? null : assignmentId,
        });
      }
      await onSaved();
      setNotice("Configuration saved.");
    } catch (reason) {
      setError(messageFor(reason));
    } finally {
      setSaving(false);
    }
  };

  const copyToken = async () => {
    if (token === null) return;
    try {
      await navigator.clipboard.writeText(token);
      setNotice("Device token copied.");
    } catch {
      setError("Device token could not be copied.");
    }
  };

  const regenerateToken = async () => {
    setTokenBusy(true);
    setError(null);
    setNotice(null);
    try {
      const next = await regenerateDeviceToken(resource.id);
      setToken(next.token ?? null);
      setNotice("Device token regenerated. The previous token is no longer active.");
    } catch (reason) {
      setError(messageFor(reason));
    } finally {
      setTokenBusy(false);
    }
  };

  const shareResource = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!canShare) return;
    setSharing(true);
    setError(null);
    setNotice(null);
    try {
      if (isDevice) {
        await createDeviceResourceInvitation(resource.id, shareUsername.trim(), sharePermission);
      } else {
        await createAssetResourceInvitation(resource.id, shareUsername.trim(), sharePermission);
      }
      setShareUsername("");
      setNotice("Invitation sent.");
    } catch (reason) {
      setError(messageFor(reason));
    } finally {
      setSharing(false);
    }
  };

  const saveRule = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!isDevice || !canManage) return;
    setRuleBusy(true);
    setError(null);
    setNotice(null);
    try {
      const input = {
        ...rule,
        threshold: Number(rule.threshold),
        window_seconds: rule.rule_type === "window_average" ? Number(rule.window_seconds ?? 300) : null,
      };
      const saved = editingRuleId === null
        ? await createDeviceAlertRule(resource.id, input)
        : await updateDeviceAlertRule(resource.id, editingRuleId, input);
      setRules((current) => editingRuleId === null
        ? [saved, ...current]
        : current.map((currentRule) => currentRule.id === saved.id ? saved : currentRule));
      setEditingRuleId(null);
      setRule(defaultAlertRule);
      setNotice(editingRuleId === null ? "Alert rule created." : "Alert rule updated.");
    } catch (reason) {
      setError(messageFor(reason));
    } finally {
      setRuleBusy(false);
    }
  };

  const editRule = (next: DeviceAlertRule) => {
    setEditingRuleId(next.id);
    setRule({
      comparison: next.comparison,
      enabled: next.enabled,
      for_seconds: next.for_seconds,
      hysteresis: next.hysteresis,
      metric_key: next.metric_key,
      name: next.name,
      reopen_grace_seconds: next.reopen_grace_seconds,
      reminder_interval_seconds: next.reminder_interval_seconds,
      resolve_after_seconds: next.resolve_after_seconds,
      rule_type: next.rule_type,
      severity: next.severity,
      threshold: next.threshold,
      window_seconds: next.window_seconds,
    });
  };

  const archiveRule = async (ruleId: string) => {
    setRuleBusy(true);
    setError(null);
    setNotice(null);
    try {
      await archiveDeviceAlertRule(resource.id, ruleId);
      setRules((current) => current.filter((currentRule) => currentRule.id !== ruleId));
      if (editingRuleId === ruleId) {
        setEditingRuleId(null);
        setRule(defaultAlertRule);
      }
      setNotice("Alert rule archived.");
    } catch (reason) {
      setError(messageFor(reason));
    } finally {
      setRuleBusy(false);
    }
  };

  return (
    <aside aria-labelledby="resource-edit-title" className="resource-edit-drawer">
      <header className="resource-edit-header">
        <div>
          <span className="eyebrow">{isDevice ? "Device configuration" : "Asset configuration"}</span>
          <h2 id="resource-edit-title">Edit {resource.name ?? resource.id}</h2>
        </div>
        <button aria-label="Close edit panel" className="drawer-close" onClick={onClose} type="button">Close</button>
      </header>

      {error !== null && <p className="drawer-notice error" role="alert">{error}</p>}
      {notice !== null && <p className="drawer-notice" role="status">{notice}</p>}

      {canManage ? (
        <form className="drawer-form" onSubmit={(event) => void saveResource(event)}>
          <section>
            <h3>General</h3>
            <label>
              <span>{isDevice ? "Device name" : "Asset name"}</span>
              <input
                aria-label={isDevice ? "Device name" : "Asset name"}
                disabled={saving}
                onChange={(event) => setName(event.target.value)}
                required
                value={name}
              />
            </label>
            <label>
              <span>{isDevice ? "Assigned asset" : "Parent asset"}</span>
              <select
                aria-label={isDevice ? "Assigned asset" : "Parent asset"}
                disabled={saving}
                onChange={(event) => setAssignmentId(event.target.value)}
                value={assignmentId}
              >
                <option value="">{isDevice ? "Unassigned" : "No parent"}</option>
                {assignmentChoices.map((asset) => <option key={asset.id} value={asset.id}>{asset.name}</option>)}
              </select>
            </label>
          </section>

          <section>
            <h3>PowerMonitor profile</h3>
            {profilesLoading ? <p className="empty-state">Loading profiles...</p> : (
              <label>
                <span>{isDevice ? "Device profile" : "Asset profile"}</span>
                <select
                  aria-label={isDevice ? "Device profile" : "Asset profile"}
                  disabled={saving}
                  onChange={(event) => setProfileId(event.target.value)}
                  value={profileId}
                >
                  <option value="">Unassigned</option>
                  {profiles.map((profile) => <option key={profile.id} value={profile.id}>{profile.name}</option>)}
                </select>
              </label>
            )}
          </section>
          <button disabled={saving || profilesLoading} type="submit">{saving ? "Saving" : "Save configuration"}</button>
        </form>
      ) : (
        <p className="empty-state">You can view this resource but cannot change its configuration.</p>
      )}

      {isDevice && canShare && (
        <section className="drawer-section">
          <h3>Device token</h3>
          {tokenLoading ? <p className="empty-state">Loading active token...</p> : (
            <div className="token-controls">
              <input aria-label="Active device token" readOnly type="text" value={token ?? "No active token"} />
              <div>
                <button disabled={token === null} onClick={() => void copyToken()} type="button">Copy token</button>
                <button disabled={tokenBusy} onClick={() => void regenerateToken()} type="button">
                  {tokenBusy ? "Regenerating" : "Regenerate token"}
                </button>
              </div>
            </div>
          )}
        </section>
      )}

      {canShare && (
        <section className="drawer-section">
          <h3>Share access</h3>
          <form className="drawer-form compact" onSubmit={(event) => void shareResource(event)}>
            <label>
              <span>Username</span>
              <input
                aria-label="Recipient username"
                disabled={sharing}
                maxLength={64}
                onChange={(event) => setShareUsername(event.target.value)}
                required
                value={shareUsername}
              />
            </label>
            <label>
              <span>Access</span>
              <select
                aria-label="Invitation access"
                disabled={sharing}
                onChange={(event) => setSharePermission(event.target.value as "viewer" | "manager")}
                value={sharePermission}
              >
                <option value="viewer">View</option>
                <option value="manager">Control</option>
              </select>
            </label>
            <button disabled={sharing} type="submit">{sharing ? "Inviting" : "Invite"}</button>
          </form>
        </section>
      )}

      {isDevice && canManage && (
        <section className="drawer-section">
          <h3>Alert rules</h3>
          {rulesLoading ? <p className="empty-state">Loading alert rules...</p> : (
            <>
              <ul className="drawer-rule-list">
                {rules.map((currentRule) => (
                  <li key={currentRule.id}>
                    <span><strong>{currentRule.name}</strong>{" "}{currentRule.metric_key} {currentRule.comparison} {currentRule.threshold}</span>
                    <div>
                      <button disabled={ruleBusy} onClick={() => editRule(currentRule)} type="button">Edit</button>
                      <button disabled={ruleBusy} onClick={() => void archiveRule(currentRule.id)} type="button">Archive</button>
                    </div>
                  </li>
                ))}
              </ul>
              <form className="drawer-form alert-rule-form" onSubmit={(event) => void saveRule(event)}>
                <label>
                  <span>Rule name</span>
                  <input disabled={ruleBusy} onChange={(event) => setRule({ ...rule, name: event.target.value })} required value={rule.name} />
                </label>
                <label>
                  <span>Metric key</span>
                  <input aria-label="Alert metric key" disabled={ruleBusy} onChange={(event) => setRule({ ...rule, metric_key: event.target.value })} required value={rule.metric_key} />
                </label>
                <label>
                  <span>Comparison</span>
                  <select disabled={ruleBusy} onChange={(event) => setRule({ ...rule, comparison: event.target.value as DeviceAlertRuleInput["comparison"] })} value={rule.comparison}>
                    <option value="gt">Greater than</option><option value="gte">Greater than or equal</option><option value="lt">Less than</option><option value="lte">Less than or equal</option>
                  </select>
                </label>
                <label>
                  <span>Threshold</span>
                  <input disabled={ruleBusy} min="0" onChange={(event) => setRule({ ...rule, threshold: Number(event.target.value) })} required step="any" type="number" value={rule.threshold} />
                </label>
                <label>
                  <span>Evaluation</span>
                  <select disabled={ruleBusy} onChange={(event) => setRule({ ...rule, rule_type: event.target.value as DeviceAlertRuleInput["rule_type"] })} value={rule.rule_type}>
                    <option value="event_threshold">Per event</option><option value="window_average">Window average</option>
                  </select>
                </label>
                {rule.rule_type === "window_average" && (
                  <label>
                    <span>Window seconds</span>
                    <input disabled={ruleBusy} min="60" onChange={(event) => setRule({ ...rule, window_seconds: Number(event.target.value) })} required type="number" value={rule.window_seconds ?? 300} />
                  </label>
                )}
                <label>
                  <span>Severity</span>
                  <select disabled={ruleBusy} onChange={(event) => setRule({ ...rule, severity: event.target.value as DeviceAlertRuleInput["severity"] })} value={rule.severity}>
                    <option value="info">Info</option><option value="warning">Warning</option><option value="critical">Critical</option>
                  </select>
                </label>
                <label className="checkbox-field"><input checked={rule.enabled} disabled={ruleBusy} onChange={(event) => setRule({ ...rule, enabled: event.target.checked })} type="checkbox" /> Enabled</label>
                <div className="drawer-form-actions">
                  {editingRuleId !== null && <button disabled={ruleBusy} onClick={() => { setEditingRuleId(null); setRule(defaultAlertRule); }} type="button">Cancel edit</button>}
                  <button disabled={ruleBusy} type="submit">{ruleBusy ? "Saving" : editingRuleId === null ? "Create alert rule" : "Save alert rule"}</button>
                </div>
              </form>
            </>
          )}
        </section>
      )}

      {!isDevice && <p className="drawer-help">Alert rules are configured on individual devices.</p>}
    </aside>
  );
}

function messageFor(reason: unknown): string {
  if (reason instanceof BffApiError && reason.status === 403) {
    return "You do not have permission for this configuration.";
  }
  return reason instanceof Error ? reason.message : "PowerMonitor request failed.";
}
