"use client";

import { Ban, Copy, KeyRound, Plus, RefreshCw, X } from "lucide-react";
import { useEffect, useState } from "react";

import {
  type ApiClient,
  type DeviceToken,
  UnauthorizedApiError,
  createDeviceToken,
  fetchDeviceTokens,
  provisionDeviceToken,
  revokeDeviceToken,
  rotateDeviceToken,
} from "../lib/api";
import { browserDateTime } from "../lib/time";

type DeviceTokenPanelProps = {
  allowProvisioning?: boolean;
  client: ApiClient;
  deviceId: string | null;
  onUnauthorized(): void;
};

function tokenDate(value: string | null): string {
  return browserDateTime(value, "Never");
}

export function DeviceTokenPanel({
  allowProvisioning = true,
  client,
  deviceId,
  onUnauthorized,
}: DeviceTokenPanelProps) {
  const [tokens, setTokens] = useState<DeviceToken[]>([]);
  const [provisionName, setProvisionName] = useState("");
  const [provisioning, setProvisioning] = useState(deviceId === null);
  const [secret, setSecret] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [working, setWorking] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (deviceId === null || provisioning) {
      setTokens([]);
      setSecret(null);
      setError(null);
      setLoading(false);
      return;
    }
    setLoading(true);
    setSecret(null);
    setError(null);
    void fetchDeviceTokens(client, deviceId)
      .then((nextTokens) => {
        setTokens(nextTokens);
        setSecret(nextTokens.find((token) => token.revoked_at === null)?.token ?? null);
      })
      .catch((loadError) => {
        if (loadError instanceof UnauthorizedApiError) {
          onUnauthorized();
          return;
        }
        setError(loadError instanceof Error ? loadError.message : "Device token request failed.");
      })
      .finally(() => setLoading(false));
  }, [client, deviceId, onUnauthorized, provisioning]);

  useEffect(() => {
    setProvisioning(deviceId === null);
    setProvisionName("");
  }, [deviceId]);

  const activeToken = tokens.find((token) => token.revoked_at === null) ?? null;
  const targetDeviceName = provisionName.trim();

  const startProvisioning = () => {
    setProvisioning(true);
    setProvisionName("");
    setTokens([]);
    setSecret(null);
    setError(null);
  };

  const issue = async (action: () => Promise<DeviceToken>) => {
    setWorking(true);
    setError(null);
    try {
      const nextToken = await action();
      setTokens([nextToken]);
      setSecret(nextToken.token ?? null);
    } catch (actionError) {
      if (actionError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setError(actionError instanceof Error ? actionError.message : "Device token update failed.");
    } finally {
      setWorking(false);
    }
  };

  const revoke = async () => {
    if (activeToken === null) {
      return;
    }
    setWorking(true);
    setError(null);
    try {
      await revokeDeviceToken(client, activeToken.id);
      setTokens((current) => current.map((token) => (
        token.id === activeToken.id ? { ...token, revoked_at: new Date().toISOString() } : token
      )));
      setSecret(null);
    } catch (actionError) {
      if (actionError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setError(actionError instanceof Error ? actionError.message : "Device token revocation failed.");
    } finally {
      setWorking(false);
    }
  };

  const copyToken = () => {
    if (secret !== null) {
      void navigator.clipboard?.writeText(secret);
    }
  };

  return (
    <section className="device-token-panel" aria-label="Device access token" aria-busy={loading}>
      <div className="device-token-heading">
        <div>
          <span className="eyebrow">Device access</span>
          <h2>Token</h2>
        </div>
        {activeToken !== null && <code>{activeToken.token_prefix}</code>}
      </div>
      {activeToken !== null && (
        <dl className="device-token-metadata">
          <div><dt>Created</dt><dd>{tokenDate(activeToken.created_at)}</dd></div>
          <div><dt>Last used</dt><dd>{tokenDate(activeToken.last_used_at)}</dd></div>
        </dl>
      )}

      {provisioning && (
        <label className="device-token-device-name">
          <span>Device name</span>
          <input
            aria-label="Device name"
            onChange={(event) => setProvisionName(event.target.value)}
            value={provisionName}
          />
        </label>
      )}

      {secret !== null && (
        <div className="device-token-secret">
          <input aria-label="Device token" readOnly value={secret} />
          <button
            aria-label="Copy device token"
            className="icon-button"
            onClick={copyToken}
            title="Copy device token"
            type="button"
          >
            <Copy aria-hidden="true" size={16} />
          </button>
        </div>
      )}

      {error !== null && <p className="system-error" role="alert">{error}</p>}

      <div className="device-token-actions">
        {allowProvisioning && !provisioning && deviceId !== null && (
          <button
            aria-label="Provision new device token"
            className="icon-button"
            disabled={working}
            onClick={startProvisioning}
            title="Provision new device token"
            type="button"
          >
            <Plus aria-hidden="true" size={16} />
          </button>
        )}
        {allowProvisioning && provisioning && deviceId !== null && (
          <button
            aria-label="Cancel new device provisioning"
            className="icon-button"
            disabled={working}
            onClick={() => setProvisioning(false)}
            title="Cancel new device provisioning"
            type="button"
          >
            <X aria-hidden="true" size={16} />
          </button>
        )}
        {activeToken === null ? (
          <button
            className="device-token-generate"
            disabled={working || loading || (provisioning && targetDeviceName === "")}
            onClick={() => void issue(() => (
              provisioning
                ? provisionDeviceToken(client, targetDeviceName)
                : createDeviceToken(client, deviceId ?? "")
            ))}
            type="button"
          >
            <KeyRound aria-hidden="true" size={16} />
            Generate device token
          </button>
        ) : (
          <>
            <button
              aria-label={allowProvisioning ? "Rotate device token" : "Re-generate device token"}
              className="icon-button"
              disabled={working}
              onClick={() => void issue(() => rotateDeviceToken(client, activeToken.id))}
              title={allowProvisioning ? "Rotate device token" : "Re-generate device token"}
              type="button"
            >
              <RefreshCw aria-hidden="true" size={16} />
            </button>
            <button
              aria-label="Revoke device token"
              className="icon-button destructive-icon"
              disabled={working}
              onClick={() => void revoke()}
              title="Revoke device token"
              type="button"
            >
              <Ban aria-hidden="true" size={16} />
            </button>
          </>
        )}
      </div>
    </section>
  );
}
