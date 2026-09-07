// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { SystemConfigurationPanel } from "./system-configuration";

const configuration = {
  mqtt: {
    host: "broker.example.test",
    port: 1883,
  },
  smtp: {
    enabled: true,
    host: "smtp.example.test",
    port: 465,
    username: "alerts@example.test",
    password_configured: true,
    from: "alerts@example.test",
    to: "ops@example.test",
    timeout_seconds: 15,
  },
  tuning: {
    retention_bytes: 2147483648,
    retention_seconds: 86400,
    segment_bytes: 134217728,
    max_record_bytes: 1048576,
    writer_batch_size: 1000,
    alert_batch_size: 250,
    notification_batch_size: 10,
    writer_flush_seconds: 1,
    alert_event_interval_milliseconds: 250,
    alert_window_interval_seconds: 60,
    notification_interval_seconds: 1,
    retention_interval_seconds: 60,
    notification_lease_seconds: 30,
    notification_retry_base_seconds: 1,
    notification_retry_max_seconds: 3600,
  },
};

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("SystemConfigurationPanel", () => {
  it("applies SMTP-only changes live without a restart requirement", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(configuration), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify(configuration), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    render(
      <SystemConfigurationPanel
        client={{ apiBaseUrl: "http://127.0.0.1:8080", sessionId: "session_admin" }}
        onBack={vi.fn()}
        onUnauthorized={vi.fn()}
      />,
    );

    await waitFor(() => {
      expect((screen.getByLabelText("SMTP host") as HTMLInputElement).value).toBe("smtp.example.test");
    });
    const password = screen.getByLabelText("SMTP password") as HTMLInputElement;
    expect(password.value).toBe("");
    expect(password.placeholder).toBe("Configured");

    fireEvent.change(password, { target: { value: "new-secret" } });
    fireEvent.click(screen.getByRole("button", { name: "Save configuration" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenNthCalledWith(
        2,
        "http://127.0.0.1:8080/api/system-configuration",
        expect.objectContaining({
          method: "PUT",
          body: expect.stringContaining("\"password\":\"new-secret\""),
        }),
      );
    });
    expect(screen.getByText("SMTP changes apply live")).not.toBeNull();
    expect(screen.queryByText("Restart required for tuning changes")).toBeNull();
    expect(screen.queryByRole("button", { name: "Restart iot-ingest" })).toBeNull();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("marks stream tuning changes as requiring a manual restart", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(configuration), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        ...configuration,
        tuning: { ...configuration.tuning, retention_seconds: 172800 },
      }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    render(
      <SystemConfigurationPanel
        client={{ apiBaseUrl: "http://127.0.0.1:8080", sessionId: "session_admin" }}
        onBack={vi.fn()}
        onUnauthorized={vi.fn()}
      />,
    );

    await waitFor(() => {
      expect((screen.getByLabelText("Retention seconds") as HTMLInputElement).value).toBe("86400");
    });
    fireEvent.change(screen.getByLabelText("Retention seconds"), {
      target: { value: "172800" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save configuration" }));

    await waitFor(() => {
      expect(screen.getByText("Restart required for MQTT or tuning changes")).not.toBeNull();
    });
  });

  it("saves MQTT host and port as a restart-required change", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(configuration), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        ...configuration,
        mqtt: { host: "mqtt.remote.test", port: 1884 },
      }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    render(
      <SystemConfigurationPanel
        client={{ apiBaseUrl: "http://127.0.0.1:8080", sessionId: "session_admin" }}
        onBack={vi.fn()}
        onUnauthorized={vi.fn()}
      />,
    );

    await waitFor(() => {
      expect((screen.getByLabelText("MQTT broker host") as HTMLInputElement).value).toBe("broker.example.test");
    });
    fireEvent.change(screen.getByLabelText("MQTT broker host"), {
      target: { value: "mqtt.remote.test" },
    });
    fireEvent.change(screen.getByLabelText("MQTT broker port"), {
      target: { value: "1884" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save configuration" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenNthCalledWith(
        2,
        "http://127.0.0.1:8080/api/system-configuration",
        expect.objectContaining({
          method: "PUT",
          body: expect.stringContaining("\"mqtt\":{\"host\":\"mqtt.remote.test\",\"port\":1884}"),
        }),
      );
    });
    expect(screen.getByText("Restart required for MQTT or tuning changes")).not.toBeNull();
  });
});
