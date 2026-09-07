// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { AlertPanel } from "./alert-panel";

const client = { apiBaseUrl: "http://127.0.0.1:8080", sessionId: "session_test" };

const rule = {
  id: "rule-1",
  name: "High temperature",
  enabled: true,
  device_id: null,
  metric_key: "temperature_c",
  rule_type: "event_threshold" as const,
  comparison: "gt" as const,
  threshold: 40,
  window_seconds: null,
  for_seconds: 300,
  resolve_after_seconds: 300,
  reopen_grace_seconds: 3600,
  hysteresis: null,
  severity: "warning" as const,
  reminder_interval_seconds: 86400,
  created_at: "2026-09-05T10:00:00Z",
  updated_at: "2026-09-05T10:00:00Z",
};

const incident = {
  id: "incident-1",
  rule_id: "rule-1",
  rule_name: "High temperature",
  severity: "warning" as const,
  device_id: "esp-000123",
  status: "open" as const,
  condition_started_at: "2026-09-05T10:00:00Z",
  opened_at: "2026-09-05T10:00:00Z",
  resolved_at: null,
  acknowledged_at: null,
  acknowledged_by: null,
  last_value: 41,
  updated_at: "2026-09-05T10:00:00Z",
};

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("AlertPanel", () => {
  it("lets an admin edit and archive a rule", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(rule), { status: 200 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("confirm", vi.fn(() => true));
    const onRefresh = vi.fn();

    render(
      <AlertPanel
        client={client}
        incidents={[incident]}
        onRefresh={onRefresh}
        onUnauthorized={vi.fn()}
        role="admin"
        rules={[rule]}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Edit High temperature" }));
    expect((screen.getByLabelText("Rule name") as HTMLInputElement).value).toBe("High temperature");
    fireEvent.change(screen.getByLabelText("Threshold"), { target: { value: "42" } });
    fireEvent.click(screen.getByRole("button", { name: "Save changes" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "http://127.0.0.1:8080/api/alert-rules/rule-1",
        expect.objectContaining({
          method: "PUT",
          body: expect.stringContaining("\"threshold\":42"),
        }),
      );
    });

    fireEvent.click(screen.getByRole("button", { name: "Delete High temperature" }));
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "http://127.0.0.1:8080/api/alert-rules/rule-1",
        expect.objectContaining({ method: "DELETE" }),
      );
    });
    expect(onRefresh).toHaveBeenCalledTimes(2);
  });

  it("hides every mutation control from a viewer", () => {
    render(
      <AlertPanel
        client={{ ...client, sessionId: "session_viewer_2" }}
        incidents={[incident]}
        onRefresh={vi.fn()}
        onUnauthorized={vi.fn()}
        role="viewer"
        rules={[rule]}
      />,
    );

    expect(screen.queryByRole("button", { name: "Create rule" })).toBeNull();
    expect(screen.queryByLabelText("Enable High temperature")).toBeNull();
    expect(screen.queryByRole("button", { name: "Edit High temperature" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Delete High temperature" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Acknowledge High temperature" })).toBeNull();
  });

  it("delegates an expired rule mutation token to the dashboard", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null, { status: 401 })));
    const onUnauthorized = vi.fn();

    render(
      <AlertPanel
        client={client}
        incidents={[]}
        onRefresh={vi.fn()}
        onUnauthorized={onUnauthorized}
        role="admin"
        rules={[]}
      />,
    );

    fireEvent.change(screen.getByLabelText("Rule name"), {
      target: { value: "High temperature" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Create rule" }));

    await waitFor(() => expect(onUnauthorized).toHaveBeenCalledTimes(1));
  });
});
