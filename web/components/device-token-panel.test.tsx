// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { DeviceTokenPanel } from "./device-token-panel";

const activeToken = {
  id: "b4c2c8c0-c4a9-4a88-b822-7c10473442f8",
  device_id: "esp-000123",
  token_prefix: "iotd_01234567890",
  created_at: "2026-09-06T00:00:00Z",
  last_used_at: null,
  revoked_at: null,
};

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("DeviceTokenPanel", () => {
  it("shows and copies the retained active token returned by the API", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, { clipboard: { writeText } });
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response(JSON.stringify([{
        ...activeToken,
        token: "iotd_retained_token_value",
      }]), { status: 200 })),
    );

    render(
      <DeviceTokenPanel
        allowProvisioning={false}
        client={{ apiBaseUrl: "http://127.0.0.1:8080", sessionId: "session_admin" }}
        deviceId="esp-000123"
        onUnauthorized={vi.fn()}
      />,
    );

    await waitFor(() => {
      expect((screen.getByLabelText("Device token") as HTMLInputElement).value)
        .toBe("iotd_retained_token_value");
    });
    fireEvent.click(screen.getByRole("button", { name: "Copy device token" }));
    expect(writeText).toHaveBeenCalledWith("iotd_retained_token_value");
  });

  it("uses selected-device mode without a new-device provision action", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response(JSON.stringify([activeToken]), { status: 200 })),
    );

    render(
      <DeviceTokenPanel
        allowProvisioning={false}
        client={{ apiBaseUrl: "http://127.0.0.1:8080", sessionId: "session_admin" }}
        deviceId="esp-000123"
        onUnauthorized={vi.fn()}
      />,
    );

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Re-generate device token" })).not.toBeNull();
    });
    expect(screen.queryByRole("button", { name: "Provision new device token" })).toBeNull();
  });

  it("admin generates and copies the plaintext token once", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, { clipboard: { writeText } });
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify([]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        ...activeToken,
        token: "iotd_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      }), { status: 201 }));
    vi.stubGlobal("fetch", fetchMock);

    render(
      <DeviceTokenPanel
        client={{ apiBaseUrl: "http://127.0.0.1:8080", sessionId: "session_admin" }}
        deviceId="esp-000123"
        onUnauthorized={vi.fn()}
      />,
    );

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Generate device token" })).not.toBeNull();
    });
    fireEvent.click(screen.getByRole("button", { name: "Generate device token" }));

    await waitFor(() => {
      expect((screen.getByLabelText("Device token") as HTMLInputElement).value)
        .toMatch(/^iotd_/);
    });
    fireEvent.click(screen.getByRole("button", { name: "Copy device token" }));

    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "http://127.0.0.1:8080/api/devices/esp-000123/tokens",
      expect.objectContaining({ method: "POST" }),
    );
    expect(writeText).toHaveBeenCalledWith(expect.stringMatching(/^iotd_/));
  });

  it("admin provisions a named device without choosing its internal ID", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({
        ...activeToken,
        device_id: "01991bc2-1514-7cdd-b9a8-6581e1f89d8a",
        token: "iotd_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      }), { status: 201 }));
    vi.stubGlobal("fetch", fetchMock);

    render(
      <DeviceTokenPanel
        client={{ apiBaseUrl: "http://127.0.0.1:8080", sessionId: "session_admin" }}
        deviceId={null}
        onUnauthorized={vi.fn()}
      />,
    );

    fireEvent.change(screen.getByLabelText("Device name"), {
      target: { value: "Python virtual sensor" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Generate device token" }));

    await waitFor(() => {
      expect((screen.getByLabelText("Device token") as HTMLInputElement).value)
        .toMatch(/^iotd_b/);
    });
    expect(fetchMock).toHaveBeenCalledWith(
      "http://127.0.0.1:8080/api/device-tokens",
      expect.objectContaining({
        body: JSON.stringify({ display_name: "Python virtual sensor" }),
        method: "POST",
      }),
    );
  });

  it("admin can switch from a selected device to new-device provisioning", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response(JSON.stringify([activeToken]), { status: 200 })),
    );

    render(
      <DeviceTokenPanel
        client={{ apiBaseUrl: "http://127.0.0.1:8080", sessionId: "session_admin" }}
        deviceId="esp-000123"
        onUnauthorized={vi.fn()}
      />,
    );

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Provision new device token" })).not.toBeNull();
    });
    fireEvent.click(screen.getByRole("button", { name: "Provision new device token" }));

    expect(screen.getByLabelText("Device name")).not.toBeNull();
    expect(screen.getByRole("button", { name: "Cancel new device provisioning" })).not.toBeNull();
  });

  it("admin rotates then revokes the active token", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify([activeToken]), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        ...activeToken,
        id: "ac9c23f8-ea12-4e59-9aa9-9b906e4f8f8d",
        token: "iotd_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      }), { status: 201 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);

    render(
      <DeviceTokenPanel
        client={{ apiBaseUrl: "http://127.0.0.1:8080", sessionId: "session_admin" }}
        deviceId="esp-000123"
        onUnauthorized={vi.fn()}
      />,
    );

    await waitFor(() => {
      expect(screen.getByRole("button", { name: "Rotate device token" })).not.toBeNull();
    });
    fireEvent.click(screen.getByRole("button", { name: "Rotate device token" }));

    await waitFor(() => {
      expect((screen.getByLabelText("Device token") as HTMLInputElement).value)
        .toMatch(/^iotd_a/);
    });
    fireEvent.click(screen.getByRole("button", { name: "Revoke device token" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenNthCalledWith(
        3,
        "http://127.0.0.1:8080/api/device-tokens/ac9c23f8-ea12-4e59-9aa9-9b906e4f8f8d/revoke",
        expect.objectContaining({ method: "POST" }),
      );
    });
  });
});
