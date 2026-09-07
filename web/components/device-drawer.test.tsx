// @vitest-environment jsdom

import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { DeviceDrawer } from "./management-panels";

describe("DeviceDrawer", () => {
  afterEach(cleanup);

  it("renders selected device information inside a right-side dialog", () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify([]), { status: 200 })));
    render(
      <DeviceDrawer
        assets={[]}
        client={{ apiBaseUrl: "http://127.0.0.1:8080", sessionId: "session_test" }}
        device={{
          asset_id: null,
          attributes: {},
          device_id: "device-1",
          device_profile_id: null,
          display_name: "Main meter",
          gateway_device_id: null,
          gateway_status: null,
          is_gateway: false,
          last_seen_at: null,
          online: true,
        }}
        gateways={[]}
        onChange={vi.fn()}
        onClose={vi.fn()}
        onDelete={vi.fn()}
        onSave={vi.fn()}
        onUnauthorized={vi.fn()}
        profiles={[]}
        token={null}
        working={false}
      />,
    );

    expect(screen.getByRole("dialog", { name: "Device details" })).not.toBeNull();
    expect(screen.getByRole("heading", { name: "Main meter" })).not.toBeNull();
    expect(screen.getByText("Last telemetry")).not.toBeNull();
    expect(screen.getByRole("heading", { name: "Token" })).not.toBeNull();
  });

  it("allows a child device to select an existing gateway and hides token controls", () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify([]), { status: 200 })));
    render(
      <DeviceDrawer
        assets={[]}
        client={{ apiBaseUrl: "http://127.0.0.1:8080", sessionId: "session_test" }}
        device={{
          asset_id: null,
          attributes: {},
          device_id: "child-1",
          device_profile_id: null,
          display_name: "Panel meter",
          gateway_device_id: "gateway-1",
          gateway_status: null,
          is_gateway: false,
          last_seen_at: null,
          online: true,
        }}
        gateways={[
          {
            asset_id: null,
            attributes: {},
            device_id: "gateway-1",
            device_profile_id: null,
            display_name: "Field gateway",
            gateway_device_id: null,
            gateway_status: "online",
            is_gateway: true,
            last_seen_at: null,
            online: true,
          },
        ]}
        onChange={vi.fn()}
        onClose={vi.fn()}
        onDelete={vi.fn()}
        onSave={vi.fn()}
        onUnauthorized={vi.fn()}
        profiles={[]}
        token={null}
        working={false}
      />,
    );

    expect(screen.getByLabelText("Gateway")).not.toBeNull();
    expect(screen.getByRole("option", { name: "Field gateway" })).not.toBeNull();
    expect(screen.queryByRole("heading", { name: "Token" })).toBeNull();
  });
});
