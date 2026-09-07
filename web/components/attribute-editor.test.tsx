// @vitest-environment jsdom

import { fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { describe, expect, it } from "vitest";

import { AttributeEditor } from "./management-panels";

function Harness() {
  const [attributes, setAttributes] = useState<Record<string, unknown>>({
    enabled: true,
    rating: 42,
  });
  return (
    <>
      <AttributeEditor label="Device attributes" onChange={setAttributes} value={attributes} />
      <output data-testid="attributes">{JSON.stringify(attributes)}</output>
    </>
  );
}

describe("AttributeEditor", () => {
  it("preserves JSON value types when an attribute key is renamed", () => {
    render(<Harness />);

    fireEvent.change(screen.getByLabelText("Device attributes key rating"), {
      target: { value: "rated_power" },
    });

    expect(screen.getByTestId("attributes").textContent).toBe(
      '{"enabled":true,"rated_power":42}',
    );
  });
});
