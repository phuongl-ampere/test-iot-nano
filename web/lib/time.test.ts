import { afterEach, describe, expect, it, vi } from "vitest";

import { browserDateTime } from "./time";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("browserDateTime", () => {
  it("uses the browser timezone and labels it in the formatted timestamp", () => {
    const format = vi.fn().mockReturnValue("06 Sep 2026, 21:13 GMT+7");
    const dateTimeFormat = vi.spyOn(Intl, "DateTimeFormat").mockImplementation(
      function DateTimeFormatMock() {
        return { format } as unknown as Intl.DateTimeFormat;
      } as unknown as typeof Intl.DateTimeFormat,
    );

    expect(browserDateTime("2026-09-06T14:13:00Z")).toBe("06 Sep 2026, 21:13 GMT+7");
    expect(dateTimeFormat).toHaveBeenCalledWith(
      undefined,
      expect.objectContaining({ timeZoneName: "short" }),
    );
    expect(dateTimeFormat.mock.calls[0][1]).not.toHaveProperty("timeZone");
  });
});
