"use client";

import { Send } from "lucide-react";

export type DeviceCommand = "sample_now" | "reboot";

interface CommandControlProps {
  disabled: boolean;
  sending: boolean;
  value: DeviceCommand;
  onChange(value: DeviceCommand): void;
  onSend(): void;
}

export function CommandControl({
  disabled,
  sending,
  value,
  onChange,
  onSend,
}: CommandControlProps) {
  return (
    <div className="command-control">
      <select
        aria-label="Device command"
        disabled={disabled || sending}
        onChange={(event) => onChange(event.target.value as DeviceCommand)}
        value={value}
      >
        <option value="sample_now">Sample now</option>
        <option value="reboot">Reboot</option>
      </select>
      <button
        aria-label="Send device command"
        className="icon-button"
        disabled={disabled || sending}
        onClick={onSend}
        title="Send device command"
        type="button"
      >
        <Send aria-hidden="true" size={16} />
      </button>
    </div>
  );
}
