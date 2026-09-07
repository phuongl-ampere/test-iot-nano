"use client";

import { LogOut, Settings, Settings2, UserRound } from "lucide-react";
import { useState } from "react";

import { type Role } from "../lib/api";

type ProfileMenuProps = {
  role: Role;
  onLogout(): void;
  onOpenProfile(): void;
  onOpenSystemConfiguration(): void;
};

export function ProfileMenu({
  role,
  onLogout,
  onOpenProfile,
  onOpenSystemConfiguration,
}: ProfileMenuProps) {
  const [open, setOpen] = useState(false);

  return (
    <div className="profile-menu">
      <button
        aria-label="Open account menu"
        aria-expanded={open}
        className="icon-button"
        onClick={() => setOpen((current) => !current)}
        title="Account menu"
        type="button"
      >
        <Settings aria-hidden="true" size={16} />
      </button>
      {open && (
        <section className="profile-popover" aria-label="Account menu">
          <button className="profile-menu-action" onClick={onOpenProfile} type="button">
            <UserRound aria-hidden="true" size={15} />
            User profile
          </button>
          {role === "admin" && (
            <button
              className="profile-menu-action"
              onClick={onOpenSystemConfiguration}
              type="button"
            >
              <Settings2 aria-hidden="true" size={15} />
              Admin setting
            </button>
          )}
          <button
            className="profile-menu-action profile-menu-logout"
            onClick={onLogout}
            type="button"
          >
            <LogOut aria-hidden="true" size={15} />
            Logout
          </button>
        </section>
      )}
    </div>
  );
}
