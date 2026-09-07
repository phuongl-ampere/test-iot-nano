"use client";

import { ArrowLeft, KeyRound } from "lucide-react";
import { FormEvent, useState } from "react";

import {
  type ApiClient,
  type Role,
  UnauthorizedApiError,
  changePassword,
} from "../lib/api";

type UserProfilePanelProps = {
  client: ApiClient;
  role: Role;
  onBack(): void;
  onPasswordChanged(): void;
  onUnauthorized(): void;
};

export function UserProfilePanel({
  client,
  role,
  onBack,
  onPasswordChanged,
  onUnauthorized,
}: UserProfilePanelProps) {
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      await changePassword(client, currentPassword, newPassword);
      onPasswordChanged();
    } catch (updateError) {
      if (updateError instanceof UnauthorizedApiError) {
        onUnauthorized();
        return;
      }
      setError(updateError instanceof Error ? updateError.message : "Password update failed.");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <section className="user-profile" aria-label="User profile">
      <header className="user-profile-header">
        <div>
          <span className="eyebrow">Account</span>
          <h1>User profile</h1>
        </div>
        <button
          aria-label="Back to dashboard"
          className="icon-button"
          onClick={onBack}
          title="Back to dashboard"
          type="button"
        >
          <ArrowLeft aria-hidden="true" size={17} />
        </button>
      </header>

      <section className="user-profile-section" aria-label="User information">
        <div className="user-profile-heading">
          <KeyRound aria-hidden="true" size={16} />
          <h2>Account information</h2>
        </div>
        <dl className="user-profile-info">
          <div>
            <dt>Role</dt>
            <dd>{role}</dd>
          </div>
        </dl>
      </section>

      <form className="user-profile-section user-profile-form" onSubmit={(event) => void submit(event)}>
        <div className="user-profile-heading">
          <KeyRound aria-hidden="true" size={16} />
          <h2>Change password</h2>
        </div>
        <label>
          <span>Current password</span>
          <input
            aria-label="Current password"
            autoComplete="current-password"
            minLength={8}
            onChange={(event) => setCurrentPassword(event.target.value)}
            required
            spellCheck={false}
            type="password"
            value={currentPassword}
          />
        </label>
        <label>
          <span>New password</span>
          <input
            aria-label="New password"
            autoComplete="new-password"
            minLength={8}
            onChange={(event) => setNewPassword(event.target.value)}
            required
            spellCheck={false}
            type="password"
            value={newPassword}
          />
        </label>
        {error !== null && <p className="profile-error" role="alert">{error}</p>}
        <button className="profile-save" disabled={submitting} type="submit">
          Save password
        </button>
      </form>
    </section>
  );
}
