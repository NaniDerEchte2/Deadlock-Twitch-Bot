# Dashboard Streamer Surfaces

Streamer-facing behavior is documented here so the code can stay feature-based.

## Primary Streamer Features

- [`bot/dashboard/live/`](../../../bot/dashboard/live/)
  - Live status, go-live embeds, Discord announcement configuration
- [`bot/dashboard/raids/`](../../../bot/dashboard/raids/)
  - Raid dashboard, history, requirements, OAuth callback flow
- [`bot/dashboard/affiliate/`](../../../bot/dashboard/affiliate/)
  - Affiliate signup, tracking, Stripe Connect onboarding

## Shared With Admins

- [`bot/dashboard/auth/`](../../../bot/dashboard/auth/)
  - OAuth and session handling
- [`bot/dashboard/billing/`](../../../bot/dashboard/billing/)
  - Billing catalog and checkout flows for subscriptions and upgrades
- [`bot/dashboard/core/`](../../../bot/dashboard/core/)
  - Shared HTML and stats infrastructure

## Notes

- New streamer features should be grouped by capability, not by audience label.
- If a feature serves both roles, keep it in the feature package and document role visibility here instead of creating a parallel role folder.
