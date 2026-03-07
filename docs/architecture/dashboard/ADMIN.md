# Dashboard Admin Surfaces

Admin-only behavior is documented here so the code can stay feature-based.

## Primary Admin Features

- [`bot/dashboard/admin/`](../../../bot/dashboard/admin/)
  - Legal pages and admin-only handlers
- [`bot/dashboard/billing/`](../../../bot/dashboard/billing/)
  - Stripe configuration, billing plans, webhook/readiness helpers
- [`bot/dashboard/live/announcement_mode_mixin.py`](../../../bot/dashboard/live/announcement_mode_mixin.py)
  - Global announcement and broadcast mode management

## Shared With Streamers

- [`bot/dashboard/auth/`](../../../bot/dashboard/auth/)
  - OAuth and dashboard session management
- [`bot/dashboard/core/`](../../../bot/dashboard/core/)
  - Shared HTML, stats rendering, and infrastructure helpers

## Notes

- Role checks should stay in route handlers and auth/session helpers, not in folder naming.
- New admin features should prefer the relevant feature package first and only use `admin/` when the behavior is truly admin-only.
