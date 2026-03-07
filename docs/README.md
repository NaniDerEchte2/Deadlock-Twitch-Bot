# Project Index

## Services

- [Twitch Bot](../bot/readme.md)
  - Runtime focus: chat, monitoring, raids, analytics collection
  - Keep independent from dashboard-only startup paths
- Dashboard
  - Runtime focus: web server, analytics API, React frontend
  - Architecture: [Dashboard Feature Map](architecture/dashboard/README.md)

## Key Code Areas

- [`bot/analytics/`](../bot/analytics/) analytics collection and API support
- [`bot/dashboard/`](../bot/dashboard/) server-side dashboard routes and HTML helpers
- [`bot/dashboard_v2/`](../bot/dashboard_v2/) frontend application
- [`bot/raid/`](../bot/raid/) raid feature area
- [`bot/social_media/`](../bot/social_media/) social media workflows

## Dashboard Role Guides

- [Admin Surfaces](architecture/dashboard/ADMIN.md)
- [Streamer Surfaces](architecture/dashboard/STREAMER.md)

## Existing Audits

- [Admin User Surface Audit 2026-03-05](admin_user_surface_audit_2026-03-05.md)

## Guardrails

- Do not introduce imports that make the Twitch Bot depend on dashboard startup code.
- Do not introduce imports that make the Dashboard depend on bot runtime entrypoints.
- Shared code should live in existing shared feature areas under `bot/`, not by crossing service boundaries.
