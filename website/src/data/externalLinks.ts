export const DISCORD_INVITE_URL = "https://dc.earlysalty.com";

export const TWITCH_BOT_AUTH_START_URL =
  "https://raid.earlysalty.com/twitch/raid/auth";

export function buildTwitchBotAuthUrl(): string {
  const url = new URL(TWITCH_BOT_AUTH_START_URL);
  // Unique URL per click avoids stale/cached redirects and forces fresh OAuth state.
  url.searchParams.set("ts", Date.now().toString());
  return url.toString();
}
