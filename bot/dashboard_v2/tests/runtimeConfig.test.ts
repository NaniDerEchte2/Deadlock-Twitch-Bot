import assert from 'node:assert/strict';
import test from 'node:test';

(globalThis as typeof globalThis & {
  window: {
    __TWITCH_DASHBOARD_RUNTIME__?: Record<string, unknown>;
  };
}).window = {
  __TWITCH_DASHBOARD_RUNTIME__: {
    apiBase: '/twitch/api/v2/../admin',
    demoMode: true,
    allowedDemoProfiles: ['midcore_live'],
    defaultDemoProfile: 'midcore_live',
  },
};

const runtimeConfigModule = await import('../src/runtimeConfig');

test('falls back to the live API base for non-allowlisted runtime config values', () => {
  assert.equal(runtimeConfigModule.dashboardRuntimeConfig.apiBase, runtimeConfigModule.LIVE_API_BASE);
});

test('requires both demo route and demo namespace for effective demo mode', () => {
  const demoConfig = {
    apiBase: runtimeConfigModule.DEMO_API_BASE,
    demoMode: true,
    allowedDemoProfiles: ['midcore_live'],
    defaultDemoProfile: 'midcore_live',
  };

  assert.equal(
    runtimeConfigModule.resolveEffectiveDemoMode({
      pathname: '/twitch/dashboard-v2',
      runtimeConfig: demoConfig,
    }),
    false
  );

  assert.equal(
    runtimeConfigModule.resolveEffectiveDemoMode({
      pathname: '/twitch/demo',
      runtimeConfig: demoConfig,
    }),
    true
  );
});

test('does not treat demoMode alone as a valid demo runtime', () => {
  assert.equal(
    runtimeConfigModule.hasDemoRuntimeConfig({
      apiBase: runtimeConfigModule.LIVE_API_BASE,
      demoMode: true,
      allowedDemoProfiles: [],
      defaultDemoProfile: null,
    }),
    false
  );
});
