// ─────────────────────────────────────────────────────────────────────────────
//  deploy-daytona.test.ts — the v2 uploader, end to end, with ZERO network and
//  no cargo.
//
//  Everything side-effectful is injected: `main(argv, requestImpl)` takes the
//  HTTP seam, and the poll/teardown helpers take `requestImpl`/`sleepImpl`. Every
//  branch asserted here comes from services/daytona/sandbox-specs.md:
//    · args      — defaults, every flag, --port absent/0/explicit, verb conflicts
//    · requests  — POST /apps vs PUT /apps/nexus, the x-app-name header, the
//                  config (auto port OMITTED), both credentials, the v2 routes
//    · manifest  — remote-build's runCmd supplies NEXUS_LISTEN=127.0.0.1:$PORT
//    · poll      — the create AND blue-green replace state machines, a probe
//                  flap, failure + log tail, 404, transient misses, timeout
//    · teardown  — DELETE /apps/nexus, `removed:false` still success
//    · gate      — a v1 runner is refused (apiVersion>=2 or capabilities [apps])
//    · entry     — importing this module starts nothing
//
//  The artifact for the main() runs is a tiny real ELF magic written under the
//  repo-root Temp/ scratch dir (AGENTS.md): the C3 format guard reads it for real.
// ─────────────────────────────────────────────────────────────────────────────

import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it, vi } from 'vitest';

import {
    appLogsUrl,
    appStateUrl,
    appTeardownUrl,
    buildAppConfig,
    buildUploadHeaders,
    credentialsOf,
    explainUploadFailure,
    fetchAppLogs,
    healthUrl,
    isCliEntryPoint,
    main,
    parseArgs,
    pollAppUntilReady,
    profileFor,
    readTeardownOutcome,
    readUploadOutcome,
    readV2Gate,
    TAR_EXCLUDES,
    uploadUrl,
    USAGE,
    type DeployArgs,
    type RequestResult,
} from './deploy-daytona';

// ── fakes ────────────────────────────────────────────────────────────────────
const OK = (json: unknown, status = 200): RequestResult => ({ status, ok: true, json, text: JSON.stringify(json) });
const ERR = (status: number, json: unknown = null, text = ''): RequestResult => ({ status, ok: false, json, text: text || (json ? JSON.stringify(json) : '') });

/** A redacted AppView, as the runner sends it (§5.2). */
const appView = (over: Record<string, unknown> = {}) => ({
    name: 'nexus',
    revision: 1,
    path: '/app/nexus/',
    publicUrl: 'https://e.example/app/nexus/',
    internalPort: 20042,
    profile: 'nexus',
    filename: 'nexus-hub',
    bundleSha256: 'abc',
    bundleBytes: 8,
    status: 'ready',
    ready: true,
    pid: 10,
    pgid: 10,
    startedAtMs: 1,
    readyAtMs: 2,
    createdAtMs: 1,
    updatedAtMs: 2,
    lastProbe: { ok: true, httpStatus: 200, atMs: 2, error: null },
    targetPrefix: '',
    config: { port: 20042, envKeys: [], hasBuildCmd: false, hasRunCmd: false, anonymous: true },
    queue: null,
    error: null,
    exit: null,
    lastReplaceError: null,
    pending: null,
    ...over,
});

/** The /health body a v2 runner sends (§7.2/§17.1). */
const V2_HEALTH = { ok: true, slot: 'multi', uptimeSec: 4, contract: 'sandbox-runtime-v2', apiVersion: 2, capabilities: ['apps', 'app-proxy'], runnerPort: 7777, publicOrigin: 'https://e.example', authRequired: false };
/** The v1 marker: neither apiVersion 2 nor the apps capability. */
const V1_HEALTH = { status: 'ok', contract: 'daytona-runtime-v1', runnerPort: 7777, authenticated: false, exposedPorts: [8085] };

type Call = { method: string; url: string; headers: Record<string, string>; body?: unknown };
/** Scripted HTTP: one entry per call, recorded with its headers/body. */
const makeRequest = (script: RequestResult[]) => {
    const calls: Call[] = [];
    let index = 0;
    const requestImpl = async (method: string, url: string, options: { headers?: Record<string, string>; body?: unknown } = {}) => {
        calls.push({ method, url, headers: options.headers ?? {}, body: options.body });
        const next = script[index++];
        if (next === undefined) throw new Error(`request script exhausted at call ${index}: ${method} ${url}`);
        return next;
    };
    return { requestImpl, calls };
};

/** Flags with no ambient env noise; defaults come from the parser itself. */
const flagsOf = (argv: readonly string[]): DeployArgs => {
    const flags = parseArgs(argv);
    return flags;
};

/** A real (tiny) ELF under the repo-root Temp/ scratch dir — the C3 guard reads it. */
const SCRATCH_ELF = (() => {
    const scriptsDir = dirname(fileURLToPath(import.meta.url));
    const repoRoot = resolve(scriptsDir, '..', '..', '..', '..');
    const dir = join(repoRoot, 'Temp', 'deploy-daytona-test');
    const path = join(dir, 'nexus-hub');
    mkdirSync(dir, { recursive: true });
    // 0x7f "E" "L" "F" + padding: enough for the magic probe and a non-empty file.
    writeFileSync(path, Buffer.from([0x7f, 0x45, 0x4c, 0x46, 0, 0, 0, 0]));
    return path;
})();

afterEach(() => {
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-daytona: argument parsing', () => {
    it('defaults to a local CREATE with an AUTO port and no credentials', () => {
        for (const name of ['NEXUS_DAYTONA_ENDPOINT', 'NEXUS_DAYTONA_PREVIEW_TOKEN', 'NEXUS_SANDBOX_ADMIN_TOKEN', 'SANDBOX_ADMIN_TOKEN']) vi.stubEnv(name, '');
        expect(flagsOf([])).toEqual({
            endpoint: '',
            token: '',
            adminToken: '',
            mode: 'local',
            port: undefined,
            env: {},
            wait: true,
            noBuild: false,
            artifact: '',
            teardown: false,
            replace: false,
            dryRun: false,
            help: false,
        });
    });

    it('parses every flag into the exact record', () => {
        const flags = flagsOf(['--endpoint', 'https://e', '--token', 'pt', '--admin-token', 'at', '--mode', 'remote-build', '--port', '9000', '--env', 'A=1', '--no-wait', '--replace']);
        expect(flags).toEqual({
            endpoint: 'https://e',
            token: 'pt',
            adminToken: 'at',
            mode: 'remote-build',
            port: 9000,
            env: { A: '1' },
            wait: false,
            noBuild: false,
            artifact: '',
            teardown: false,
            replace: true,
            dryRun: false,
            help: false,
        });
    });

    it('reads credentials from env, with the special admin token name winning', () => {
        vi.stubEnv('NEXUS_DAYTONA_ENDPOINT', 'https://env.example');
        vi.stubEnv('NEXUS_DAYTONA_PREVIEW_TOKEN', 'env-preview');
        vi.stubEnv('NEXUS_SANDBOX_ADMIN_TOKEN', 'env-admin');
        vi.stubEnv('SANDBOX_ADMIN_TOKEN', 'generic-admin');
        expect(flagsOf([])).toMatchObject({ endpoint: 'https://env.example', token: 'env-preview', adminToken: 'env-admin' });
    });

    it('falls back to the generic SANDBOX_ADMIN_TOKEN when the nexus-specific one is unset', () => {
        vi.stubEnv('NEXUS_SANDBOX_ADMIN_TOKEN', '');
        vi.stubEnv('SANDBOX_ADMIN_TOKEN', 'generic-admin');
        expect(flagsOf([]).adminToken).toBe('generic-admin');
    });

    it('treats --port 0 as explicit auto, and rejects only out-of-range integers', () => {
        expect(flagsOf(['--port', '0']).port).toBeUndefined();
        expect(flagsOf(['--port', '65535']).port).toBe(65535);
    });

    it('accumulates repeated --env in typed order and keeps = inside values', () => {
        const flags = flagsOf(['--env', 'B=2', '--env', 'A=1', '--env', 'TOKEN=a=b']);
        expect(Object.keys(flags.env)).toEqual(['B', 'A', 'TOKEN']);
        expect(flags.env).toEqual({ B: '2', A: '1', TOKEN: 'a=b' });
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-daytona: v2 routes and app request shape', () => {
    it('uses the named-app routes, never the v1 root aliases', () => {
        expect(uploadUrl('https://e/pfx', false)).toBe('https://e/pfx/apps');
        expect(uploadUrl('https://e/pfx', true)).toBe('https://e/pfx/apps/nexus');
        expect(appStateUrl('https://e/pfx')).toBe('https://e/pfx/apps/nexus');
        expect(appTeardownUrl('https://e/pfx')).toBe('https://e/pfx/apps/nexus');
        expect(appLogsUrl('https://e/pfx', 40)).toBe('https://e/pfx/apps/nexus/logs?lines=40');
        expect(healthUrl('https://e/pfx')).toBe('https://e/pfx/health');
    });

    it('omits config.port by default (auto-allocation) and includes it only when typed', () => {
        expect(buildAppConfig(flagsOf([]))).toEqual({ env: {} });
        expect(buildAppConfig(flagsOf(['--port', '20042']))).toEqual({ port: 20042, env: {} });
        expect(buildAppConfig(flagsOf(['--port', '0']))).toEqual({ env: {} });
        expect(profileFor(flagsOf([]))).toBe('nexus');
        expect(profileFor(flagsOf(['--mode', 'remote-build']))).toBe('manifest');
    });

    it('remote-build supplies NEXUS_LISTEN=$PORT in the manifest runCmd', () => {
        // T4: the manifest profile gets no derived NEXUS_LISTEN (§8.6) and the nexus
        // binary otherwise defaults to 8085, which v2 no longer allocates.
        const config = buildAppConfig(flagsOf(['--mode', 'remote-build']));
        expect(config).toEqual({
            buildCmd: 'cargo build --release --locked -p nexus-server',
            buildTimeoutSec: 1800,
            runCmd: 'NEXUS_LISTEN=127.0.0.1:$PORT ./target/release/nexus-hub',
            env: {},
            probePath: '/v1/observe',
            probeTimeoutSec: 60,
            stopSignal: 'SIGINT',
        });
        // NO artifactPath: §8.2 chmods it right after staging and BEFORE buildCmd
        // (runtime-server/src/runtime.rs: stage_bundle :231 → run_build_step :233;
        // chmod :341-366), so a build output named there fails staging. Cargo's
        // output is already executable and runCmd names it.
        expect('artifactPath' in config).toBe(false);
        // The reserved key is never smuggled through config.env (R-8.3).
        expect((config.env as Record<string, string>).NEXUS_LISTEN).toBeUndefined();
        expect(buildAppConfig(flagsOf(['--mode', 'remote-build', '--port', '20043']))).toMatchObject({ port: 20043 });
    });

    it('keeps host build caches out of the remote-build source tarball', () => {
        // `.rust` is the host cargo target/cache (4.6 GiB measured here → an
        // 874 MB tarball, over the runner's 512 MiB cap, §8.1). Every entry is
        // git-ignored host state, never source; the same list is printed by the
        // dry run, which is the only public surface of the packing step.
        expect(TAR_EXCLUDES).toEqual(['.git', '.rust', '.turbo', 'var', 'frontend', 'target', 'pkg', 'node_modules', 'Temp']);
    });

    it('sends the exact binary-form headers: app name, profile, config, filename, both credentials', () => {
        const args = flagsOf(['--endpoint', 'https://e', '--token', 'preview-token', '--admin-token', 'admin-token', '--env', 'A=1']);
        const config = buildAppConfig(args);
        const headers = buildUploadHeaders(args, config, 'nexus-hub');
        expect(headers).toEqual({
            'content-type': 'application/octet-stream',
            'x-app-name': 'nexus',
            'x-runtime-profile': 'nexus',
            'x-runtime-config': Buffer.from(JSON.stringify({ env: { A: '1' } }), 'utf8').toString('base64url'),
            'x-runtime-filename': 'nexus-hub',
            'x-daytona-preview-token': 'preview-token',
            authorization: 'Bearer admin-token',
        });
    });

    it('sends no auth headers at all when no credential was given', () => {
        const args = flagsOf(['--endpoint', 'https://e']);
        expect(buildUploadHeaders(args, buildAppConfig(args), 'nexus-hub')).toEqual({
            'content-type': 'application/octet-stream',
            'x-app-name': 'nexus',
            'x-runtime-profile': 'nexus',
            'x-runtime-config': Buffer.from(JSON.stringify({ env: {} }), 'utf8').toString('base64url'),
            'x-runtime-filename': 'nexus-hub',
        });
        expect(credentialsOf(args)).toEqual({ previewToken: '', adminToken: '' });
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-daytona: upload outcomes and failure explanations', () => {
    it('reads a create 202 as the accepted revision', () => {
        const view = appView({ status: 'queued', ready: false });
        expect(readUploadOutcome(OK({ accepted: true, app: view }, 202), 'create')).toEqual({ accepted: true, view: expect.objectContaining({ revision: 1, status: 'queued' }), operation: 'create', targetRevision: 1 });
    });

    it('reads a replace 202 as the PENDING revision, not the current one', () => {
        const current = appView({ revision: 1 });
        const outcome = readUploadOutcome(OK({ accepted: true, app: current, pending: { revision: 2, status: 'queued', internalPort: 20043, queue: { position: 1, ahead: 0 }, error: null } }, 202), 'replace');
        expect(outcome).toMatchObject({ accepted: true, operation: 'replace', targetRevision: 2, view: { revision: 1 } });
    });

    it('rejects a replace 202 whose pending revision is missing — old current is never success', () => {
        // §7.7 requires `pending` with the revision the client waits for. Without it
        // the poll would have targetRevision null and could accept the OLD revision.
        const current = appView({ revision: 1, status: 'ready', ready: true });
        expect(readUploadOutcome(OK({ accepted: true, app: current, pending: {} }, 202), 'replace')).toEqual({
            accepted: false,
            reason: 'HTTP 202 accepted the replacement but reported no numeric pending.revision — cannot identify the revision to wait for, and the still-current revision is never success',
        });
        expect(readUploadOutcome(OK({ accepted: true, app: current, pending: null }, 202), 'replace')).toEqual({
            accepted: false,
            reason: 'HTTP 202 accepted the replacement but reported no numeric pending.revision — cannot identify the revision to wait for, and the still-current revision is never success',
        });
        // Non-integer revision is malformed too; create still accepts its app revision.
        expect(readUploadOutcome(OK({ accepted: true, app: current, pending: { revision: '2.5' } }, 202), 'replace')).toMatchObject({ accepted: false });
        expect(readUploadOutcome(OK({ accepted: true, app: appView({ status: 'queued', ready: false }) }, 202), 'create')).toMatchObject({ accepted: true, operation: 'create', targetRevision: 1 });
    });

    it('explains every 409 in operator words (and names the other verb)', () => {
        const msg = (status: number, json: unknown) => explainUploadFailure(ERR(status, json), 'create');
        expect(msg(409, { error: 'app_exists' })).toBe('app "nexus" already exists — re-run with --replace (blue-green PUT /apps/nexus) or --teardown, then deploy again');
        expect(msg(409, { error: 'app_not_replaceable', details: { status: 'building' } })).toBe('app "nexus" is "building" — --replace only accepts a ready or failed app; run --teardown first, then deploy again');
        expect(msg(409, { error: 'replace_in_progress' })).toBe('a replacement of app "nexus" is already in progress — poll GET /apps/nexus until pending is null, then retry');
        expect(msg(400, { error: 'reserved_port' })).toContain('drop --port');
        expect(msg(401, { error: 'unauthorized' })).toContain('--admin-token');
        expect(msg(500, { error: 'internal_error' })).toBe('HTTP 500 internal_error');
    });

    it('treats removed:false as an idempotent teardown success', () => {
        expect(readTeardownOutcome(OK({ tornDown: true, name: 'nexus', removed: false }))).toEqual({ ok: true, removed: false });
        expect(readTeardownOutcome(OK({ tornDown: true, name: 'nexus', removed: true }))).toEqual({ ok: true, removed: true });
        expect(readTeardownOutcome(ERR(404, { error: 'not_found' }))).toEqual({ ok: false, reason: 'HTTP 404 not_found' });
        expect(readTeardownOutcome(OK({ tornDown: false }))).toMatchObject({ ok: false });
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-daytona: the v2 health gate', () => {
    it('accepts apiVersion >= 2 or the apps capability, and refuses the v1 marker', async () => {
        const v2 = makeRequest([OK(V2_HEALTH)]);
        expect(await readV2Gate('https://e', {}, v2.requestImpl)).toMatchObject({ ok: true, health: { contract: 'sandbox-runtime-v2', apiVersion: 2 } });

        // Either signal alone is enough (§17.1).
        const capsOnly = makeRequest([OK({ ...V2_HEALTH, apiVersion: undefined })]);
        expect(await readV2Gate('https://e', {}, capsOnly.requestImpl)).toMatchObject({ ok: true });

        const v1 = makeRequest([OK(V1_HEALTH)]);
        const refused = await readV2Gate('https://e', {}, v1.requestImpl);
        expect(refused.ok).toBe(false);
        if (!refused.ok) {
            expect(refused.message).toContain('not a sandbox runtime v2 runner');
            expect(refused.message).toContain('daytona-runtime-v1');
            expect(refused.hint).toContain('/apps');
        }
    });

    it('sends the preview token (and only that) on the gate request', async () => {
        const fake = makeRequest([OK(V2_HEALTH)]);
        await readV2Gate('https://e', { previewToken: 'pt' }, fake.requestImpl);
        expect(fake.calls).toEqual([{ method: 'GET', url: 'https://e/health', headers: { 'x-daytona-preview-token': 'pt' }, body: undefined }]);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-daytona: the poll state machine (GET /apps/nexus, never /status)', () => {
    const poll = (script: RequestResult[], target: { operation: 'create' | 'replace'; revision: number | null } = { operation: 'create', revision: 1 }, capMs = 60_000) => {
        const fake = makeRequest(script);
        const lines: string[] = [];
        const problems: string[] = [];
        return {
            fake,
            lines,
            problems,
            run: pollAppUntilReady({ endpoint: 'https://e', credentials: { previewToken: 'pt' }, target, capMs, requestImpl: fake.requestImpl, sleepImpl: async () => undefined, logImpl: (line) => lines.push(line), errorImpl: (line) => problems.push(line) }),
        };
    };

    it('walks staging → queued → building → booting → ready and prints only transitions', async () => {
        const { run, lines, fake } = poll([
            OK(appView({ status: 'staging', ready: false })),
            OK(appView({ status: 'queued', ready: false, queue: { position: 1, ahead: 0 } })),
            OK(appView({ status: 'building', ready: false })),
            OK(appView({ status: 'booting', ready: false })),
            OK(appView({ status: 'ready', ready: true })),
        ]);
        const outcome = await run;
        expect(outcome).toMatchObject({ ok: true, view: { status: 'ready', revision: 1, internalPort: 20042 } });
        expect(lines).toEqual([
            'app nexus: revision 1, status staging, internal port 20042',
            'app nexus: revision 1, status queued, internal port 20042, queue 1 (ahead 0)',
            'app nexus: revision 1, status building, internal port 20042',
            'app nexus: revision 1, status booting, internal port 20042',
            'app nexus: revision 1, status ready, internal port 20042, ready',
        ]);
        // Every poll is the named-app route with the preview header, and nothing else.
        expect(fake.calls.map((c) => `${c.method} ${c.url}`)).toEqual(Array(5).fill('GET https://e/apps/nexus'));
        expect(fake.calls[0]!.headers).toEqual({ 'x-daytona-preview-token': 'pt' });
    });

    it('keeps waiting on a probe flap (status ready, ready:false) and on an unknown future status', async () => {
        const { run, lines } = poll([OK(appView({ status: 'ready', ready: false })), OK(appView({ status: 'mystery', ready: false })), OK(appView({ status: 'ready', ready: true }))]);
        expect((await run).ok).toBe(true);
        expect(lines).toContain('app nexus: revision 1, status mystery, internal port 20042');
    });

    it('fails on status "failed" and carries the runner error code', async () => {
        const { run } = poll([OK(appView({ status: 'failed', ready: false, error: 'probe_timeout' }))]);
        expect(await run).toEqual({ ok: false, view: expect.objectContaining({ status: 'failed', error: 'probe_timeout' }), reason: 'app "nexus" failed: probe_timeout' });
    });

    it('does NOT accept the still-ready current revision during a blue-green replace', async () => {
        // The trap: while pending revision 2 boots, the CURRENT record keeps saying
        // ready/rev 1. Returning then would report success before the switch.
        const { run, lines } = poll(
            [
                OK(appView({ revision: 1, status: 'ready', ready: true, pending: { revision: 2, status: 'queued', internalPort: 20043, queue: { position: 1, ahead: 0 }, error: null } })),
                OK(appView({ revision: 1, status: 'ready', ready: true, pending: { revision: 2, status: 'booting', internalPort: 20043, queue: null, error: null } })),
                OK(appView({ revision: 2, status: 'ready', ready: true, pending: null })),
            ],
            { operation: 'replace', revision: 2 },
        );
        expect(await run).toMatchObject({ ok: true, view: { revision: 2, status: 'ready' } });
        expect(lines).toEqual([
            'app nexus: revision 1, status ready, internal port 20042, ready, pending revision 2 queued',
            'app nexus: revision 1, status ready, internal port 20042, ready, pending revision 2 booting',
            'app nexus: revision 2, status ready, internal port 20042, ready',
        ]);
    });

    it('reports a failed replace when the pending revision disappears without becoming current', async () => {
        const { run } = poll(
            [OK(appView({ revision: 1, status: 'ready', ready: true, pending: { revision: 2, status: 'building', internalPort: 20043, queue: null, error: null } })), OK(appView({ revision: 1, status: 'ready', ready: true, pending: null, lastReplaceError: 'build_failed' }))],
            { operation: 'replace', revision: 2 },
        );
        expect(await run).toEqual({ ok: false, view: expect.objectContaining({ revision: 1 }), reason: 'the replacement revision 2 failed — build_failed; the previous revision is still serving' });
    });

    it('keeps waiting through a FAILED current revision while the replacement is pending', async () => {
        // RV1 repro: PUT is legal on failed (§7.7), so current rev1/failed + pending
        // rev2 is OLD state. The failed current must not end the poll; rev2 ready does.
        const { run, lines } = poll(
            [
                OK(appView({ revision: 1, status: 'failed', ready: false, error: 'probe_timeout', pending: { revision: 2, status: 'queued', internalPort: 20043, queue: { position: 1, ahead: 0 }, error: null } })),
                OK(appView({ revision: 1, status: 'failed', ready: false, error: 'probe_timeout', pending: { revision: 2, status: 'booting', internalPort: 20043, queue: null, error: null } })),
                OK(appView({ revision: 2, status: 'ready', ready: true, pending: null })),
            ],
            { operation: 'replace', revision: 2 },
        );
        expect(await run).toMatchObject({ ok: true, view: { revision: 2, status: 'ready' } });
        expect(lines).toEqual([
            'app nexus: revision 1, status failed, internal port 20042, pending revision 2 queued, error probe_timeout',
            'app nexus: revision 1, status failed, internal port 20042, pending revision 2 booting, error probe_timeout',
            'app nexus: revision 2, status ready, internal port 20042, ready',
        ]);
    });

    it('reports the replacement failure when pending clears and the old FAILED revision remains', async () => {
        // Not a generic "app failed": the OLD app was already failed and still serves
        // (failed retains its path, §9.1). The pending revision is the one that failed.
        const { run } = poll(
            [
                OK(appView({ revision: 1, status: 'failed', ready: false, error: 'probe_timeout', pending: { revision: 2, status: 'queued', internalPort: 20043, queue: { position: 1, ahead: 0 }, error: null } })),
                OK(appView({ revision: 1, status: 'failed', ready: false, error: 'probe_timeout', pending: null, lastReplaceError: 'build_failed' })),
            ],
            { operation: 'replace', revision: 2 },
        );
        expect(await run).toEqual({
            ok: false,
            view: expect.objectContaining({ revision: 1, status: 'failed' }),
            // The old record is preserved, but a failed old app is NOT "still serving".
            reason: 'the replacement revision 2 failed — build_failed; the previous revision (status failed) remains registered and is NOT serving',
        });
    });

    it('errors when the new revision became current and then failed', async () => {
        const { run } = poll(
            [
                OK(appView({ revision: 1, status: 'ready', ready: true, pending: { revision: 2, status: 'booting', internalPort: 20043, queue: null, error: null } })),
                OK(appView({ revision: 2, status: 'failed', ready: false, error: 'probe_timeout', pending: null })),
            ],
            { operation: 'replace', revision: 2 },
        );
        expect(await run).toEqual({ ok: false, view: expect.objectContaining({ revision: 2, status: 'failed' }), reason: 'the replacement revision 2 became current and failed: probe_timeout' });
    });

    it('refuses a replace poll with no pending revision instead of accepting the current app', async () => {
        // readUploadOutcome rejects such a 202; a direct caller must not reintroduce
        // the old success-on-old-revision path.
        const { run, fake } = poll([OK(appView({ revision: 1, status: 'ready', ready: true }))], { operation: 'replace', revision: null });
        expect(await run).toEqual({ ok: false, view: expect.objectContaining({ revision: 1 }), reason: 'a replace poll needs the pending revision, but the target was null — refusing to treat the current revision as the replacement' });
        expect(fake.calls).toHaveLength(1);
    });

    it('stops immediately when the app disappears (404), instead of waiting or reporting ready', async () => {
        const { run } = poll([ERR(404, { error: 'app_not_found' })]);
        expect(await run).toEqual({ ok: false, reason: 'GET /apps/nexus returned 404 — the app disappeared while polling (deleted elsewhere?)' });
    });

    it('tolerates a transient fetch miss and keeps polling', async () => {
        const { run, lines } = poll([{ status: 0, ok: false, json: null, text: '', networkError: 'TLS reset' }, OK(appView())]);
        expect((await run).ok).toBe(true);
        expect(lines[0]).toBe('GET /apps/nexus fetch failed (TLS reset) — transient miss 1/5, the app may still be booting');
    });

    it('times out with the last state named, never claiming success', async () => {
        // capMs 0 means the budget is already spent: no poll, and the reason names
        // the target revision so an operator can correlate it with GET /apps/nexus.
        const fake = makeRequest([]);
        const outcome = await pollAppUntilReady({ endpoint: 'https://e', credentials: {}, target: { operation: 'create', revision: 1 }, capMs: 0, requestImpl: fake.requestImpl, sleepImpl: async () => undefined, logImpl: () => undefined });
        expect(outcome).toEqual({ ok: false, reason: 'timed out after 0 min waiting for revision 1 to be ready (last state: never seen)' });
        expect(fake.calls).toEqual([]);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-daytona: log tail', () => {
    it('reads GET /apps/nexus/logs?lines=40 with the credentials, and returns null on a refusal', async () => {
        const ok = makeRequest([{ status: 200, ok: true, json: null, text: 'boot failed' }]);
        expect(await fetchAppLogs('https://e', { previewToken: 'pt', adminToken: 'at' }, 40, ok.requestImpl)).toBe('boot failed');
        expect(ok.calls[0]).toEqual({ method: 'GET', url: 'https://e/apps/nexus/logs?lines=40', headers: { 'x-daytona-preview-token': 'pt', authorization: 'Bearer at' }, body: undefined });

        const denied = makeRequest([ERR(401, { error: 'unauthorized' })]);
        expect(await fetchAppLogs('https://e', {}, 40, denied.requestImpl)).toBeNull();
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-daytona: main() drives the exact v2 sequence', () => {
    const capture = () => {
        const logged: string[] = [];
        const errored: string[] = [];
        vi.spyOn(console, 'log').mockImplementation((...args: unknown[]) => logged.push(args.map(String).join(' ')));
        vi.spyOn(console, 'error').mockImplementation((...args: unknown[]) => errored.push(args.map(String).join(' ')));
        return { logged, errored };
    };
    const argv = (extra: string[] = []) => ['--endpoint', 'https://e.example', '--token', 'pt', '--admin-token', 'at', '--artifact', SCRATCH_ELF, ...extra];

    it('CREATE: GET /health → POST /apps (auto port, app header) → GET /apps/nexus, then prints the deterministic URL', async () => {
        const { logged, errored } = capture();
        const fake = makeRequest([OK(V2_HEALTH), OK({ accepted: true, app: appView({ status: 'queued', ready: false, internalPort: null }) }, 202), OK(appView())]);
        expect(await main(argv(), fake.requestImpl)).toBe(0);
        expect(errored).toEqual([]);
        expect(fake.calls.map((c) => `${c.method} ${c.url}`)).toEqual(['GET https://e.example/health', 'POST https://e.example/apps', 'GET https://e.example/apps/nexus']);
        expect(fake.calls[1]!.headers).toMatchObject({ 'x-app-name': 'nexus', 'x-runtime-profile': 'nexus', 'x-daytona-preview-token': 'pt', authorization: 'Bearer at' });
        // The config header decodes to {env:{}} — NO port key, so the runner allocates.
        expect(JSON.parse(Buffer.from(fake.calls[1]!.headers['x-runtime-config']!, 'base64url').toString('utf8'))).toEqual({ env: {} });
        expect(logged).toContain('✓ app "nexus" is ready — revision 1, status ready, internal port 20042');
        // `log()` prefixes every line with `>>> ` and the two rendered fields start
        // with two spaces, so the exact text is `>>>   path: …`.
        expect(logged).toContain('>>>   path:    /app/nexus/');
        expect(logged).toContain('>>>   hub URL: https://e.example/app/nexus/  ← clients use this base; no per-port preview link');
        // The last line is the redacted AppView + computed URL, parseable as JSON.
        expect(JSON.parse(logged[logged.length - 1]!)).toMatchObject({ hubUrl: 'https://e.example/app/nexus/', app: { name: 'nexus', revision: 1, status: 'ready', internalPort: 20042 } });
    });

    it('REPLACE: PUT /apps/nexus and waits for the PENDING revision, never POST', async () => {
        const { logged } = capture();
        const fake = makeRequest([
            OK(V2_HEALTH),
            OK({ accepted: true, app: appView({ revision: 1 }), pending: { revision: 2, status: 'queued', internalPort: 20043, queue: { position: 1, ahead: 0 }, error: null } }, 202),
            OK(appView({ revision: 2 })),
        ]);
        expect(await main(argv(['--replace']), fake.requestImpl)).toBe(0);
        expect(fake.calls.map((c) => `${c.method} ${c.url}`)).toEqual(['GET https://e.example/health', 'PUT https://e.example/apps/nexus', 'GET https://e.example/apps/nexus']);
        expect(logged).toContain('>>> blue-green: the current revision keeps serving at /app/nexus/ until revision 2 is ready');
        expect(logged).toContain('✓ app "nexus" is ready — revision 2, status ready, internal port 20042');
    });

    it('TEARDOWN: DELETE /apps/nexus and treats removed:false as success', async () => {
        const { logged } = capture();
        const fake = makeRequest([OK(V2_HEALTH), OK({ tornDown: true, name: 'nexus', removed: false })]);
        expect(await main(['--endpoint', 'https://e.example', '--token', 'pt', '--teardown'], fake.requestImpl)).toBe(0);
        expect(fake.calls.map((c) => `${c.method} ${c.url}`)).toEqual(['GET https://e.example/health', 'DELETE https://e.example/apps/nexus']);
        expect(logged).toContain('✓ no app named "nexus" — nothing to remove (removed:false); the sandbox keeps running');
    });

    it('a failed app fetches GET /apps/nexus/logs?lines=40 and exits 1', async () => {
        const { logged, errored } = capture();
        const fake = makeRequest([OK(V2_HEALTH), OK({ accepted: true, app: appView({ status: 'queued', ready: false }) }, 202), OK(appView({ status: 'failed', ready: false, error: 'probe_timeout' })), { status: 200, ok: true, json: null, text: 'listen EADDRINUSE' }]);
        expect(await main(argv(), fake.requestImpl)).toBe(1);
        expect(fake.calls[3]!.url).toBe('https://e.example/apps/nexus/logs?lines=40');
        expect(errored).toContain('✗ app "nexus" failed: probe_timeout');
        expect(errored.join('\n')).toContain('listen EADDRINUSE');
        expect(logged.some((line) => line.startsWith('✓ app "nexus" is ready'))).toBe(false);
    });

    it('--dry-run prints the v2 request and touches neither the network nor cargo', async () => {
        const { logged } = capture();
        const fake = makeRequest([]);
        expect(await main(['--endpoint', 'https://e.example', '--token', 'secret', '--artifact', SCRATCH_ELF, '--dry-run'], fake.requestImpl)).toBe(0);
        expect(fake.calls).toEqual([]);
        const text = logged.join('\n');
        expect(text).toContain('dry run — mode local, profile nexus, app nexus');
        expect(text).toContain('POST https://e.example/apps');
        expect(text).toContain('x-app-name: nexus');
        expect(text).toContain('x-daytona-preview-token: *** (length 6)');
        expect(text).toContain('config.port: ABSENT — the runner auto-allocates a loopback port (v2 default)');
        expect(text).toContain('GET https://e.example/apps/nexus');
        expect(text).not.toContain('secret');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-daytona: usage and the import-is-a-no-op guard', () => {
    it('USAGE names the v2 routes and drops the v1 slot language', () => {
        expect(USAGE).toContain('POST /apps');
        expect(USAGE).toContain('PUT /apps/nexus');
        expect(USAGE).toContain('DELETE /apps/nexus');
        expect(USAGE).toContain('AUTO-ALLOCATES');
        expect(USAGE).toContain('--admin-token');
        expect(USAGE).not.toContain('/status');
    });

    it('isCliEntryPoint: vitest never runs, and the three entry rules are exact', () => {
        const module = '/repo/packages/typecrate/nexus/scripts/deploy-daytona.ts';
        expect(isCliEntryPoint({ vitest: true, argv: ['/usr/bin/node', module], modulePath: module })).toBe(false);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', module], modulePath: module })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node', '--script', 'scripts/deploy-daytona.ts'], modulePath: module })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], env: { npm_lifecycle_script: 'vite-node scripts/deploy-daytona.ts' }, modulePath: module })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], modulePath: module, stack: `Error\n    at Object.<anonymous> (${module}:1:1)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], modulePath: module, stack: `Error\n    at Object.<anonymous> (${module}:1:1)\n    at consumer (/repo/x.ts:2:2)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(false);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node', '@services/daytona/sandbox'], modulePath: module })).toBe(false);
        expect(isCliEntryPoint({})).toBe(false);
    });
});
