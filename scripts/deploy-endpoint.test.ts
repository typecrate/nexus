// ─────────────────────────────────────────────────────────────────────────────
//  deploy-endpoint.test.ts — the platform-neutral v2 deploy verb, end to end,
//  with ZERO network and no cargo.
//
//  Everything side-effectful in deploy-endpoint.ts is an injected dep (probe /
//  spawn / sleep / now / log / error / env), so these tests drive the REAL control
//  flow against scripted endpoint answers and assert exact values:
//    · parser     — defaults (AUTO port, no admin token), the positional URL, every
//                   flag, --env repeats, env fallbacks/precedence, illegal combos
//    · probes     — classifyProbe's cold-start/auth/dead table, and the v2-specific
//                   classifyRunnerHealth / classifyAppProbe tables
//    · app policy — ready|failed → PUT (blue-green), queued|building|… → app-scoped
//                   DELETE then POST, --no-teardown → conflict, pending → conflict
//    · hub URL    — AppView.path + endpoint, publicUrl fallback, absent
//    · argv       — the exact child argv (no --port unless typed, --replace when PUT)
//    · flow       — cold start, empty registry, replace, non-replaceable states,
//                   teardown (DELETE + verify 404), child failure, dry run
//    · modal      — container recycle is OPT-IN: never implied by the replace
//                   posture, only DELETE /_provider/sandbox, box-only, refused for
//                   a sandbox tunnel, and proven by a young uptime + absent app
//    · spawnDeploy— the child wiring through the SpawnImpl seam
//    · import     — isCliEntryPoint's three entry rules
// ─────────────────────────────────────────────────────────────────────────────

import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { modalContainer } from '@config/vault';

import {
    appDeleteUrl,
    appStateUrl,
    authHeaders,
    buildDeployArgs,
    buildDeployInvocation,
    buildHubUrl,
    buildSummary,
    classifyAppProbe,
    classifyModalContainer,
    classifyProbe,
    classifyRunnerHealth,
    createDefaultDeps,
    credentialsOf,
    deleteApp,
    DEPLOY_SCRIPT_PATH,
    ENDPOINT_WAIT_MS,
    EndpointDeployError,
    healthUrl,
    isCliEntryPoint,
    joinUrl,
    main,
    PACKAGE_ROOT,
    parseArgs,
    readApp,
    RECYCLE_WAIT_MS,
    requestModalContainerReset,
    redactSecretArgs,
    requireEndpoint,
    resetModalBoxContainer,
    resolveMode,
    runDeployEndpoint,
    SLOT_WAIT_MS,
    spawnDeploy,
    USAGE,
    verifyAppAbsent,
    VITE_NODE_CLI,
    waitForModalContainerReset,
    waitForRoute,
    type EndpointDeps,
    type EndpointFlags,
    type Probe,
    type ProbeResult,
} from './deploy-endpoint';

// ══════════════════════════════════════════════════════════════════════════════
//  FAKES
// ══════════════════════════════════════════════════════════════════════════════

const OK = (json: unknown): ProbeResult => ({ status: 200, ok: true, json, text: JSON.stringify(json) });
const FAIL = (status: number, json: unknown = null, text = ''): ProbeResult => ({ status, ok: false, json, text: text || (json ? JSON.stringify(json) : '') });
const DOWN = (message = 'connect ECONNREFUSED 127.0.0.1:7777'): ProbeResult => ({ status: 0, ok: false, json: null, text: '', transportError: message });

/** The body a v2 runner answers /health with (§7.2/§17.1). */
const HEALTH = {
    ok: true,
    slot: 'multi',
    uptimeSec: 4,
    contract: 'sandbox-runtime-v2',
    apiVersion: 2,
    compatibleContracts: ['daytona-runtime-v1'],
    capabilities: ['apps', 'app-proxy', 'blue-green-replace'],
    runnerPort: 7777,
    publicOrigin: 'https://box.example',
    authRequired: false,
    limits: { maxApps: 8 },
    apps: { count: 0, ready: 0, pending: 0 },
};
/** The v1 single-slot marker: no apiVersion 2 and no `apps` capability. */
const V1_HEALTH = { status: 'ok', contract: 'daytona-runtime-v1', runnerPort: 7777, authenticated: false, exposedPorts: [8085] };
/** The Modal runner's /health: v2 plus the `service` marker that makes a box recognizable. */
const MODAL_HEALTH = { ...HEALTH, service: 'modal-container-runtime' };
/** A long-lived box's /health — its uptime predates any reset, so it cannot pass as a replacement. */
const MODAL_OLD_HEALTH = { ...MODAL_HEALTH, uptimeSec: 600 };
/** A Modal box is a stable deployment URL; a sandbox tunnel's URL dies with its container. */
const MODAL_BOX = Object.values(modalContainer)[0]!;
const MODAL_SANDBOX = 'https://abc123-7777-deadbeef.modal.host';
const BOX = 'https://box.example';

/** A redacted AppView (§5.2). */
const APP = (over: Record<string, unknown> = {}) => ({
    name: 'nexus',
    revision: 1,
    path: '/app/nexus/',
    publicUrl: `${BOX}/app/nexus/`,
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
const ABSENT = FAIL(404, { error: 'app_not_found' });
const TORN_DOWN = (removed = true) => OK({ tornDown: true, name: 'nexus', removed });

/** One probe call, as recorded. */
type Call = { method: string; url: string; token?: string; admin?: string };

/**
 * A scripted endpoint: answers each call from `script` in call order, and records
 * every call so a test can pin the whole request sequence. A function entry sees
 * the call, for the few branches that depend on the URL.
 */
const makeProbe = (script: Array<ProbeResult | ((call: Call) => ProbeResult)>) => {
    const calls: Call[] = [];
    let index = 0;
    const probe: Probe = async (method, url, init) => {
        const call: Call = {
            method,
            url,
            ...(init.headers['x-daytona-preview-token'] ? { token: init.headers['x-daytona-preview-token'] } : {}),
            ...(init.headers.authorization ? { admin: init.headers.authorization } : {}),
        };
        calls.push(call);
        const next = script[index++];
        if (next === undefined) throw new Error(`probe script exhausted at call ${index}: ${method} ${url}`);
        return typeof next === 'function' ? next(call) : next;
    };
    return { probe, calls };
};

/**
 * Deps with a MOVABLE clock and no waiting: `sleep(ms)` advances the clock, so a
 * bounded retry loop terminates deterministically and the attempt count is exact.
 */
const makeDeps = (probe: Probe, options: { spawnCode?: number } = {}) => {
    const lines: string[] = [];
    const problems: string[] = [];
    const invocations: Array<{ command: string; args: string[]; cwd: string; env: Record<string, string | undefined> }> = [];
    let nowMs = 0;
    const deps: EndpointDeps = {
        probe,
        spawn: async (invocation) => {
            invocations.push(invocation);
            return options.spawnCode ?? 0;
        },
        log: (line) => lines.push(line),
        error: (line) => problems.push(line),
        env: {},
        sleep: async (ms) => {
            nowMs += ms;
        },
        now: () => nowMs,
    };
    return { deps, lines, problems, invocations };
};

/** parseArgs with no env noise, so defaults come from the parser itself. */
const flagsOf = (argv: readonly string[]): EndpointFlags => {
    const { flags, errors } = parseArgs(argv, {});
    expect(errors).toEqual([]);
    return flags;
};
const bareFlags = (): EndpointFlags => flagsOf([BOX]);

afterEach(() => vi.restoreAllMocks());

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: module pins', () => {
    it('resolves the sibling script inside the package root, and it exists', () => {
        expect(PACKAGE_ROOT.endsWith(join('packages', 'typecrate', 'nexus'))).toBe(true);
        expect(DEPLOY_SCRIPT_PATH).toBe(join(PACKAGE_ROOT, 'scripts', 'deploy-daytona.ts'));
        expect(existsSync(DEPLOY_SCRIPT_PATH)).toBe(true);
    });

    it('keeps the wait budgets long enough for a cold container', () => {
        expect(ENDPOINT_WAIT_MS).toBe(90_000);
        expect(SLOT_WAIT_MS).toBe(30_000);
        expect(RECYCLE_WAIT_MS).toBe(60_000);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: argument parsing', () => {
    it('defaults to a local deploy with AUTO port, replace posture and no credentials', () => {
        expect(bareFlags()).toEqual({
            help: false,
            dryRun: false,
            teardownOnly: false,
            replace: true,
            recycleContainer: false,
            endpoint: BOX,
            token: '',
            adminToken: '',
            mode: 'local',
            port: undefined,
            env: {},
            noBuild: false,
            artifact: '',
            wait: true,
        });
    });

    it('parses every flag into the exact record', () => {
        // --no-teardown and --recycle-container are mutually exclusive, so each is
        // exercised in its own flag set (see the conflict test below).
        const flags = flagsOf([BOX, '--token', 'pt', '--admin-token', 'at', '--mode', 'remote-build', '--port', '20042', '--env', 'A=1', '--no-wait', '--no-teardown']);
        expect(flags).toMatchObject({ endpoint: BOX, token: 'pt', adminToken: 'at', mode: 'remote-build', port: 20042, env: { A: '1' }, wait: false, replace: false });
        expect(flagsOf([BOX, '--recycle-container']).recycleContainer).toBe(true);
    });

    it('takes --endpoint, and a positional that agrees with it is not an error', () => {
        expect(parseArgs([BOX, '--endpoint', BOX], {}).errors).toEqual([]);
    });

    it('rejects two different endpoints, because picking one silently could deploy to the wrong box', () => {
        expect(parseArgs([BOX, '--endpoint', 'https://other.example'], {}).errors).toEqual([`two different endpoints given: "${BOX}" and --endpoint "https://other.example"`]);
    });

    it('rejects a second positional', () => {
        expect(parseArgs([BOX, 'https://other.example'], {}).errors).toEqual([`unexpected extra argument "https://other.example" — the endpoint is the only positional (got "${BOX}" already)`]);
    });

    it('requires an endpoint — there is no platform default, that is the whole point', () => {
        expect(parseArgs([], {}).errors).toEqual(['no endpoint — pass it as the argument (deploy:endpoint <url>) or with --endpoint / NEXUS_ENDPOINT']);
    });

    it('reads endpoint/token/admin from env, with the platform-neutral names winning', () => {
        const env = { NEXUS_ENDPOINT: BOX, NEXUS_DAYTONA_ENDPOINT: 'https://old.example', NEXUS_ENDPOINT_TOKEN: 'new', NEXUS_DAYTONA_PREVIEW_TOKEN: 'old', NEXUS_SANDBOX_ADMIN_TOKEN: 'admin' };
        expect(parseArgs([], env).flags).toMatchObject({ endpoint: BOX, token: 'new', adminToken: 'admin' });
        // Empty nexus-specific admin name falls through to the generic one.
        expect(parseArgs([], { ...env, NEXUS_SANDBOX_ADMIN_TOKEN: '', SANDBOX_ADMIN_TOKEN: 'generic' }).flags.adminToken).toBe('generic');
    });

    it('maps the app verbs onto their independent switches, and rejects contradictory pairs', () => {
        expect(resolveMode(flagsOf([BOX]))).toBe('deploy');
        expect(resolveMode(flagsOf([BOX, '--teardown']))).toBe('teardown');
        // --teardown ships nothing, so shipping knobs would be silently ignored.
        expect(parseArgs([BOX, '--teardown', '--env', 'A=1'], {}).errors).toEqual(['--teardown ships nothing, so --env/--no-build/--artifact/--no-wait do not apply (drop them, or drop --teardown)']);
        // A container recycle is much wider than an app teardown: pick one.
        expect(parseArgs([BOX, '--teardown', '--recycle-container'], {}).errors).toEqual(['--teardown removes app "nexus" only; --recycle-container discards the whole Modal box — pick one']);
        expect(parseArgs([BOX, '--no-teardown', '--recycle-container'], {}).errors).toEqual(['--no-teardown forbids touching the endpoint, and --recycle-container destroys the whole box — pick one']);
    });

    it('accumulates repeatable --env, splitting on the first =', () => {
        const flags = flagsOf([BOX, '--env', 'B=2', '--env', 'A=1', '--env', 'TOKEN=a=b']);
        expect(Object.keys(flags.env)).toEqual(['B', 'A', 'TOKEN']);
        expect(flags.env).toEqual({ B: '2', A: '1', TOKEN: 'a=b' });
    });

    it('passes the deploy knobs through, with --port 0 meaning AUTO', () => {
        expect(flagsOf([BOX, '--mode', 'remote-build', '--port', '20042'])).toMatchObject({ mode: 'remote-build', port: 20042 });
        expect(flagsOf([BOX, '--no-build', '--artifact', '/tmp/x'])).toMatchObject({ noBuild: true, artifact: '/tmp/x' });
        expect(flagsOf([BOX, '--port', '0']).port).toBeUndefined();
    });

    it('rejects malformed values instead of guessing', () => {
        expect(parseArgs([BOX, '--port', '70000'], {}).errors).toEqual(['--port "70000" is out of range (TCP port 0-65535, 0 = auto)']);
        expect(parseArgs([BOX, '--mode', 'weird'], {}).errors).toEqual(['--mode must be "local" or "remote-build", got "weird"']);
        expect(parseArgs([BOX, '--mode', 'remote-build', '--no-build'], {}).errors).toEqual(['--no-build and --artifact apply to --mode local only (remote-build ships a source tarball)']);
        expect(parseArgs([BOX, '--env', 'NOPE'], {}).errors).toEqual(['--env expects KEY=VALUE, got "NOPE"']);
    });

    it('reports a missing value without treating the next flag as one', () => {
        expect(parseArgs([BOX, '--token', '--dry-run'], {}).errors).toEqual(['--token needs a value']);
    });

    it('collects every problem in one pass, and --help still wins over a broken line', () => {
        expect(parseArgs([], {}).errors).toEqual(['no endpoint — pass it as the argument (deploy:endpoint <url>) or with --endpoint / NEXUS_ENDPOINT']);
        expect(parseArgs(['--help'], {}).flags.help).toBe(true);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: URL joining and auth headers (must match deploy-daytona.ts)', () => {
    it('joins routes onto the base without clobbering a path prefix', () => {
        expect(joinUrl('https://e.example', 'health')).toBe('https://e.example/health');
        expect(joinUrl('https://e.example/pfx', 'apps/nexus')).toBe('https://e.example/pfx/apps/nexus');
        expect(healthUrl('https://e.example/pfx')).toBe('https://e.example/pfx/health');
        expect(appStateUrl('https://e.example/pfx')).toBe('https://e.example/pfx/apps/nexus');
        expect(appDeleteUrl('https://e.example/pfx')).toBe('https://e.example/pfx/apps/nexus');
    });

    it('sends the token header only when there is a token, so public endpoints keep working', () => {
        expect(authHeaders('')).toEqual({});
        expect(authHeaders('t')).toEqual({ 'x-daytona-preview-token': 't' });
        expect(credentialsOf(bareFlags())).toEqual({ previewToken: '', adminToken: '' });
    });

    it('redactSecretArgs masks only the value after --token/--admin-token', () => {
        expect(redactSecretArgs(['--endpoint', 'https://e', '--token', 't', '--admin-token', 'a', '--mode', 'local'])).toEqual(['--endpoint', 'https://e', '--token', '***', '--admin-token', '***', '--mode', 'local']);
        expect(redactSecretArgs(['--mode', 'local'])).toEqual(['--mode', 'local']);
    });

    it('validates the endpoint where it is first needed', () => {
        expect(requireEndpoint(BOX)).toBe(BOX);
        expect(() => requireEndpoint('')).toThrow(EndpointDeployError);
        expect(() => requireEndpoint('not a url')).toThrow('endpoint is not a valid absolute URL: "not a url"');
        expect(() => requireEndpoint('ftp://x')).toThrow('endpoint must be http(s), got "ftp:"');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: probe tables', () => {
    it('classifyProbe: retryable cold starts, fatal auth, fatal wrong service', () => {
        expect(classifyProbe(OK({}))).toBe('ready');
        for (const status of [408, 429, 500, 502, 503]) expect(classifyProbe(FAIL(status))).toBe('retry');
        expect(classifyProbe(DOWN())).toBe('retry');
        expect(classifyProbe(FAIL(401))).toBe('auth');
        expect(classifyProbe(FAIL(403))).toBe('auth');
        expect(classifyProbe(FAIL(404))).toBe('dead');
        // A 2xx that is not JSON is a DIFFERENT service, not a runner.
        expect(classifyProbe({ status: 200, ok: true, json: null, text: '<html>' })).toBe('dead');
    });

    it('classifyRunnerHealth requires apiVersion >= 2 OR capabilities [apps]', () => {
        expect(classifyRunnerHealth(HEALTH)).toEqual({ ok: true, health: expect.objectContaining({ contract: 'sandbox-runtime-v2', apiVersion: 2 }) });
        expect(classifyRunnerHealth({ ...HEALTH, apiVersion: undefined }).ok).toBe(true);
        const v1 = classifyRunnerHealth(V1_HEALTH);
        expect(v1.ok).toBe(false);
        if (!v1.ok) {
            expect(v1.message).toContain('not a sandbox runtime v2 runner');
            expect(v1.message).toContain('daytona-runtime-v1');
        }
    });

    it('classifyAppProbe: 404 is the only "absent"; a non-AppView 2xx is fatal', () => {
        expect(classifyAppProbe(OK(APP()))).toBe('present');
        expect(classifyAppProbe(ABSENT)).toBe('absent');
        expect(classifyAppProbe(FAIL(401))).toBe('auth');
        expect(classifyAppProbe(DOWN())).toBe('retry');
        expect(classifyAppProbe(FAIL(502))).toBe('retry');
        expect(classifyAppProbe(FAIL(400, { error: 'invalid_app_name' }))).toBe('dead');
        expect(classifyAppProbe(OK({ hello: 'world' }))).toBe('dead');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the hub URL is AppView.path + endpoint, never a port guess', () => {
    it('joins the path onto the known endpoint (preserving a prefix)', () => {
        expect(buildHubUrl({ present: true, view: { ...APP(), path: '/app/nexus/' } as never }, 'https://e.example/pfx')).toEqual({ hubUrl: 'https://e.example/pfx/app/nexus/', note: 'endpoint + /app/nexus/ (the only public app URL)' });
    });

    it('falls back to the server publicUrl when a body carries no path', () => {
        expect(buildHubUrl({ present: true, view: { ...APP(), path: '', publicUrl: 'https://server.example/app/nexus/' } as never }, BOX).hubUrl).toBe('https://server.example/app/nexus/');
    });

    it('an absent app names null instead of guessing a URL', () => {
        expect(buildHubUrl({ present: false }, BOX)).toEqual({ hubUrl: null, note: 'app "nexus" is not registered' });
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the child invocation', () => {
    it('builds the exact deploy-daytona.ts argv, omitting --port unless typed', () => {
        expect(buildDeployArgs(flagsOf([BOX, '--token', 'pt', '--admin-token', 'at', '--env', 'A=1', '--no-wait']))).toEqual([
            '--endpoint', BOX,
            '--token', 'pt',
            '--admin-token', 'at',
            '--mode', 'local',
            '--replace',
            '--env', 'A=1',
            '--no-wait',
        ]);
        expect(buildDeployArgs(flagsOf([BOX, '--port', '20042', '--no-teardown', '--no-build', '--artifact', '/tmp/x']))).toEqual([
            '--endpoint', BOX,
            '--mode', 'local',
            '--port', '20042',
            '--no-build',
            '--artifact', '/tmp/x',
        ]);
    });

    it('wraps it in the vite-node child that the house runner requires', () => {
        const invocation = buildDeployInvocation(bareFlags(), { PATH: '/usr/bin' });
        expect(invocation.command).toBe(process.execPath);
        expect(invocation.command).not.toBe('node');
        expect(invocation.args[0]).toBe(VITE_NODE_CLI);
        expect(invocation.args[1]).toBe(DEPLOY_SCRIPT_PATH);
        expect(invocation.cwd).toBe(PACKAGE_ROOT);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the machine-readable line', () => {
    it('is one JSON object with v2 fields and no v1 slot language', () => {
        const line = buildSummary({ mode: 'deploy', endpoint: BOX, hubUrl: `${BOX}/app/nexus/`, status: 'ready', revision: 2, internalPort: 20042, token: 'pt' });
        expect(line).toBe('{"mode":"deploy","endpoint":"https://box.example","app":"nexus","path":"/app/nexus/","hubUrl":"https://box.example/app/nexus/","status":"ready","revision":2,"internalPort":20042,"token":"pt","tokenScope":"management"}');
        expect(line).not.toContain('slot');
    });

    it('records a teardown as removed:true|false, with the app path', () => {
        expect(buildSummary({ mode: 'teardown', endpoint: BOX, removed: false })).toBe('{"mode":"teardown","endpoint":"https://box.example","app":"nexus","path":"/app/nexus/","removed":false}');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: waitForRoute (the cold-start budget)', () => {
    const credentials = { previewToken: 'pt' };

    it('returns the first ready answer without waiting', async () => {
        const fake = makeProbe([OK(HEALTH)]);
        const { deps } = makeDeps(fake.probe);
        const result = await waitForRoute({ method: 'GET', url: 'https://e/health', credentials, what: 'GET /health', budgetMs: ENDPOINT_WAIT_MS, deps });
        expect(result.json).toEqual(HEALTH);
        expect(fake.calls).toEqual([{ method: 'GET', url: 'https://e/health', token: 'pt' }]);
    });

    it('retries transport errors and 5xx, and says so once', async () => {
        const fake = makeProbe([DOWN(), FAIL(502), OK(HEALTH)]);
        const { deps, lines } = makeDeps(fake.probe);
        expect((await waitForRoute({ method: 'GET', url: 'https://e/health', credentials: {}, what: 'GET /health', budgetMs: ENDPOINT_WAIT_MS, deps })).ok).toBe(true);
        expect(fake.calls).toHaveLength(3);
        expect(lines).toEqual(['[nexus:endpoint] GET /health is not answering yet (connect ECONNREFUSED 127.0.0.1:7777) — waiting up to 90s while the container comes up…']);
    });

    it('aborts immediately on a token problem, naming the flag that fixes it', async () => {
        const fake = makeProbe([FAIL(401, { error: 'unauthorized' })]);
        const { deps } = makeDeps(fake.probe);
        await expect(waitForRoute({ method: 'GET', url: 'https://e/health', credentials: {}, what: 'GET /health', budgetMs: ENDPOINT_WAIT_MS, deps })).rejects.toThrow('Pass --token <the Daytona preview token>');
        expect(fake.calls).toHaveLength(1);
    });

    it('aborts immediately when the answer is not a v2 runner route', async () => {
        const fake = makeProbe([FAIL(404)]);
        const { deps } = makeDeps(fake.probe);
        await expect(waitForRoute({ method: 'GET', url: 'https://e/health', credentials: {}, what: 'GET /health', budgetMs: ENDPOINT_WAIT_MS, deps })).rejects.toThrow('that is not a sandbox runtime v2 runner');
        expect(fake.calls).toHaveLength(1);
    });

    it('gives up within its budget when the endpoint never answers', async () => {
        const fake = makeProbe(Array.from({ length: Math.ceil(ENDPOINT_WAIT_MS / 2000) }, () => DOWN()));
        const { deps } = makeDeps(fake.probe);
        await expect(waitForRoute({ method: 'GET', url: 'https://e/health', credentials: {}, what: 'GET /health', budgetMs: ENDPOINT_WAIT_MS, deps })).rejects.toThrow('never answered at https://e/health');
        expect(fake.calls).toHaveLength(Math.ceil(ENDPOINT_WAIT_MS / 2000));
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the named-app teardown', () => {
    it('DELETE /apps/nexus treats removed:false as success, then verifies 404', async () => {
        const fake = makeProbe([TORN_DOWN(false), ABSENT]);
        const { deps } = makeDeps(fake.probe);
        expect(await deleteApp(BOX, {}, deps)).toEqual({ removed: false });
        await verifyAppAbsent(BOX, {}, deps);
        expect(fake.calls.map((c) => `${c.method} ${c.url}`)).toEqual(['DELETE https://box.example/apps/nexus', 'GET https://box.example/apps/nexus']);
    });

    it('fails when the DELETE does not confirm tornDown', async () => {
        const fake = makeProbe([OK({ tornDown: false })]);
        const { deps } = makeDeps(fake.probe);
        await expect(deleteApp(BOX, {}, deps)).rejects.toThrow('did not confirm the teardown');
    });

    it('fails when the record survives the verified 404 check', async () => {
        const fake = makeProbe([OK(APP({ status: 'draining' }))]);
        const { deps } = makeDeps(fake.probe);
        await expect(verifyAppAbsent(BOX, {}, deps)).rejects.toThrow('still reports "draining"');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the Modal container recycle is explicit and box-only', () => {
    it('classifies a Modal box and a Modal sandbox, and leaves everything else alone', () => {
        expect(classifyModalContainer(MODAL_BOX, MODAL_HEALTH)).toBe('box');
        expect(classifyModalContainer(MODAL_SANDBOX, MODAL_HEALTH)).toBe('sandbox');
        // Without the service marker, or without a Modal host, it is not ours to reset.
        expect(classifyModalContainer(MODAL_BOX, HEALTH)).toBeNull();
        expect(classifyModalContainer(BOX, MODAL_HEALTH)).toBeNull();
    });

    it('uses DELETE /_provider/sandbox — never the v1 root /sandbox route', async () => {
        const fake = makeProbe([OK({ tornDown: true })]);
        const { deps } = makeDeps(fake.probe);
        await requestModalContainerReset(MODAL_BOX, { previewToken: 'pt' }, deps);
        expect(fake.calls[0]).toMatchObject({ method: 'DELETE', url: `${MODAL_BOX}/_provider/sandbox`, token: 'pt' });
    });

    it('fails closed: a 404 says the adapter lacks the v2 route, a 500 refuses to pretend', async () => {
        const missing = makeProbe([FAIL(404)]);
        await expect(requestModalContainerReset(MODAL_BOX, {}, makeDeps(missing.probe).deps)).rejects.toThrow('does not implement DELETE /_provider/sandbox');
        const broken = makeProbe([FAIL(500, null, 'runner exploded')]);
        await expect(requestModalContainerReset(MODAL_BOX, {}, makeDeps(broken.probe).deps)).rejects.toThrow('without DELETE /_provider/sandbox the container cannot be recycled');
    });

    it('waits for a NEW process (young uptime) whose registry no longer knows nexus', async () => {
        const fake = makeProbe([OK(MODAL_OLD_HEALTH), OK(MODAL_HEALTH), ABSENT]);
        const { deps } = makeDeps(fake.probe);
        expect(await waitForModalContainerReset(MODAL_BOX, {}, deps, 0)).toEqual({ present: false });
        expect(fake.calls.map((c) => `${c.method} ${c.url}`)).toEqual([`GET ${MODAL_BOX}/health`, `GET ${MODAL_BOX}/health`, `GET ${MODAL_BOX}/apps/nexus`]);
    });

    it('resetModalBoxContainer sends the DELETE then proves the replacement', async () => {
        const fake = makeProbe([OK({ tornDown: true }), OK(MODAL_HEALTH), ABSENT]);
        const { deps } = makeDeps(fake.probe);
        expect(await resetModalBoxContainer(MODAL_BOX, {}, deps)).toEqual({ present: false });
        expect(fake.calls.map((c) => `${c.method} ${c.url}`)).toEqual([`DELETE ${MODAL_BOX}/_provider/sandbox`, `GET ${MODAL_BOX}/health`, `GET ${MODAL_BOX}/apps/nexus`]);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the deploy flow', () => {
    const flags = (extra: string[] = []) => flagsOf([BOX, ...extra]);

    it('waits out a cold start, finds NO app, POSTs, and reports the v2 URL', async () => {
        const fake = makeProbe([OK(HEALTH), ABSENT, OK(APP())]);
        const { deps, lines, problems, invocations } = makeDeps(fake.probe);
        expect(await runDeployEndpoint(flags(), deps)).toBe(0);
        expect(problems).toEqual([]);
        expect(fake.calls.map((c) => `${c.method} ${c.url}`)).toEqual(['GET https://box.example/health', 'GET https://box.example/apps/nexus', 'GET https://box.example/apps/nexus']);
        // CREATE: the child gets no --replace and no --port (auto-allocate).
        expect(invocations[0]!.args).toEqual([VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, '--endpoint', BOX, '--mode', 'local']);
        expect(lines).toContain('[nexus:endpoint] app nexus: absent (POST /apps will create it)');
        expect(lines).toContain('[nexus:endpoint] ✓ sandbox runtime v2 — sandbox-runtime-v2, apiVersion 2, capabilities [apps, app-proxy, blue-green-replace], runner port 7777, public origin https://box.example, auth none');
        expect(lines).toContain('[nexus:endpoint] ✓ app nexus — revision 1, status ready, internal port 20042, ready');
        expect(lines).toContain('[nexus:endpoint]   hub (clients):    https://box.example/app/nexus/');
        // LAST line, bare single-line JSON.
        expect(JSON.parse(lines[lines.length - 1]!)).toEqual({ mode: 'deploy', endpoint: BOX, app: 'nexus', path: '/app/nexus/', hubUrl: 'https://box.example/app/nexus/', status: 'ready', revision: 1, internalPort: 20042 });
    });

    it('REPLACES a ready app with PUT (blue-green), never a container reset', async () => {
        const fake = makeProbe([OK(HEALTH), OK(APP({ revision: 1 })), OK(APP({ revision: 2 }))]);
        const { deps, lines, invocations } = makeDeps(fake.probe);
        expect(await runDeployEndpoint(flags(['--token', 'pt']), deps)).toBe(0);
        expect(invocations[0]!.args).toEqual([VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, '--endpoint', BOX, '--token', 'pt', '--mode', 'local', '--replace']);
        expect(lines).toContain('[nexus:endpoint] app nexus is ready — PUT https://box.example/apps/nexus (blue-green; revision 1 keeps serving until the new one is ready)');
        expect(JSON.parse(lines[lines.length - 1]!)).toMatchObject({ revision: 2, replaced: { status: 'ready', revision: 1 } });
        // No provider route was touched on a non-Modal endpoint.
        expect(fake.calls.some((call) => call.url.includes('/_provider/'))).toBe(false);
    });

    it('a failed app is also replaceable with PUT', async () => {
        const fake = makeProbe([OK(HEALTH), OK(APP({ status: 'failed', ready: false, error: 'process_exited' })), OK(APP({ revision: 2 }))]);
        const { deps, invocations } = makeDeps(fake.probe);
        expect(await runDeployEndpoint(flags(), deps)).toBe(0);
        expect(invocations[0]!.args).toContain('--replace');
    });

    it('a queued|building|booting app cannot be PUT: it is deleted APP-SCOPED, then POSTed', async () => {
        const fake = makeProbe([OK(HEALTH), OK(APP({ status: 'building' })), TORN_DOWN(true), ABSENT, OK(APP({ revision: 2 }))]);
        const { deps, lines, invocations } = makeDeps(fake.probe);
        expect(await runDeployEndpoint(flags(), deps)).toBe(0);
        expect(fake.calls.map((c) => `${c.method} ${c.url}`)).toEqual([
            'GET https://box.example/health',
            'GET https://box.example/apps/nexus',
            'DELETE https://box.example/apps/nexus',
            'GET https://box.example/apps/nexus',
            'GET https://box.example/apps/nexus',
        ]);
        expect(invocations[0]!.args).not.toContain('--replace');
        expect(lines).toContain('[nexus:endpoint] app nexus is "building", which §6.2 does not allow a PUT to replace — deleting just this app first (DELETE https://box.example/apps/nexus)');
    });

    it('--no-teardown refuses an existing app before any delete or child', async () => {
        const fake = makeProbe([OK(HEALTH), OK(APP({ status: 'building' }))]);
        const { deps, invocations } = makeDeps(fake.probe);
        await expect(runDeployEndpoint(flags(['--no-teardown']), deps)).rejects.toThrow('app "nexus" already exists (building) and --no-teardown forbids replacing it');
        expect(fake.calls.map((c) => `${c.method} ${c.url}`)).toEqual(['GET https://box.example/health', 'GET https://box.example/apps/nexus']);
        expect(invocations).toEqual([]);
    });

    it('a pending replace is a conflict, never a delete of the in-flight revision', async () => {
        const fake = makeProbe([OK(HEALTH), OK(APP({ pending: { revision: 2, status: 'booting', internalPort: 20043, queue: null, error: null } }))]);
        const { deps, invocations } = makeDeps(fake.probe);
        await expect(runDeployEndpoint(flags(), deps)).rejects.toThrow('a replacement of app "nexus" is already in progress');
        expect(invocations).toEqual([]);
        expect(fake.calls.some((c) => c.method === 'DELETE')).toBe(false);
    });

    it('a Modal box is NOT reset by the replace posture; the text says the container survives', async () => {
        const fake = makeProbe([OK(MODAL_HEALTH), OK(APP({ status: 'building' })), TORN_DOWN(true), ABSENT, OK(APP())]);
        const { deps, lines } = makeDeps(fake.probe);
        expect(await runDeployEndpoint(flagsOf([MODAL_BOX]), deps)).toBe(0);
        expect(lines).toContain('[nexus:endpoint] Modal box: the container is NOT recycled — replacement is app-scoped; --recycle-container opts into discarding every app');
        // The only DELETE is the APP route — /_provider/sandbox was not called.
        expect(fake.calls.filter((c) => c.method === 'DELETE').map((c) => c.url)).toEqual([`${MODAL_BOX}/apps/nexus`]);
    });

    it('--recycle-container on a Modal box sends DELETE /_provider/sandbox, waits for a fresh container, then POSTs', async () => {
        const fake = makeProbe([OK(MODAL_HEALTH), OK(APP({ status: 'building' })), OK({ tornDown: true }), OK(MODAL_OLD_HEALTH), OK(MODAL_HEALTH), ABSENT, OK(APP())]);
        const { deps, lines, invocations } = makeDeps(fake.probe);
        expect(await runDeployEndpoint(flagsOf([MODAL_BOX, '--recycle-container']), deps)).toBe(0);
        expect(fake.calls.map((c) => `${c.method} ${c.url}`)).toEqual([
            `GET ${MODAL_BOX}/health`,
            `GET ${MODAL_BOX}/apps/nexus`,
            `DELETE ${MODAL_BOX}/_provider/sandbox`,
            `GET ${MODAL_BOX}/health`,
            `GET ${MODAL_BOX}/health`,
            `GET ${MODAL_BOX}/apps/nexus`,
            `GET ${MODAL_BOX}/apps/nexus`,
        ]);
        expect(invocations[0]!.args).not.toContain('--replace');
        expect(lines).toContain(`[nexus:endpoint] DELETE ${MODAL_BOX}/_provider/sandbox — --recycle-container: this ends the container and discards EVERY app on it, not only nexus`);
        expect(JSON.parse(lines[lines.length - 1]!)).toMatchObject({ recycled: 'box' });
    });

    it('--recycle-container is refused for a non-Modal endpoint and for a Modal sandbox tunnel', async () => {
        const nonModal = makeProbe([OK(HEALTH), ABSENT]);
        await expect(runDeployEndpoint(flags(['--recycle-container']), makeDeps(nonModal.probe).deps)).rejects.toThrow('--recycle-container only applies to a Modal box');
        const tunnel = makeProbe([OK(MODAL_HEALTH), ABSENT]);
        await expect(runDeployEndpoint(flagsOf([MODAL_SANDBOX, '--recycle-container']), makeDeps(tunnel.probe).deps)).rejects.toThrow('is a Modal SANDBOX tunnel (…modal.host)');
    });

    it('surfaces the child\'s exit code and does not read the app afterwards', async () => {
        const fake = makeProbe([OK(HEALTH), ABSENT]);
        const { deps, problems } = makeDeps(fake.probe, { spawnCode: 3 });
        expect(await runDeployEndpoint(flags(), deps)).toBe(3);
        expect(fake.calls).toHaveLength(2);
        expect(problems[0]).toContain('✗ deploy failed (exit 3)');
    });

    it('TEARDOWN removes only the app and verifies the 404', async () => {
        const fake = makeProbe([OK(HEALTH), OK(APP()), TORN_DOWN(true), ABSENT]);
        const { deps, lines } = makeDeps(fake.probe);
        expect(await runDeployEndpoint(flagsOf([BOX, '--teardown', '--token', 'pt']), deps)).toBe(0);
        expect(fake.calls.map((c) => `${c.method} ${c.url}`)).toEqual(['GET https://box.example/health', 'GET https://box.example/apps/nexus', 'DELETE https://box.example/apps/nexus', 'GET https://box.example/apps/nexus']);
        expect(lines).toContain('[nexus:endpoint] ✓ app nexus removed — GET https://box.example/apps/nexus now answers 404; the endpoint keeps serving');
        expect(JSON.parse(lines[lines.length - 1]!)).toEqual({ mode: 'teardown', endpoint: BOX, app: 'nexus', path: '/app/nexus/', removed: true, token: 'pt', tokenScope: 'management', replaced: { status: 'ready', revision: 1 } });
    });

    it('TEARDOWN of an absent app is a no-op: no DELETE at all', async () => {
        const fake = makeProbe([OK(HEALTH), ABSENT]);
        const { deps, lines } = makeDeps(fake.probe);
        expect(await runDeployEndpoint(flagsOf([BOX, '--teardown']), deps)).toBe(0);
        expect(fake.calls.map((c) => c.method)).toEqual(['GET', 'GET']);
        expect(lines).toContain('[nexus:endpoint] ✓ app nexus is already absent — nothing to remove (removed:false); the endpoint is untouched');
        expect(JSON.parse(lines[lines.length - 1]!)).toMatchObject({ mode: 'teardown', removed: false });
    });

    it('hands both credentials to the child (preview flag + admin env)', async () => {
        const fake = makeProbe([OK(HEALTH), ABSENT, OK(APP())]);
        const { deps, invocations } = makeDeps(fake.probe);
        expect(await runDeployEndpoint(flags(['--token', 'pt', '--admin-token', 'at']), deps)).toBe(0);
        expect(invocations[0]!.args).toContain('--admin-token');
        expect(invocations[0]!.args).toContain('at');
        expect(invocations[0]!.env.NEXUS_DAYTONA_PREVIEW_TOKEN).toBe('pt');
        expect(invocations[0]!.env.NEXUS_SANDBOX_ADMIN_TOKEN).toBe('at');
    });

    it('reports an absent app after --no-wait with a null hub URL instead of guessing', async () => {
        const fake = makeProbe([OK(HEALTH), ABSENT, ABSENT]);
        const { deps, lines } = makeDeps(fake.probe);
        expect(await runDeployEndpoint(flags(['--no-wait']), deps)).toBe(0);
        expect(lines.some((line) => line.includes('is not registered after the child exited 0'))).toBe(true);
        expect(JSON.parse(lines[lines.length - 1]!)).toMatchObject({ hubUrl: null, hubNote: 'app "nexus" is not registered' });
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: dry run touches nothing', () => {
    it('prints the plan with no probe, no DELETE, no child and no fetch', async () => {
        const fetchSpy = vi.fn(() => {
            throw new Error('dry run must not fetch');
        });
        vi.stubGlobal('fetch', fetchSpy);
        try {
            const fake = makeProbe([]);
            const { deps, lines, invocations } = makeDeps(fake.probe);
            expect(await runDeployEndpoint(flagsOf([BOX, '--dry-run', '--token', 'secret', '--admin-token', 'admin-secret']), deps)).toBe(0);
            expect(fake.calls).toEqual([]);
            expect(invocations).toEqual([]);
            expect(fetchSpy).not.toHaveBeenCalled();
            const text = lines.join('\n');
            expect(text).toContain('v2 gate:   GET https://box.example/health');
            expect(text).toContain('POST https://box.example/apps');
            expect(text).toContain('PUT https://box.example/apps/nexus (blue-green; no peer touched)');
            expect(text).toContain('DELETE https://box.example/apps/nexus — app-scoped; verifies GET → 404');
            expect(text).toContain('x-daytona-preview-token: *** (length 6)');
            expect(text).toContain('authorization: Bearer *** (length 12)');
            // The child command is rendered with BOTH credential values masked.
            expect(text).toContain('--token *** --admin-token *** --mode local');
            expect(text).not.toContain('secret');
            expect(text).toContain('internal port AUTO-ALLOCATED');
            expect(text).toContain('container: no reset');
        } finally {
            vi.unstubAllGlobals();
        }
    });

    it('shows --recycle-container as destructive when it is given', async () => {
        const { deps, lines } = makeDeps(makeProbe([]).probe);
        expect(await runDeployEndpoint(flagsOf([MODAL_BOX, '--dry-run', '--recycle-container']), deps)).toBe(0);
        expect(lines.join('\n')).toContain('DESTRUCTIVE: removes EVERY app on the Modal box');
    });

    it('dry-runs the teardown posture without inventing work', async () => {
        const { deps, lines, invocations } = makeDeps(makeProbe([]).probe);
        expect(await runDeployEndpoint(flagsOf([BOX, '--dry-run', '--teardown']), deps)).toBe(0);
        expect(invocations).toEqual([]);
        expect(lines.join('\n')).toContain('mode: teardown — removes app "nexus" only');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: main() maps results onto exit codes', () => {
    it('--help prints the usage and never touches the network', async () => {
        const fake = makeProbe([]);
        const { deps, lines } = makeDeps(fake.probe);
        expect(await main(['--help'], deps)).toBe(0);
        expect(lines).toEqual([USAGE]);
        expect(USAGE).toContain('sandbox runtime V2');
        expect(USAGE).toContain('/apps/nexus');
        expect(USAGE).toContain('--admin-token');
        expect(USAGE).toContain('--recycle-container');
        expect(USAGE).not.toContain('DELETE /sandbox');
    });

    it('a bad command line exits 1 with every problem and the usage', async () => {
        const { deps, problems } = makeDeps(makeProbe([]).probe);
        expect(await main([], deps)).toBe(1);
        expect(problems[0]).toBe('[nexus:endpoint] ✗ no endpoint — pass it as the argument (deploy:endpoint <url>) or with --endpoint / NEXUS_ENDPOINT');
        expect(problems[1]).toBe('');
        expect(problems[2]).toBe(USAGE);
    });

    it('a v1 runner exits 1 with the v2 gate message, before any app read', async () => {
        const fake = makeProbe([OK(V1_HEALTH)]);
        const { deps, problems } = makeDeps(fake.probe);
        expect(await main([BOX], deps)).toBe(1);
        expect(problems[0]).toContain('not a sandbox runtime v2 runner');
        expect(problems[0]).toContain('daytona-runtime-v1');
        expect(fake.calls).toHaveLength(1);
    });

    it('a dead URL exits 1 with one actionable line', async () => {
        const { deps, problems } = makeDeps(makeProbe([FAIL(404)]).probe);
        expect(await main([BOX], deps)).toBe(1);
        expect(problems[0]).toContain('that is not a sandbox runtime v2 runner');
    });

    it('an operator error and a bug are reported differently', async () => {
        const operator = makeDeps(makeProbe([]).probe);
        expect(await main([BOX, '--recycle-container', '--dry-run', '--no-teardown'], operator.deps)).toBe(1);
        expect(operator.problems[0]).toContain('--no-teardown forbids touching the endpoint');

        const bug = makeDeps(makeProbe([]).probe);
        // A thrown non-EndpointDeployError keeps its stack, so a real defect is not
        // dressed up as operator error; here the failure is the endpoint read.
        bug.deps.probe = async () => {
            throw new TypeError('boom');
        };
        expect(await main([BOX], bug.deps)).toBe(1);
        expect(bug.problems[0]).toContain('unexpected failure: TypeError: boom');
    });

    it('a full run through main() exits 0 and ends with the summary line', async () => {
        const fake = makeProbe([OK(HEALTH), ABSENT, OK(APP())]);
        const { deps, lines } = makeDeps(fake.probe);
        expect(await main([BOX], deps)).toBe(0);
        expect(JSON.parse(lines[lines.length - 1]!)).toMatchObject({ mode: 'deploy', app: 'nexus', hubUrl: 'https://box.example/app/nexus/' });
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: spawnDeploy — the one real side effect (SpawnImpl seam)', () => {
    const child = () => {
        const listeners: Record<string, ((arg?: unknown) => void)[]> = {};
        const fake = {
            on: vi.fn((event: string, listener: (arg?: unknown) => void) => {
                (listeners[event] ??= []).push(listener);
                return fake;
            }),
            emit: (event: string, arg?: unknown) => {
                for (const listener of listeners[event] ?? []) listener(arg);
            },
        };
        return fake;
    };

    it('inherits stdio, runs from the package root, and forwards the child env untouched', async () => {
        const fakeChild = child();
        const impl = vi.fn(() => fakeChild);
        const invocation = buildDeployInvocation(bareFlags(), { PATH: '/usr/bin' });
        const run = spawnDeploy(invocation, impl as never);
        expect((impl as unknown as ReturnType<typeof vi.fn>).mock.calls[0]).toEqual([process.execPath, invocation.args, { cwd: PACKAGE_ROOT, env: invocation.env, stdio: 'inherit' }]);
        fakeChild.emit('close', 0);
        expect(await run).toBe(0);
    });

    it('maps every non-zero outcome to a non-zero exit code', async () => {
        for (const [code, expected] of [[0, 0], [2, 2], [null, 1]] as const) {
            const fakeChild = child();
            const run = spawnDeploy(buildDeployInvocation(bareFlags(), {}), (() => fakeChild) as never);
            fakeChild.emit('close', code);
            expect(await run).toBe(expected);
        }
    });

    it('the default deps are the real fetch probe and the real spawner', () => {
        const deps = createDefaultDeps();
        expect(deps.spawn).toBe(spawnDeploy);
        expect(typeof deps.probe).toBe('function');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the import-is-a-no-op guard', () => {
    const module = '/repo/packages/typecrate/nexus/scripts/deploy-endpoint.ts';

    it('never runs under vitest, whatever the argv looks like', () => {
        expect(isCliEntryPoint({ vitest: true, argv: ['/usr/bin/node', module], modulePath: module })).toBe(false);
    });

    it('runs when the script positional names this file (direct and --script forms)', () => {
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', module], modulePath: module })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node', '--script', 'scripts/deploy-endpoint.ts'], modulePath: module })).toBe(true);
    });

    it('runs when the npm lifecycle line names it, and not when it names a sibling', () => {
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], env: { npm_lifecycle_script: 'vite-node scripts/deploy-endpoint.ts' }, modulePath: module })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], env: { npm_lifecycle_script: 'vite-node scripts/deploy-daytona.ts' }, modulePath: module })).toBe(false);
    });

    it('runs when vite-node evaluated this file as its entry, and not when another module imported it', () => {
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], modulePath: module, stack: `Error\n    at Object.<anonymous> (${module}:1:1)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], modulePath: module, stack: `Error\n    at Object.<anonymous> (${module}:1:1)\n    at startModal (/repo/packages/typecrate/nexus/scripts/start-modal.ts:2:2)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(false);
    });

    it('ignores module query strings, and rejects plain node without any runner evidence', () => {
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', `${module}?v=1`], modulePath: module })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node', '@services/daytona/sandbox'], modulePath: module })).toBe(false);
        expect(isCliEntryPoint({})).toBe(false);
    });
});
