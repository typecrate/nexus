// ─────────────────────────────────────────────────────────────────────────────
//  deploy-endpoint.test.ts — the platform-neutral deploy verb, end to end, with
//  ZERO network and no cargo.
//
//  Everything side-effectful in deploy-endpoint.ts is an injected dep (probe /
//  spawn / sleep / now / log / error / env), so these tests drive the REAL control
//  flow against scripted endpoint answers and assert exact values:
//    · parser     — defaults, the positional URL, every flag, --env repeats,
//                   env fallbacks and their precedence, and each illegal combination
//    · probes     — classifyProbe's cold-start / auth / dead table (the branch that
//                   decides whether a miss is a cold container or a wrong URL)
//    · slot policy— decideTeardown's table, INCLUDING 'unknown' counting as busy
//    · hub URL    — buildHubUrl: reported, never guessed (box / port / no-preview)
//    · argv       — the exact child argv per flag set, plus command/cwd/env
//    · flow       — cold start retried, empty slot, replace-on-arrival (DELETE then
//                   prove empty then deploy), --no-teardown abort, --teardown verb,
//                   a DELETE that does not confirm, a teardown that leaves the slot
//                   busy, auth abort, dead-URL abort, budget exhaustion, child
//                   failure's exit code, and dry run touching NOTHING
//    · modal      — the container reset: box/sandbox classified, an empty slot used as
//                   the fresh machine, a busy box recycled before the POST (the new
//                   process proven by its uptime or a 5xx gap, the replacement's slot
//                   proven empty), a busy sandbox tunnel refused, --no-teardown leaving
//                   the container alone, and every fail-closed branch (unconfirmed
//                   reset, the old container still answering, a replacement not empty)
//    · spawnDeploy— the child wiring through the SpawnImpl seam (see SpawnImpl in the
//                   source for why vi.mock cannot rebind a builtin here)
//    · import     — isCliEntryPoint's three entry rules
//
//  The three pure helpers the flow depends on (joinUrl / authHeaders / buildSummary)
//  are pinned here too, because the same URL join and the same header name must match
//  deploy-daytona.ts byte for byte — a drift between them is a mis-posted bundle.
// ─────────────────────────────────────────────────────────────────────────────

import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';

import { modalContainer } from '@config/vault';

import {
    authHeaders,
    buildDeployArgs,
    buildDeployInvocation,
    buildHubUrl,
    buildSummary,
    classifyModalContainer,
    classifyProbe,
    createDefaultDeps,
    DEFAULT_HUB_PORT,
    decideTeardown,
    DEPLOY_SCRIPT_PATH,
    describeRuntime,
    EndpointDeployError,
    type EndpointDeps,
    type EndpointFlags,
    type Probe,
    type ProbeResult,
    isCliEntryPoint,
    joinUrl,
    main,
    PACKAGE_ROOT,
    parseArgs,
    RECYCLE_WAIT_MS,
    requireEndpoint,
    resolveMode,
    runDeployEndpoint,
    SLOT_WAIT_MS,
    spawnDeploy,
    USAGE,
    VITE_NODE_CLI,
    waitForRoute,
} from './deploy-endpoint';

// ══════════════════════════════════════════════════════════════════════════════
//  FAKES
// ══════════════════════════════════════════════════════════════════════════════

const OK = (json: unknown): ProbeResult => ({ status: 200, ok: true, json, text: JSON.stringify(json) });
const FAIL = (status: number, text = ''): ProbeResult => ({ status, ok: status >= 200 && status < 300, json: null, text });
const DOWN = (message = 'connect ECONNREFUSED 127.0.0.1:7777'): ProbeResult => ({ status: 0, ok: false, json: null, text: '', transportError: message });

/** The body a real runner answers /health with (contract v1). */
const HEALTH = { status: 'ok', contract: 'daytona-runtime-v1', runnerPort: 7777, authenticated: false, exposedPorts: [8085] };
/**
 * The MODAL runner's /health: the same contract, plus the `service` marker that makes
 * it the one endpoint shape this script is allowed to replace wholesale.
 */
const MODAL_HEALTH = { ok: true, slot: 'empty', uptimeSec: 4, contract: 'daytona-runtime-v1', runnerPort: 7777, authenticated: false, exposedPorts: [8085], service: 'modal-container-runtime' };
/** A long-lived box's /health — its uptime is older than any reset, so it can never pass as a replacement. */
const MODAL_OLD_HEALTH = { ...MODAL_HEALTH, uptimeSec: 600 };
/** A Modal box is a stable deployment URL; a sandbox tunnel's URL dies with its container. */
const MODAL_BOX = Object.values(modalContainer)[0];
const MODAL_SANDBOX = 'https://abc123-7777-deadbeef.modal.host';
/** GET /status bodies used across the flow tests. */
const EMPTY = { slot: 'empty', runtime: null };
const busy = (previewUrl: string | null): ProbeResult =>
    OK({
        slot: 'ready',
        runtime: { profile: 'nexus', port: 8085, runtimeId: 'rt-1', startedAtMs: 1700000000000, ready: true, preview: { port: 8085, url: previewUrl, exposed: true, exposedPorts: [8085] } },
    });
const READY_NO_PREVIEW = OK({
    slot: 'ready',
    runtime: {
        profile: 'nexus',
        port: 8085,
        runtimeId: 'rt-2',
        ready: true,
        preview: { port: 8085, url: null, exposed: true, exposedPorts: [8085], warning: 'port 8085 has no preview URL — the driver has not pushed one' },
    },
});

/** One probe call, as recorded. */
type Call = { method: string; url: string; token?: string };

/**
 * A scripted endpoint: answers each (method, url) from `script` in call order, and
 * records every call so a test can pin the whole request sequence. A script entry may
 * be a ProbeResult or a function of the call.
 */
const makeProbe = (script: Array<ProbeResult | ((call: Call) => ProbeResult)>) => {
    const calls: Call[] = [];
    let index = 0;
    const probe: Probe = async (method, url, init) => {
        const call: Call = { method, url, ...(init.headers['x-daytona-preview-token'] ? { token: init.headers['x-daytona-preview-token' as keyof typeof init.headers] as string } : {}) };
        calls.push(call);
        const next = script[index++];
        if (next === undefined) throw new Error(`probe script exhausted at call ${index}: ${method} ${url}`);
        return typeof next === 'function' ? next(call) : next;
    };
    return { probe, calls, remaining: () => script.length - index };
};

/** Deps with no clock and no waiting: the flow must run in one tick. */
const makeDeps = (probe: Probe, exitCode = 0, options: { spawnCode?: number } = {}) => {
    const lines: string[] = [];
    const problems: string[] = [];
    const invocations: Array<{ command: string; args: string[]; cwd: string; env: Record<string, string | undefined> }> = [];
    const deps: EndpointDeps = {
        probe,
        spawn: async (invocation) => {
            invocations.push(invocation);
            return options.spawnCode ?? exitCode;
        },
        log: (line) => lines.push(line),
        error: (line) => problems.push(line),
        env: {},
        sleep: async () => undefined,
        now: () => 0,
    };
    return { deps, lines, problems, invocations };
};

/** parseArgs with no env noise, so defaults come from the parser itself. */
const flagsOf = (argv: readonly string[]): EndpointFlags => {
    const { flags, errors } = parseArgs(argv, {});
    expect(errors).toEqual([]);
    return flags;
};
const bareFlags = (): EndpointFlags => flagsOf(['https://box.example']);

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: module pins', () => {
    it('resolves the sibling script inside the package root, and it exists', () => {
        expect(PACKAGE_ROOT.endsWith(join('packages', 'typecrate', 'nexus'))).toBe(true);
        expect(DEPLOY_SCRIPT_PATH).toBe(join(PACKAGE_ROOT, 'scripts', 'deploy-daytona.ts'));
        expect(existsSync(DEPLOY_SCRIPT_PATH)).toBe(true);
    });

    it('keeps the hub-port default in lockstep with deploy-daytona.ts DEFAULT_PORT', () => {
        expect(DEFAULT_HUB_PORT).toBe(8085);
        expect(bareFlags().port).toBe(8085);
    });

    it('waits long enough for a cold container, and its slot fence is shorter', () => {
        // A recycled/scaled-to-zero box needs tens of seconds; a hung proxy must not
        // turn the run into a mystery. 90 s cold-start, 30 s for the DELETE proof.
        expect(SLOT_WAIT_MS).toBeLessThan(60_000);
        expect(createDefaultDeps().probe).toBeTypeOf('function');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: argument parsing', () => {
    it('takes the URL as the positional argument, and every default is the deploy posture', () => {
        const flags = flagsOf([MODAL_BOX]);
        expect(flags).toEqual({
            help: false,
            dryRun: false,
            teardownOnly: false,
            replace: true,
            endpoint: MODAL_BOX,
            token: '',
            mode: 'local',
            port: DEFAULT_HUB_PORT,
            env: {},
            noBuild: false,
            artifact: '',
            wait: true,
        });
    });

    it('accepts --endpoint, and a positional that agrees with it is not an error', () => {
        expect(flagsOf(['--endpoint', 'https://a.example']).endpoint).toBe('https://a.example');
        expect(flagsOf(['https://a.example', '--endpoint', 'https://a.example']).endpoint).toBe('https://a.example');
    });

    it('rejects two different endpoints, because picking one silently could deploy to the wrong box', () => {
        const { errors } = parseArgs(['https://a.example', '--endpoint', 'https://b.example'], {});
        expect(errors).toEqual(['two different endpoints given: "https://a.example" and --endpoint "https://b.example"']);
    });

    it('rejects a second positional', () => {
        const { errors } = parseArgs(['https://a.example', 'https://b.example'], {});
        expect(errors).toEqual(['unexpected extra argument "https://b.example" — the endpoint is the only positional (got "https://a.example" already)']);
    });

    it('requires an endpoint — there is no platform default, that is the whole point', () => {
        const { errors } = parseArgs([], {});
        expect(errors).toEqual(['no endpoint — pass it as the argument (deploy:endpoint <url>) or with --endpoint / NEXUS_ENDPOINT']);
    });

    it('reads the endpoint and token from env, with the platform-neutral names winning', () => {
        expect(parseArgs([], { NEXUS_DAYTONA_ENDPOINT: 'https://d.example' }).flags.endpoint).toBe('https://d.example');
        expect(parseArgs([], { NEXUS_DAYTONA_ENDPOINT: 'https://d.example', NEXUS_ENDPOINT: 'https://m.example' }).flags.endpoint).toBe('https://m.example');
        expect(parseArgs([], { NEXUS_DAYTONA_PREVIEW_TOKEN: 'daytona' }).flags.token).toBe('daytona');
        expect(parseArgs([], { NEXUS_DAYTONA_PREVIEW_TOKEN: 'daytona', NEXUS_ENDPOINT_TOKEN: 'neutral' }).flags.token).toBe('neutral');
        // A positional/flag always outranks env.
        expect(parseArgs(['https://x.example', '--token', 't'], { NEXUS_ENDPOINT: 'https://d.example', NEXUS_ENDPOINT_TOKEN: 'env' }).flags).toMatchObject({ endpoint: 'https://x.example', token: 't' });
    });

    it('maps the slot verbs onto their two independent switches', () => {
        expect(flagsOf(['https://a.example', '--teardown']).teardownOnly).toBe(true);
        expect(flagsOf(['https://a.example', '--no-teardown']).replace).toBe(false);
        // Both at once is meaningful: free the slot, and never do it implicitly.
        const both = flagsOf(['https://a.example', '--teardown', '--no-teardown']);
        expect(both).toMatchObject({ teardownOnly: true, replace: false });
    });

    it('accumulates repeatable --env, splitting on the first = so values may contain =', () => {
        const flags = flagsOf(['https://a.example', '--env', 'A=1', '--env', 'NEXUS_TOKEN=x=y', '--env', 'B=']);
        expect(flags.env).toEqual({ A: '1', NEXUS_TOKEN: 'x=y', B: '' });
    });

    it('passes the deploy knobs through verbatim', () => {
        const flags = flagsOf(['https://a.example', '--mode', 'remote-build', '--port', '9000', '--no-wait']);
        expect(flags).toMatchObject({ mode: 'remote-build', port: 9000, wait: false });
        const shipping = flagsOf(['https://a.example', '--no-build', '--artifact', '/tmp/nexus-hub', '--wait']);
        expect(shipping).toMatchObject({ noBuild: true, artifact: '/tmp/nexus-hub', wait: true });
    });

    it('rejects malformed values instead of guessing', () => {
        expect(parseArgs(['https://a.example', '--port', '70000'], {}).errors).toEqual(['--port "70000" is out of range (TCP port 1-65535)']);
        expect(parseArgs(['https://a.example', '--port', 'abc'], {}).errors).toEqual(['--port "abc" is out of range (TCP port 1-65535)']);
        expect(parseArgs(['https://a.example', '--mode', 'cross'], {}).errors).toEqual(['--mode must be "local" or "remote-build", got "cross"']);
        expect(parseArgs(['https://a.example', '--env', 'oops'], {}).errors).toEqual(['--env expects KEY=VALUE, got "oops"']);
        expect(parseArgs(['https://a.example', '--no-build', '--mode', 'remote-build'], {}).errors).toEqual(['--no-build and --artifact apply to --mode local only (remote-build ships a source tarball)']);
        expect(parseArgs(['https://a.example', '--teardown', '--env', 'A=b'], {}).errors).toEqual(['--teardown ships nothing, so --env/--no-build/--artifact/--no-wait do not apply (drop them, or drop --teardown)']);
        // An unknown flag never hides the missing endpoint: every problem comes back.
        expect(parseArgs(['--nope'], {}).errors).toEqual(['unknown flag "--nope"', 'no endpoint — pass it as the argument (deploy:endpoint <url>) or with --endpoint / NEXUS_ENDPOINT']);
    });

    it('reports a missing value without treating the next flag as one', () => {
        // `--token --dry-run` is a missing value, not the token "--dry-run".
        const { flags, errors } = parseArgs(['https://a.example', '--token', '--dry-run'], {});
        expect(errors).toEqual(['--token needs a value']);
        expect(flags.token).toBe('');
        // The rejected token is consumed, so --dry-run does not silently re-attach to
        // it. The run exits 1 on the error either way — no flag is guessed at.
        expect(flags.dryRun).toBe(false);
    });

    it('collects every problem in one pass, and --help still wins over a broken line', () => {
        // Flag-shaped problems come first (parse order), then the cross-flag rules.
        expect(parseArgs(['--mode', 'x', '--port', '0'], {}).errors).toEqual(['--port "0" is out of range (TCP port 1-65535)', 'no endpoint — pass it as the argument (deploy:endpoint <url>) or with --endpoint / NEXUS_ENDPOINT', '--mode must be "local" or "remote-build", got "x"']);
        const helped = parseArgs(['--mode', 'x', '--help'], {});
        expect(helped.flags.help).toBe(true);
        // --help short-circuits BEFORE the cross-flag rules, so a broken line still
        // gets the usage screen and exit 0 — the flag shape errors are kept, the
        // semantic ones are never computed.
        expect(helped.errors).toEqual([]);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: mode resolution', () => {
    it('--teardown is the only thing that switches verbs', () => {
        expect(resolveMode({ teardownOnly: false })).toBe('deploy');
        expect(resolveMode({ teardownOnly: true })).toBe('teardown');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: URL joining and auth headers (must match deploy-daytona.ts)', () => {
    it('joins routes onto the base without clobbering a path prefix', () => {
        expect(joinUrl('https://box.example', 'health')).toBe('https://box.example/health');
        expect(joinUrl('https://proxy.example/runner-7777/', 'status')).toBe('https://proxy.example/runner-7777/status');
        // Daytona preview links ARE path-prefixed; new URL(suffix, base) would drop it.
        expect(joinUrl('https://proxy.example/sd/sandbox/7777', 'previews')).toBe('https://proxy.example/sd/sandbox/7777/previews');
        expect(joinUrl('https://box.example', '')).toBe('https://box.example/');
    });

    it('sends the token header only when there is a token, so public endpoints keep working', () => {
        expect(authHeaders('')).toEqual({});
        expect(authHeaders('s3cret')).toEqual({ 'x-daytona-preview-token': 's3cret' });
    });

    it('validates the endpoint where it is first needed', () => {
        expect(requireEndpoint('http://127.0.0.1:7897')).toBe('http://127.0.0.1:7897');
        expect(() => requireEndpoint('')).toThrowError(EndpointDeployError);
        expect(() => requireEndpoint('box.example')).toThrowError(/not a valid absolute URL/);
        expect(() => requireEndpoint('ftp://box.example')).toThrowError(/must be http\(s\)/);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: classifyProbe — cold start vs wrong URL vs wrong token', () => {
    it('treats every "not serving yet" answer as retryable', () => {
        // Modal answers 502/503 until a container holds the port; a Daytona proxy drops
        // the connection while the sandbox boots. Both are the NORMAL first minutes.
        expect(classifyProbe(DOWN())).toBe('retry');
        for (const status of [408, 429, 500, 502, 503]) expect(classifyProbe(FAIL(status))).toBe('retry');
    });

    it('treats 401/403 as a token problem — retrying cannot fix it', () => {
        expect(classifyProbe(FAIL(401))).toBe('auth');
        expect(classifyProbe(FAIL(403))).toBe('auth');
    });

    it('treats a wrong path, a stopped app and a different service as fatal', () => {
        expect(classifyProbe(FAIL(404))).toBe('dead');
        expect(classifyProbe(FAIL(302))).toBe('dead');
        // An edge/HTML error page parses to null JSON: not the runner.
        expect(classifyProbe({ status: 200, ok: true, json: null, text: '<html>404</html>' })).toBe('dead');
        // Something JSON-speaking, but not a contract runner (e.g. the hub itself).
        expect(classifyProbe(OK({ status: 'ok', service: 'nexus-hub' }), { requireContract: true })).toBe('dead');
    });

    it('accepts a 2xx JSON body, and only the contract marker satisfies the identity check', () => {
        expect(classifyProbe(OK({ slot: 'empty' }))).toBe('ready');
        expect(classifyProbe(OK({ slot: 'empty' }), { requireContract: true })).toBe('dead');
        expect(classifyProbe(OK(HEALTH), { requireContract: true })).toBe('ready');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the slot policy table', () => {
    it('an empty slot needs nothing, in either posture', () => {
        expect(decideTeardown('empty', true)).toEqual({ action: 'none', why: 'the slot is already empty' });
        expect(decideTeardown('empty', false)).toEqual({ action: 'none', why: 'the slot is already empty' });
    });

    it('a busy slot is replaced by default, and an unrecognised slot counts as busy', () => {
        expect(decideTeardown('ready', true).action).toBe('teardown');
        expect(decideTeardown('building', true).action).toBe('teardown');
        expect(decideTeardown('failed', true).action).toBe('teardown');
        // 'unknown' = a runner whose state cannot be trusted: deploying blind risks a
        // 409 after the bundle has already been paid for.
        expect(decideTeardown('unknown', true).action).toBe('teardown');
    });

    it('--no-teardown turns a busy slot into an error that names the way out', () => {
        const decision = decideTeardown('ready', false);
        expect(decision).toEqual({ action: 'abort', why: 'the slot holds a ready runtime and --no-teardown forbids replacing it' });
    });

    it('names the runtime being destroyed, so the destructive step is never anonymous', () => {
        expect(describeRuntime(null)).toBe('runtime');
        // The /status body nests the runtime object; describeRuntime takes THAT, not the body.
        const status = busy('https://hub.example').json as { slot: string; runtime: Record<string, unknown> };
        expect(describeRuntime(status.runtime)).toBe('profile=nexus port=8085 runtimeId=rt-1 since=2023-11-14T22:13:20.000Z serving=https://hub.example');
        expect(describeRuntime({ profile: 'manifest', port: 9000 })).toBe('profile=manifest port=9000 runtimeId=?');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the hub URL is reported, never guessed', () => {
    it('a box that serves the hub on its own URL says so', () => {
        // /status nests the runtime object, and buildHubUrl reads runtime.preview.
        const body = busy('https://box.example/').json as unknown as { runtime: Record<string, unknown> };
        expect(buildHubUrl({ slot: 'ready', runtime: body.runtime, raw: {} } as never, 'https://box.example')).toEqual({
            hubUrl: 'https://box.example/',
            note: 'the endpoint itself serves the hub (it forwards every non-contract path)',
        });
    });

    it('a runtime port with its own preview link reports that link', () => {
        const body = busy('https://proxy.example/hub-8085').json as unknown as { runtime: Record<string, unknown> };
        expect(buildHubUrl({ slot: 'ready', runtime: body.runtime, raw: {} } as never, 'https://proxy.example/runner-7777')).toEqual({ hubUrl: 'https://proxy.example/hub-8085', note: 'the runtime port has its own preview URL' });
    });

    it('no preview URL keeps hubUrl null and carries the runner\'s own warning', () => {
        const noUrl = { slot: 'ready', runtime: { port: 8085, preview: { url: null, warning: 'the driver has not pushed a tunnel map yet' } }, raw: {} } as never;
        expect(buildHubUrl(noUrl, 'https://box.example')).toEqual({ hubUrl: null, note: 'the driver has not pushed a tunnel map yet' });
        const bare = { slot: 'ready', runtime: { port: 8085, preview: {} }, raw: {} } as never;
        expect(buildHubUrl(bare, 'https://box.example')).toEqual({ hubUrl: null, note: 'the endpoint reported no preview URL for port 8085' });
        // A runtime torn down between the deploy and the read must not throw.
        expect(buildHubUrl({ slot: 'empty', runtime: null, raw: {} } as never, 'https://box.example').hubUrl).toBeNull();
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the child invocation', () => {
    it('builds the exact deploy-daytona.ts argv, in a fixed order, with the URL first', () => {
        expect(buildDeployArgs(flagsOf(['https://box.example']))).toEqual(['--endpoint', 'https://box.example', '--mode', 'local', '--port', '8085']);
        expect(buildDeployArgs(flagsOf(['https://box.example', '--token', 's3cret']))).toEqual(['--endpoint', 'https://box.example', '--token', 's3cret', '--mode', 'local', '--port', '8085']);
        expect(buildDeployArgs(flagsOf(['https://box.example', '--mode', 'remote-build']))).toEqual(['--endpoint', 'https://box.example', '--mode', 'remote-build', '--port', '8085']);
        expect(buildDeployArgs(flagsOf(['https://box.example', '--port', '9000', '--no-wait']))).toEqual(['--endpoint', 'https://box.example', '--mode', 'local', '--port', '9000', '--no-wait']);
        expect(buildDeployArgs(flagsOf(['https://box.example', '--no-build', '--artifact', '/tmp/hub']))).toEqual(['--endpoint', 'https://box.example', '--mode', 'local', '--port', '8085', '--no-build', '--artifact', '/tmp/hub']);
        // --env keeps the order it was typed in.
        expect(buildDeployArgs(flagsOf(['https://box.example', '--env', 'Z=1', '--env', 'A=2']))).toEqual(['--endpoint', 'https://box.example', '--mode', 'local', '--port', '8085', '--env', 'Z=1', '--env', 'A=2']);
    });

    it('wraps it in the vite-node child that the house runner requires', () => {
        const invocation = buildDeployInvocation(bareFlags(), { HTTPS_PROXY: 'http://proxy:3128' });
        expect(invocation.command).toBe(process.execPath);
        expect(invocation.args).toEqual([VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, '--endpoint', 'https://box.example', '--mode', 'local', '--port', '8085']);
        expect(invocation.cwd).toBe(PACKAGE_ROOT);
        // cargo/openssl need the caller's proxy vars untouched.
        expect(invocation.env).toEqual({ HTTPS_PROXY: 'http://proxy:3128' });
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the machine-readable line', () => {
    it('is one JSON object with hubUrl always present for a deploy', () => {
        expect(buildSummary({ mode: 'deploy', endpoint: 'https://box.example', hubUrl: 'https://box.example', hubPort: 8085, hubNote: 'the endpoint itself serves the hub (it forwards every non-contract path)' })).toBe(
            '{"mode":"deploy","endpoint":"https://box.example","hubUrl":"https://box.example","hubPort":8085,"hubNote":"the endpoint itself serves the hub (it forwards every non-contract path)"}',
        );
        expect(buildSummary({ mode: 'deploy', endpoint: 'https://box.example', hubUrl: null, hubPort: 8085, hubNote: 'no preview URL' })).toBe('{"mode":"deploy","endpoint":"https://box.example","hubUrl":null,"hubPort":8085,"hubNote":"no preview URL"}');
    });

    it('records the token as runner-scoped, and records what was replaced', () => {
        expect(buildSummary({ mode: 'teardown', endpoint: 'https://box.example', slot: 'empty', token: 't', replaced: { slot: 'ready', profile: 'nexus', port: 8085 } })).toBe(
            '{"mode":"teardown","endpoint":"https://box.example","slot":"empty","token":"t","tokenScope":"management","replaced":{"slot":"ready","profile":"nexus","port":8085}}',
        );
        // No token → no token key at all (a consumer must not read a masked placeholder).
        expect(buildSummary({ mode: 'deploy', endpoint: 'https://box.example' })).toBe('{"mode":"deploy","endpoint":"https://box.example"}');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: waitForRoute (the cold-start budget)', () => {
    it('returns the first ready answer without waiting', async () => {
        const { probe, calls } = makeProbe([OK(HEALTH)]);
        const { deps } = makeDeps(probe);
        const result = await waitForRoute({ method: 'GET', url: 'https://box.example/health', token: '', what: 'GET /health', budgetMs: 1000, requireContract: true, deps });
        expect(result.json).toEqual(HEALTH);
        expect(calls).toEqual([{ method: 'GET', url: 'https://box.example/health' }]);
    });

    it('retries transport errors and 5xx, and says so once', async () => {
        // The budget is what caps the loop (POLL_MS is 2 s), so it must cover 2 waits.
        const { probe, calls } = makeProbe([DOWN(), FAIL(502), OK(HEALTH)]);
        const { deps, lines } = makeDeps(probe);
        expect((await waitForRoute({ method: 'GET', url: 'https://box.example/health', token: '', what: 'GET /health', budgetMs: 6_000, requireContract: true, deps })).json).toEqual(HEALTH);
        expect(calls).toHaveLength(3);
        expect(lines.filter((line) => line.includes('is not answering yet'))).toHaveLength(1);
        expect(lines[0]).toContain('connect ECONNREFUSED');
        expect(lines[0]).toContain('waiting up to 6s');
    });

    it('aborts immediately on a token problem, naming the flag that fixes it', async () => {
        const rejected = makeProbe([FAIL(401)]);
        const { deps } = makeDeps(rejected.probe);
        await expect(waitForRoute({ method: 'GET', url: 'https://box.example/health', token: 'wrong', what: 'GET /health', budgetMs: 1_000, requireContract: true, deps })).rejects.toThrowError(/this token was rejected/);
        expect(rejected.calls).toHaveLength(1);

        const missing = makeProbe([FAIL(403)]);
        const missingDeps = makeDeps(missing.probe);
        await expect(waitForRoute({ method: 'GET', url: 'https://box.example/health', token: '', what: 'GET /health', budgetMs: 1_000, requireContract: true, deps: missingDeps.deps })).rejects.toThrowError(/no token was given/);
        // 403 is an answer, so nothing is retried — a wrong token never fixes itself.
        expect(missing.calls).toHaveLength(1);
    });

    it('aborts immediately when the answer is not a runner', async () => {
        const { probe } = makeProbe([FAIL(404)]);
        const { deps } = makeDeps(probe);
        await expect(waitForRoute({ method: 'GET', url: 'https://box.example/health', token: '', what: 'GET /health', budgetMs: 1000, requireContract: true, deps })).rejects.toThrowError(/that is not a daytona-runtime-v1 runner/);
    });

    it('gives up within its budget when the endpoint never answers', async () => {
        // Frozen clock: the attempt cap is what bounds the loop, so this is instant.
        const script = Array.from({ length: 500 }, () => DOWN('socket hang up'));
        const { probe } = makeProbe(script);
        const { deps } = makeDeps(probe);
        await expect(waitForRoute({ method: 'GET', url: 'https://box.example/health', token: '', what: 'GET /health', budgetMs: 1000, requireContract: true, deps })).rejects.toThrowError(/never answered at https:\/\/box.example\/health \(last: socket hang up/);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the deploy flow', () => {
    it('waits out a cold start, finds an empty slot, deploys, and reports the hub URL', async () => {
        const flags = flagsOf(['https://box.example']);
        // health (down → up) → status (empty) → [child] → status (ready + preview).
        const scripted = makeProbe([DOWN(), OK(HEALTH), OK(EMPTY), busy('https://box.example')]);
        const { deps, lines, invocations } = makeDeps(scripted.probe);
        expect(await runDeployEndpoint(flags, deps)).toBe(0);
        expect(scripted.remaining()).toBe(0);
        expect(scripted.calls).toEqual([
            { method: 'GET', url: 'https://box.example/health' },
            { method: 'GET', url: 'https://box.example/health' },
            { method: 'GET', url: 'https://box.example/status' },
            { method: 'GET', url: 'https://box.example/status' },
        ]);
        expect(invocations).toHaveLength(1);
        expect(lines.some((line) => line.includes('✓ runtime runner — contract daytona-runtime-v1, runner port 7777, auth none, exposed ports [8085]'))).toBe(true);
        expect(lines.some((line) => line.includes('slot: empty'))).toBe(true);
        expect(lines.some((line) => line.includes('slot is empty — nothing to replace'))).toBe(true);
        expect(lines.at(-3)).toContain('hub (clients):    https://box.example');
        expect(lines.at(-2)).toContain('management:       https://box.example/health');
        // The LAST stdout line is bare JSON — a consumer can `tail -1 | jq` it.
        expect(lines.at(-1)).toBe('{"mode":"deploy","endpoint":"https://box.example","hubUrl":"https://box.example","hubPort":8085,"hubNote":"the endpoint itself serves the hub (it forwards every non-contract path)"}');
    });

    it('sends the token on every contract call and hands it to the child', async () => {
        const flags = flagsOf(['https://box.example', '--token', 's3cret']);
        const scripted = makeProbe([OK(HEALTH), OK(EMPTY), busy('https://box.example')]);
        const { deps, invocations, lines } = makeDeps(scripted.probe);
        expect(await runDeployEndpoint(flags, deps)).toBe(0);
        expect(scripted.calls.map((call) => `${call.method} ${call.url}`)).toEqual(['GET https://box.example/health', 'GET https://box.example/status', 'GET https://box.example/status']);
        expect(scripted.calls.every((call) => call.token === 's3cret')).toBe(true);
        expect(invocations[0]!.args).toContain('s3cret');
        expect(invocations[0]!.env.NEXUS_DAYTONA_ENDPOINT).toBe('https://box.example');
        expect(invocations[0]!.env.NEXUS_DAYTONA_PREVIEW_TOKEN).toBe('s3cret');
        expect(lines.at(-1)).toContain('"token":"s3cret","tokenScope":"management"');
    });

    it('REPLACES a busy slot by default: DELETE, prove empty, then deploy', async () => {
        const flags = flagsOf(['https://box.example']);
        const scripted = makeProbe([OK(HEALTH), busy('https://box.example'), OK({ tornDown: true, runtimeId: 'rt-1' }), OK(EMPTY), busy('https://box.example')]);
        const { deps, lines, invocations } = makeDeps(scripted.probe);
        expect(await runDeployEndpoint(flags, deps)).toBe(0);
        expect(scripted.remaining()).toBe(0);
        expect(scripted.calls.map((call) => `${call.method} ${call.url}`)).toEqual([
            'GET https://box.example/health',
            'GET https://box.example/status',
            'DELETE https://box.example/',
            'GET https://box.example/status',
            'GET https://box.example/status',
        ]);
        expect(lines.some((line) => line.includes(`slot: ready (profile=nexus port=8085 runtimeId=rt-1 since=2023-11-14T22:13:20.000Z serving=https://box.example)`))).toBe(true);
        expect(lines.some((line) => line.includes("DELETE https://box.example/ — the slot holds a ready runtime, and replacing it is this command's default, replacing profile=nexus port=8085 runtimeId=rt-1"))).toBe(true);
        expect(lines.some((line) => line.includes('✓ slot empty'))).toBe(true);
        expect(invocations).toHaveLength(1);
        expect(lines.at(-1)).toContain('"replaced":{"slot":"ready","profile":"nexus","port":8085}');
    });

    it('never recycles a NON-Modal endpoint — only DELETE / is ever called there', async () => {
        const scripted = makeProbe([OK(HEALTH), busy(null), OK({ tornDown: true }), OK(EMPTY), OK(EMPTY), busy(null)]);
        const { deps } = makeDeps(scripted.probe);
        await runDeployEndpoint(bareFlags(), deps);
        expect(scripted.calls.some((call) => call.url.includes('sandbox'))).toBe(false);
    });

    it('--no-teardown refuses a busy slot before any deploy runs', async () => {
        const flags = flagsOf(['https://box.example', '--no-teardown']);
        const scripted = makeProbe([OK(HEALTH), busy(null)]);
        const { deps, problems, invocations } = makeDeps(scripted.probe);
        await expect(runDeployEndpoint(flags, deps)).rejects.toThrowError('--no-teardown forbids replacing it — pass --teardown to free it, or drop --no-teardown to let this command replace it');
        expect(scripted.calls.map((call) => call.method)).toEqual(['GET', 'GET']);
        expect(invocations).toEqual([]);
        expect(problems).toEqual([]);
    });

    it('reports hubUrl:null plus the runner\'s warning when the endpoint cannot name the service', async () => {
        const scripted = makeProbe([OK(HEALTH), OK(EMPTY), READY_NO_PREVIEW]);
        const { deps, lines } = makeDeps(scripted.probe);
        expect(await runDeployEndpoint(bareFlags(), deps)).toBe(0);
        expect(lines.some((line) => line.includes('hub (clients):    not reported (port 8085 has no preview URL'))).toBe(true);
        expect(lines.some((line) => line.includes('note:             Daytona sandboxes mint a per-port link with getPreviewLink(8085)'))).toBe(true);
        expect(lines.at(-1)).toContain('"hubUrl":null');
        expect(lines.at(-1)).toContain('"hubNote":"port 8085 has no preview URL — the driver has not pushed one"');
    });

    it('surfaces the child\'s exit code, and does not read the hub URL after a failed deploy', async () => {
        const scripted = makeProbe([OK(HEALTH), OK(EMPTY)]);
        const { deps, problems, invocations } = makeDeps(scripted.probe, 0, { spawnCode: 1 });
        expect(await runDeployEndpoint(bareFlags(), deps)).toBe(1);
        expect(scripted.calls).toHaveLength(2);
        expect(problems[0]).toContain('✗ deploy failed (exit 1)');
        expect(invocations).toHaveLength(1);
    });

    it('refuses to deploy after a DELETE that leaves the slot busy', async () => {
        const scripted = makeProbe([OK(HEALTH), busy(null), OK({ tornDown: true }), OK({ slot: 'stopping', runtime: {} }), OK({ slot: 'stopping', runtime: {} })]);
        const { deps } = makeDeps(scripted.probe);
        await expect(runDeployEndpoint(bareFlags(), deps)).rejects.toThrowError(/DELETE \/ was accepted but the slot still reads "stopping"/);
    });

    it('refuses to deploy when the DELETE itself is not confirmed', async () => {
        const scripted = makeProbe([OK(HEALTH), busy(null), OK({ slot: 'ready' })]);
        const { deps } = makeDeps(scripted.probe);
        await expect(runDeployEndpoint(bareFlags(), deps)).rejects.toThrowError(/did not confirm the teardown/);
    });

    it('a failed cold start ends as a clear timeout, not a stack', async () => {
        const scripted = makeProbe(Array.from({ length: 500 }, () => FAIL(503, 'sandbox is starting')));
        const { deps, lines } = makeDeps(scripted.probe);
        await expect(runDeployEndpoint(bareFlags(), deps)).rejects.toThrowError(/GET \/health never answered/);
        expect(lines[1]).toContain('waiting up to');
    });
});

// ══════════════════════════════════════════════════════════════════════════════
//  The Modal container reset — the security half of this script. The container is
//  ASKED what it holds: an empty slot is taken as the fresh machine, a busy slot
//  replaces the CONTAINER before the POST so nothing the old one held survives.
// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the Modal container reset', () => {
    it('classifies a Modal box and a Modal sandbox, and leaves everything else alone', () => {
        expect(classifyModalContainer(MODAL_BOX, MODAL_HEALTH)).toBe('box');
        expect(classifyModalContainer(MODAL_SANDBOX, MODAL_HEALTH)).toBe('sandbox');
        // The Daytona twin reports no `service` marker, so it is never replaceable.
        expect(classifyModalContainer(MODAL_BOX, HEALTH)).toBeNull();
        // A Modal runner on loopback (a host smoke run) has no platform to relaunch it.
        expect(classifyModalContainer('http://127.0.0.1:7777', MODAL_HEALTH)).toBeNull();
        // A Daytona preview URL is not a Modal host even if the marker appeared.
        expect(classifyModalContainer('https://proxy.example/sd/7777', MODAL_HEALTH)).toBeNull();
    });

    it('uses an EMPTY-slot Modal box as-is — nothing is in it, so it is the fresh machine', async () => {
        const scripted = makeProbe([OK(MODAL_HEALTH), OK(EMPTY), busy(`${MODAL_BOX}/`)]);
        const { deps, lines, invocations } = makeDeps(scripted.probe);
        expect(await runDeployEndpoint(flagsOf([MODAL_BOX]), deps)).toBe(0);
        expect(scripted.calls.map((call) => `${call.method} ${call.url}`)).toEqual([`GET ${MODAL_BOX}/health`, `GET ${MODAL_BOX}/status`, `GET ${MODAL_BOX}/status`]);
        expect(lines.some((line) => line.includes('slot is empty — nothing is running in the container, so it is used as the fresh machine (no reset)'))).toBe(true);
        expect(invocations).toHaveLength(1);
        // No reset happened, so the audit line must not claim one.
        expect(lines.at(-1)).not.toContain('"recycled"');
    });

    it('REPLACES a BUSY Modal box: DELETE /sandbox, then wait for a NEW container with an empty slot', async () => {
        const scripted = makeProbe([
            OK(MODAL_HEALTH), // preflight
            busy(`${MODAL_BOX}/`), // what the old container held
            OK({ tornDown: true }), // DELETE /sandbox
            OK(MODAL_HEALTH), // the replacement: a young uptime proves it started after the delete
            OK(EMPTY), // and its slot is empty
            busy(`${MODAL_BOX}/`), // after the deploy
        ]);
        const { deps, lines, invocations } = makeDeps(scripted.probe);
        expect(await runDeployEndpoint(flagsOf([MODAL_BOX]), deps)).toBe(0);
        expect(scripted.remaining()).toBe(0);
        expect(scripted.calls.map((call) => `${call.method} ${call.url}`)).toEqual([
            `GET ${MODAL_BOX}/health`,
            `GET ${MODAL_BOX}/status`,
            `DELETE ${MODAL_BOX}/sandbox`,
            `GET ${MODAL_BOX}/health`,
            `GET ${MODAL_BOX}/status`,
            `GET ${MODAL_BOX}/status`,
        ]);
        // The runtime teardown is part of the container reset; DELETE / would leave the
        // machine — and everything malicious that survived in it — in place.
        expect(scripted.calls.some((call) => call.method === 'DELETE' && call.url.endsWith('/'))).toBe(false);
        expect(lines.some((line) => line.includes(`DELETE ${MODAL_BOX}/sandbox — the slot holds a ready runtime, so the container is replaced`))).toBe(true);
        expect(lines.some((line) => line.includes('✓ replacement container is up — the deploy lands in THIS container'))).toBe(true);
        expect(invocations).toHaveLength(1);
        // The machine-readable line records the reset, so a consumer can audit it.
        expect(lines.at(-1)).toContain('"recycled":"box"');
        expect(lines.at(-1)).not.toContain('"replaced"');
    });

    it('accepts a replacement first seen while the old container was gone (a non-2xx proves the swap)', async () => {
        // The uptime check and the "old one left" observation are independent proofs;
        // a 503 gap is enough on its own, which is what this pins.
        const scripted = makeProbe([OK(MODAL_HEALTH), busy(null), OK({ tornDown: true }), FAIL(503), OK(MODAL_HEALTH), OK(EMPTY), busy(null)]);
        const { deps, lines } = makeDeps(scripted.probe);
        expect(await runDeployEndpoint(flagsOf([MODAL_BOX]), deps)).toBe(0);
        expect(scripted.calls.some((call) => call.url === `${MODAL_BOX}/sandbox`)).toBe(true);
        expect(lines.some((line) => line.includes('"recycled":"box"'))).toBe(true);
    });

    it('--no-teardown keeps its hands off the Modal container', async () => {
        const scripted = makeProbe([OK(MODAL_HEALTH), OK(EMPTY), busy(`${MODAL_BOX}/`)]);
        const { deps } = makeDeps(scripted.probe);
        expect(await runDeployEndpoint(flagsOf([MODAL_BOX, '--no-teardown']), deps)).toBe(0);
        expect(scripted.calls.map((call) => `${call.method} ${call.url}`)).toEqual([`GET ${MODAL_BOX}/health`, `GET ${MODAL_BOX}/status`, `GET ${MODAL_BOX}/status`]);
        expect(scripted.calls.some((call) => call.url.includes('/sandbox'))).toBe(false);
    });

    it('uses an EMPTY-slot Modal sandbox as supplied', async () => {
        const scripted = makeProbe([OK(MODAL_HEALTH), OK(EMPTY), busy(null)]);
        const { deps } = makeDeps(scripted.probe);
        expect(await runDeployEndpoint(flagsOf([MODAL_SANDBOX]), deps)).toBe(0);
        expect(scripted.calls.some((call) => call.url.includes('/sandbox'))).toBe(false);
    });

    it('REFUSES a BUSY Modal sandbox tunnel — its URL dies with the container, so it cannot be reset in place', async () => {
        const scripted = makeProbe([OK(MODAL_HEALTH), busy(null)]);
        const { deps, invocations } = makeDeps(scripted.probe);
        await expect(runDeployEndpoint(flagsOf([MODAL_SANDBOX]), deps)).rejects.toThrowError(/Modal SANDBOX tunnel \(…modal\.host\) holding a ready runtime/);
        expect(scripted.calls.map((call) => `${call.method} ${call.url}`)).toEqual([`GET ${MODAL_SANDBOX}/health`, `GET ${MODAL_SANDBOX}/status`]);
        expect(invocations).toEqual([]);
    });

    it('--teardown on a Modal box frees the slot and never resets the container', async () => {
        const scripted = makeProbe([OK(MODAL_HEALTH), busy(null), OK({ tornDown: true }), OK(EMPTY)]);
        const { deps } = makeDeps(scripted.probe);
        expect(await runDeployEndpoint(flagsOf([MODAL_BOX, '--teardown']), deps)).toBe(0);
        expect(scripted.calls.some((call) => call.url.includes('/sandbox'))).toBe(false);
    });

    it('refuses to deploy when DELETE /sandbox is not confirmed', async () => {
        const scripted = makeProbe([OK(MODAL_HEALTH), busy(null), OK({ slot: 'ready' })]);
        const { deps, invocations } = makeDeps(scripted.probe);
        await expect(runDeployEndpoint(flagsOf([MODAL_BOX]), deps)).rejects.toThrowError(/did not confirm the container reset/);
        expect(invocations).toEqual([]);
    });

    it('refuses to deploy while the OLD container is still the one answering', async () => {
        // A long-lived box's uptime can never pass the freshness check, so a reset that
        // did not actually replace the process fails closed. The budget is frozen in
        // tests, so the attempt cap ends the loop: ceil(RECYCLE_WAIT_MS / 2000 ms).
        const stubborn = Array.from({ length: Math.ceil(RECYCLE_WAIT_MS / 2_000) }, () => OK(MODAL_OLD_HEALTH));
        const scripted = makeProbe([OK(MODAL_HEALTH), busy(null), OK({ tornDown: true }), ...stubborn]);
        const { deps, invocations } = makeDeps(scripted.probe);
        await expect(runDeployEndpoint(flagsOf([MODAL_BOX]), deps)).rejects.toThrowError(/never reported a fresh, empty container after DELETE \/sandbox \(last: the old container is still answering\)/);
        expect(invocations).toEqual([]);
    });

    it('refuses to deploy when the replacement container is not empty', async () => {
        const perAttempt = Array.from({ length: Math.ceil(RECYCLE_WAIT_MS / 2_000) }, () => [OK(MODAL_HEALTH), busy(null)]).flat();
        const scripted = makeProbe([OK(MODAL_HEALTH), busy(null), OK({ tornDown: true }), ...perAttempt]);
        const { deps, invocations } = makeDeps(scripted.probe);
        await expect(runDeployEndpoint(flagsOf([MODAL_BOX]), deps)).rejects.toThrowError(/never reported a fresh, empty container after DELETE \/sandbox \(last: a fresh container whose slot reads "ready"\)/);
        expect(invocations).toEqual([]);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the teardown verb', () => {
    it('frees the slot and ships nothing', async () => {
        const flags = flagsOf(['https://box.example', '--teardown']);
        const scripted = makeProbe([OK(HEALTH), busy('https://box.example'), OK({ tornDown: true }), OK(EMPTY)]);
        const { deps, lines, invocations } = makeDeps(scripted.probe);
        expect(await runDeployEndpoint(flags, deps)).toBe(0);
        expect(invocations).toEqual([]);
        expect(scripted.calls.map((call) => call.method)).toEqual(['GET', 'GET', 'DELETE', 'GET']);
        expect(lines.at(-2)).toContain('✓ no runtime holds https://box.example');
        expect(lines.at(-1)).toBe('{"mode":"teardown","endpoint":"https://box.example","slot":"empty","replaced":{"slot":"ready","profile":"nexus","port":8085}}');
    });

    it('an already-empty slot is a no-op, not a DELETE', async () => {
        const flags = flagsOf(['https://box.example', '--teardown']);
        const scripted = makeProbe([OK(HEALTH), OK(EMPTY)]);
        const { deps, lines, invocations } = makeDeps(scripted.probe);
        expect(await runDeployEndpoint(flags, deps)).toBe(0);
        expect(scripted.calls.map((call) => call.method)).toEqual(['GET', 'GET']);
        expect(invocations).toEqual([]);
        expect(lines.at(-1)).toBe('{"mode":"teardown","endpoint":"https://box.example","slot":"empty"}');
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
            const flags = flagsOf(['https://box.example', '--dry-run', '--token', 's3cret', '--env', 'NEXUS_TOKEN=xyz']);
            const scripted = makeProbe([]);
            const { deps, lines, invocations } = makeDeps(scripted.probe);
            expect(await runDeployEndpoint(flags, deps)).toBe(0);
            expect(scripted.calls).toEqual([]);
            expect(invocations).toEqual([]);
            expect(fetchSpy).not.toHaveBeenCalled();
            expect(lines[0]).toContain('DRY RUN — no probe, no DELETE, no child process, no cargo, no network');
            expect(lines.some((line) => line.includes('preflight: GET https://box.example/health  →  GET https://box.example/status  →  on a BUSY slot: DELETE /sandbox on a Modal box (the container is replaced) or DELETE / anywhere else'))).toBe(true);
            expect(lines.some((line) => line.includes('x-daytona-preview-token: *** (length 6)'))).toBe(true);
            expect(lines.some((line) => line.includes('a busy slot is replaced'))).toBe(true);
            expect(lines.some((line) => line.includes('hub URL after success: read from GET /status → runtime.preview.url (never guessed)'))).toBe(true);
            // The dry run prints exactly the argv it WOULD have run.
            expect(lines.some((line) => line.trim().endsWith(`${VITE_NODE_CLI} ${DEPLOY_SCRIPT_PATH} --endpoint https://box.example --token s3cret --mode local --port 8085 --env NEXUS_TOKEN=xyz`))).toBe(true);
            expect(lines.at(-1)).not.toMatch(/^\{"/); // no machine-readable line for a plan
        } finally {
            vi.unstubAllGlobals();
        }
    });

    it('dry-runs the teardown and strict postures without inventing work', async () => {
        const teardown = makeDeps(makeProbe([]).probe);
        expect(await runDeployEndpoint(flagsOf(['https://box.example', '--dry-run', '--teardown']), teardown.deps)).toBe(0);
        expect(teardown.lines.some((line) => line.includes('mode: teardown — frees the slot and stops here'))).toBe(true);

        const strict = makeDeps(makeProbe([]).probe);
        expect(await runDeployEndpoint(flagsOf(['https://box.example', '--dry-run', '--no-teardown']), strict.deps)).toBe(0);
        expect(strict.lines.some((line) => line.includes('a busy slot is an error (--no-teardown)'))).toBe(true);

        const shipped = makeDeps(makeProbe([]).probe);
        expect(await runDeployEndpoint(flagsOf(['https://box.example', '--dry-run', '--mode', 'remote-build']), shipped.deps)).toBe(0);
        expect(shipped.lines.some((line) => line.includes('--mode remote-build --port 8085'))).toBe(true);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: main() maps results onto exit codes', () => {
    it('--help prints the usage and never touches the network', async () => {
        const scripted = makeProbe([]);
        const { deps, lines } = makeDeps(scripted.probe);
        expect(await main(['--help'], deps)).toBe(0);
        // USAGE goes out as ONE log call — one line in the sink, verbatim.
        expect(lines).toEqual([USAGE]);
        expect(scripted.calls).toEqual([]);
    });

    it('a bad command line exits 1 with every problem and the usage', async () => {
        const scripted = makeProbe([]);
        const { deps, problems } = makeDeps(scripted.probe);
        expect(await main(['--port', '99999'], deps)).toBe(1);
        expect(problems[0]).toContain('✗ --port "99999" is out of range');
        expect(problems.some((line) => line.includes('no endpoint'))).toBe(true);
        expect(problems.at(-2)).toBe('');
        expect(problems.at(-1)).toBe(USAGE);
        expect(scripted.calls).toEqual([]);
    });

    it('a dead URL exits 1 with one actionable line', async () => {
        const scripted = makeProbe([FAIL(404, '<html>App not found</html>')]);
        const { deps, problems, lines } = makeDeps(scripted.probe);
        expect(await main(['https://typo--container-sandbox.modal.run'], deps)).toBe(1);
        expect(problems).toHaveLength(1);
        expect(problems[0]).toContain('that is not a daytona-runtime-v1 runner');
        expect(lines.at(-1)).not.toMatch(/^\{"/);
    });

    it('an operator error and a bug are reported differently', async () => {
        const throwing = makeDeps(async () => {
            throw new Error('boom');
        });
        expect(await main(['https://box.example'], throwing.deps)).toBe(1);
        expect(throwing.problems[0]).toContain('unexpected failure');

        const guarded = makeDeps(async () => {
            throw new EndpointDeployError('pass --token');
        });
        expect(await main(['https://box.example'], guarded.deps)).toBe(1);
        expect(guarded.problems).toEqual(['[nexus:endpoint] ✗ pass --token']);
    });

    it('a full run through main() exits 0 and ends with the summary line', async () => {
        const scripted = makeProbe([OK(HEALTH), OK(EMPTY), busy('https://box.example')]);
        const { deps, lines } = makeDeps(scripted.probe);
        expect(await main(['https://box.example'], deps)).toBe(0);
        // Nothing but JSON on the last line — that is what a chained consumer parses.
        expect(JSON.parse(lines.at(-1)!)).toMatchObject({ mode: 'deploy', endpoint: 'https://box.example', hubUrl: 'https://box.example', hubPort: 8085 });
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: spawnDeploy — the one real side effect (SpawnImpl seam)', () => {
    const child = () => {
        const listeners: Record<string, ((arg?: unknown) => void)[]> = {};
        const spawned = {
            on: vi.fn((event: string, listener: (arg?: unknown) => void) => {
                (listeners[event] ??= []).push(listener);
                return spawned;
            }),
            emit: (event: string, arg?: unknown) => {
                for (const listener of listeners[event] ?? []) listener(arg);
            },
        };
        return spawned;
    };

    it('inherits stdio, runs from the package root, and forwards the child env untouched', async () => {
        const spawned = child();
        const impl = vi.fn(() => spawned);
        const promise = spawnDeploy({ command: 'node', args: ['a', 'b'], cwd: PACKAGE_ROOT, env: { PATH: '/x', NEXUS_TOKEN: 'secret' } }, impl);
        spawned.emit('close', 0);
        expect(await promise).toBe(0);
        expect(impl).toHaveBeenCalledWith('node', ['a', 'b'], { cwd: PACKAGE_ROOT, env: { PATH: '/x', NEXUS_TOKEN: 'secret' }, stdio: 'inherit' });
    });

    it('maps every non-zero outcome to a non-zero exit code', async () => {
        const failed = child();
        const failedRun = spawnDeploy({ command: 'node', args: [], cwd: PACKAGE_ROOT, env: {} }, () => failed);
        failed.emit('close', 7);
        expect(await failedRun).toBe(7);

        const signalled = child();
        const signalledRun = spawnDeploy({ command: 'node', args: [], cwd: PACKAGE_ROOT, env: {} }, () => signalled);
        signalled.emit('close', null); // SIGINT/SIGTERM: no numeric code
        expect(await signalledRun).toBe(1);

        const missing = child();
        const missingRun = spawnDeploy({ command: 'node', args: [], cwd: PACKAGE_ROOT, env: {} }, () => missing);
        missing.emit('error', new Error('ENOENT vite-node'));
        expect(await missingRun).toBe(1);
    });
});

// ══════════════════════════════════════════════════════════════════════════════
describe('deploy-endpoint: the import-is-a-no-op guard', () => {
    const module = join(PACKAGE_ROOT, 'scripts', 'deploy-endpoint.ts');

    it('never runs under vitest, whatever the argv looks like', () => {
        expect(isCliEntryPoint({ vitest: true, argv: ['node', module], modulePath: module })).toBe(false);
    });

    it('runs when the script positional names this file (direct and --script forms)', () => {
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', module], modulePath: module })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node', '--script', 'scripts/deploy-endpoint.ts'], modulePath: module })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node', '--script', 'scripts/deploy-endpoint.ts', 'https://x'], modulePath: module })).toBe(true);
    });

    it('runs when the npm lifecycle line names it, and not when it names a sibling', () => {
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], env: { npm_lifecycle_script: 'vite-node scripts/deploy-endpoint.ts' }, modulePath: module })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], env: { npm_lifecycle_script: 'vite-node scripts/deploy-daytona.ts' }, modulePath: module })).toBe(false);
    });

    it('runs when vite-node evaluated this file as its entry, and not when another module imported it', () => {
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], modulePath: module, stack: `Error\n    at Object.<anonymous> (${module}:1:1)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], modulePath: module, stack: `Error\n    at Object.<anonymous> (${module}:1:1)\n    at consumer (/repo/packages/typecrate/nexus/scripts/consumer.ts:2:2)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(false);
        // A sibling's module id in the stack is not evidence for this file.
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], modulePath: module, stack: 'Error\n    at Object.<anonymous> (/repo/other.ts:1:1)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)' })).toBe(false);
    });

    it('ignores module query strings, and rejects plain node without any runner evidence', () => {
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', `${module}?v=3`], modulePath: module })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node'], modulePath: module, stack: new Error().stack })).toBe(false);
        expect(isCliEntryPoint({})).toBe(false);
    });
});
