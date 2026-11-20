// ─────────────────────────────────────────────────────────────────────────────
//  start-daytona.test.ts — the orchestrator, end to end, with ZERO network.
//
//  Everything side-effectful in start-daytona.ts sits behind an injected dep
//  (createSandbox / spawn / log / error / env), so these tests drive the REAL
//  control flow with fakes and assert EXACT values:
//    · parser     — defaults (AUTO port, no admin token), every flag, --env repeats,
//                   out-of-range words, and each illegal combination
//    · plan / argv— the whole deploy argv array per mode, plus command/cwd/env
//    · summary    — endpoint + /app/nexus/ in fresh AND reuse mode, hubToken as the
//                   SAME credential as token, hubPort only when explicitly typed
//    · flow       — fresh success (create once → spawn once → derived hub URL → the
//                   machine-readable last line, and NO getPreviewLink call), deploy
//                   failure (delete / --keep-on-fail / a delete that fails), reuse
//                   (derived URL, no lifecycle), teardown (DELETE /apps/nexus only),
//                   dry-run (no create, no spawn, no fetch), exit codes
//    · interrupt  — decideInterruptCleanup's v2 phase table, and the fresh-mode fence
//                   fired through an injected onSignal: SIGINT/SIGTERM mid-deploy
//                   delete the sandbox and exit 130/143, a hung delete still exits
//                   after the bounded wait, and reuse/teardown/dry-run trap nothing
//    · spawnDeploy— the one real side effect, driven through the injectable spawner
//                   seam (see SpawnImpl in the source for why vi.mock cannot rebind a
//                   builtin here): argv/cwd/env/stdio and the exit-code mapping
//    · import     — isCliEntryPoint's entry rules, i.e. the guarantee that
//                   importing the module starts nothing
//
//  @services/daytona/sandbox is vi.mock'd so the real SDK / @config/vault modules
//  are never loaded here; the only other global touched is `fetch`, stubbed as a
//  tripwire in the dry-run test (root vitest env is jsdom, where fetch is flaky).
//  No cargo build is ever started: the real child process is only ever reached
//  through the injected SpawnImpl seam.
// ─────────────────────────────────────────────────────────────────────────────

import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';

// Hoisted above the imports by vitest: the barrel resolves to a spy, not to the real
// SDK, so nothing in this file can reach Daytona even by accident.
vi.mock('@services/daytona/sandbox', () => ({
    createSandbox: vi.fn(),
    // The real barrel's §12.3 env names, so the production resolver is exercised
    // against the same spellings the shared module exports.
    ADMIN_TOKEN_ENV: 'SANDBOX_ADMIN_TOKEN',
    ADMIN_TOKEN_FILE_ENV: 'SANDBOX_ADMIN_TOKEN_FILE',
}));

import { createSandbox } from '@services/daytona/sandbox';
import {
    buildDeployArgs,
    buildPlan,
    buildSandboxOptions,
    buildSummary,
    createDefaultDeps,
    decideInterruptCleanup,
    DEPLOY_SCRIPT_PATH,
    HUB_PATH,
    INTERRUPT_DELETE_TIMEOUT_MS,
    isCliEntryPoint,
    main,
    needsPreviewToken,
    PACKAGE_ROOT,
    parseArgs,
    previewLabel,
    resolveMode,
    runStartDaytona,
    spawnDeploy,
    USAGE,
    VITE_NODE_CLI,
    type CreateSandboxOptions,
    type DeployInvocation,
    type InterruptPhase,
    type SandboxHandle,
    type SpawnImpl,
    type StartDaytonaDeps,
    type StartDaytonaFlags,
    type StartSignal,
} from './start-daytona';

/** The library spy behind the mocked barrel (see createDefaultDeps below). */
const createSandboxMock = createSandbox as unknown as ReturnType<typeof vi.fn>;

/** Flags exactly as the CLI produces them — one source of truth for the defaults. */
const flagsOf = (argv: readonly string[]): StartDaytonaFlags => parseArgs(argv).flags;

/** The env object handed to every fake; identity is asserted to prove it is untouched. */
const ENV = { PATH: '/usr/bin', HTTPS_PROXY: 'http://proxy:3128' };

/** A real 0600 admin-token file under the repo-root Temp scratch (AGENTS.md). */
const ADMIN_TOKEN = 'file-admin-token-0123456789';
const writeAdminTokenFile = (): string => {
    const path = join(PACKAGE_ROOT, '..', '..', '..', 'Temp', 't7b', 'admin-token');
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, `${ADMIN_TOKEN}\n`, { mode: 0o600 });
    return path;
};

/**
 * What a fake createSandbox resolves: a structural SandboxHandle with recorded calls.
 * `getPreviewLink` is present ON PURPOSE even though the v2 handle type no longer has
 * it: the regression this suite owns is that a fresh/reuse run NEVER calls it.
 */
const makeHandle = (overrides: Partial<SandboxHandle> = {}) =>
    ({
        endpoint: 'https://proxy.example/runner-7777',
        token: 'dtn_preview_token',
        sandboxId: 'sbx-1234',
        name: 'nexus-1',
        public: false,
        sandbox: {
            delete: vi.fn(async () => undefined),
            getPreviewLink: vi.fn(async (port: number) => ({ url: `https://proxy.example/hub-${port}`, token: 'hub_token' })),
        },
        ...overrides,
    }) as SandboxHandle & { sandbox: { delete: ReturnType<typeof vi.fn>; getPreviewLink: ReturnType<typeof vi.fn> } };

/** A dep set whose every side effect is a spy, plus the captured sinks for assertions. */
const makeDeps = (handle: SandboxHandle | undefined, exitCode = 0) => {
    const lines: string[] = [];
    const problems: string[] = [];
    const invocations: DeployInvocation[] = [];
    const createSpy = vi.fn(async (_options: CreateSandboxOptions) => {
        if (!handle) throw new Error('createSandbox must not run in this mode');
        return handle;
    });
    const spawnSpy = vi.fn(async (invocation: DeployInvocation) => {
        invocations.push(invocation);
        return exitCode;
    });
    const deps: StartDaytonaDeps = { createSandbox: createSpy, spawn: spawnSpy, log: (line) => lines.push(line), error: (line) => problems.push(line), env: ENV };
    return { deps, createSpy, spawnSpy, lines, problems, invocations };
};

/**
 * Deps whose signal wiring is CAPTURED rather than installed, so a test can fire a
 * signal at an exact instant of the flow (the same instant a real Ctrl-C lands) and
 * so `trapped` doubles as the per-mode evidence that reuse/teardown/dry-run arm nothing.
 */
const makeInterruptDeps = (
    handle: SandboxHandle,
    options: {
        signal?: StartSignal;
        /** false = record the wiring but never fire, for the "only the happy path ran" test. */
        fire?: boolean;
        /** Override sandbox.delete to model a failing or a never-returning delete. */
        deleteImpl?: () => Promise<unknown>;
        /** Override the bounded wait so the timeout branch needs no real 10s. */
        deleteTimeout?: (ms: number) => Promise<'timeout'>;
    } = {},
) => {
    const lines: string[] = [];
    const problems: string[] = [];
    /** signal → live listener. Empty after a run means nothing is left trapped. */
    const trapped = new Map<StartSignal, () => void>();
    const signal = options.signal ?? 'SIGINT';
    /** Fires at most once, while the deploy child is in flight. */
    let fired = false;
    const fire = (): void => {
        if (fired) return;
        fired = true;
        trapped.get(signal)?.();
    };
    if (options.deleteImpl) handle.sandbox.delete = vi.fn(options.deleteImpl) as unknown as typeof handle.sandbox.delete;
    const deps: StartDaytonaDeps = {
        createSandbox: vi.fn(async () => handle),
        spawn: vi.fn(async () => {
            if (options.fire !== false) fire();
            return 0;
        }),
        log: (line) => lines.push(line),
        error: (line) => problems.push(line),
        env: ENV,
        onSignal: (name, listener) => {
            trapped.set(name, listener);
            return () => {
                trapped.delete(name);
            };
        },
        ...(options.deleteTimeout ? { deleteTimeout: options.deleteTimeout } : {}),
    };
    return { deps, lines, problems, trapped };
};

/**
 * A fake ChildProcess: every `on()` is recorded so a test can emit exactly one
 * lifecycle event, and `mock.calls` on the spawn spy shows what was requested.
 */
const makeChild = () => {
    const listeners: Record<string, ((arg?: unknown) => void)[]> = {};
    const child = {
        on: vi.fn((event: string, listener: (arg?: unknown) => void) => {
            (listeners[event] ??= []).push(listener);
            return child;
        }),
        /** Fires every listener registered for `event` (mirrors EventEmitter ordering). */
        emit: (event: string, arg?: unknown): void => {
            for (const listener of listeners[event] ?? []) listener(arg);
        },
    };
    return { child, listeners };
};

describe('start-daytona: module pins (paths resolved from import.meta.url, never cwd)', () => {
    it('resolves the sibling script inside the package root, and it exists', () => {
        expect(PACKAGE_ROOT.endsWith(join('packages', 'typecrate', 'nexus'))).toBe(true);
        expect(DEPLOY_SCRIPT_PATH).toBe(join(PACKAGE_ROOT, 'scripts', 'deploy-daytona.ts'));
        expect(existsSync(DEPLOY_SCRIPT_PATH)).toBe(true);
    });

    it('names the one public app path, and has no per-port default left to drift', () => {
        expect(HUB_PATH).toBe('/app/nexus/');
        expect(flagsOf([]).port).toBeUndefined();
    });
});

describe('start-daytona: argument parsing', () => {
    it('defaults to a fresh local deploy with an AUTO port and no admin token', () => {
        const { flags, errors } = parseArgs([]);
        expect(errors).toEqual([]);
        expect(flags).toEqual({
            help: false,
            dryRun: false,
            teardown: false,
            keepOnFail: false,
            apiKey: '',
            public: true,
            endpoint: '',
            token: '',
            adminToken: '',
            mode: 'local',
            port: undefined,
            replace: false,
            env: {},
            noBuild: false,
            artifact: '',
            wait: true,
        });
    });

    it('parses every flag into the exact flag record', () => {
        const { flags, errors } = parseArgs(['--api-key', 'dtn_key', '--ttl', '30', '--private', '--snapshot', 'toolchain-v1', '--cpu', '2', '--mem', '4', '--disk', '20', '--mode', 'remote-build', '--port', '9000', '--replace', '--admin-token', 'at', '--env', 'A=1', '--no-wait', '--keep-on-fail']);
        expect(errors).toEqual([]);
        expect(flags).toEqual({
            help: false,
            dryRun: false,
            teardown: false,
            keepOnFail: true,
            apiKey: 'dtn_key',
            ttl: 30,
            snapshot: 'toolchain-v1',
            cpu: 2,
            mem: 4,
            disk: 20,
            public: false,
            endpoint: '',
            token: '',
            adminToken: 'at',
            mode: 'remote-build',
            port: 9000,
            replace: true,
            env: { A: '1' },
            noBuild: false,
            artifact: '',
            wait: false,
        });
    });

    it('accumulates repeated --env in order and keeps "=" inside values', () => {
        const { flags, errors } = parseArgs(['--env', 'B=2', '--env', 'A=1', '--env', 'TOKEN=a=b']);
        expect(errors).toEqual([]);
        // Key order IS the typed order (JS string-key guarantee) — the deploy argv relies on it.
        expect(Object.keys(flags.env)).toEqual(['B', 'A', 'TOKEN']);
        expect(flags.env).toEqual({ B: '2', A: '1', TOKEN: 'a=b' });
    });

    it('lets the last of --public/--private win (the service CLI rule)', () => {
        expect(flagsOf(['--private', '--public']).public).toBe(true);
        expect(flagsOf(['--public', '--private']).public).toBe(false);
    });

    it('reports unknown flags and missing values with the deploy script wording', () => {
        expect(parseArgs(['--wat']).errors).toEqual(['unknown flag "--wat"']);
        expect(parseArgs(['--endpoint']).errors).toEqual(['--endpoint needs a value']);
        // A `--`-leading token after a value flag is a missing value, not a value.
        expect(parseArgs(['--token', '--dry-run']).errors).toEqual(['--token needs a value']);
    });

    it('rejects --env without a key', () => {
        expect(parseArgs(['--env', 'NOPE']).errors).toEqual(['--env expects KEY=VALUE, got "NOPE"']);
    });

    it('range-checks every numeric flag, with --port 0 meaning AUTO', () => {
        expect(parseArgs(['--port', '-1']).errors).toEqual(['--port "-1" is out of range (TCP port 0-65535 (0 = auto))']);
        expect(parseArgs(['--port', '65536']).errors).toEqual(['--port "65536" is out of range (TCP port 0-65535 (0 = auto))']);
        expect(parseArgs(['--cpu', 'zero']).errors).toEqual(['--cpu "zero" is out of range (vCPU, >= 1)']);
        expect(parseArgs(['--ttl', '-1']).errors).toEqual(['--ttl "-1" is out of range (minutes, >= 0 (0 = disabled))']);
        // ttl is the ONE flag where 0 is meaningful ("no fence").
        expect(parseArgs(['--ttl', '0']).errors).toEqual([]);
        expect(flagsOf(['--ttl', '0']).ttl).toBe(0);
        // port 0 is the explicit spelling of auto, and must land as "no port".
        expect(parseArgs(['--port', '0']).errors).toEqual([]);
        expect(flagsOf(['--port', '0']).port).toBeUndefined();
        expect(flagsOf(['--port', '20042']).port).toBe(20042);
    });

    it('rejects an unknown --mode and the remote-build build-skip contradiction', () => {
        expect(parseArgs(['--mode', 'weird']).errors).toEqual(['--mode must be "local" or "remote-build", got "weird"']);
        expect(parseArgs(['--mode', 'remote-build', '--no-build']).errors).toEqual(['--no-build and --artifact apply to --mode local only (remote-build ships a source tarball)']);
        expect(parseArgs(['--mode', 'remote-build', '--artifact', 'x']).errors).toEqual(['--no-build and --artifact apply to --mode local only (remote-build ships a source tarball)']);
    });

    it('rejects --teardown without --endpoint (a fresh registry has nothing to tear down)', () => {
        expect(parseArgs(['--teardown']).errors).toEqual(['--teardown needs --endpoint <url> — a fresh sandbox has an empty app registry, and this script never tears down a sandbox it did not create']);
        expect(parseArgs(['--teardown']).flags.teardown).toBe(true);
        expect(parseArgs(['--teardown', '--replace', '--endpoint', 'https://proxy.example/runner-7777']).errors).toEqual(['--teardown and --replace are different verbs — pick one']);
        expect(parseArgs(['--teardown', '--endpoint', 'https://proxy.example/runner-7777']).errors).toEqual([]);
    });

    it('rejects sandbox-creation flags combined with --endpoint, naming only the offenders', () => {
        expect(parseArgs(['--endpoint', 'https://e.example', '--cpu', '2']).errors).toEqual(['--cpu only apply when this script creates the sandbox — drop --endpoint (fresh) or drop it (reusing https://e.example)']);
        expect(parseArgs(['--endpoint', 'https://e.example', '--private', '--ttl', '5']).errors).toEqual(['--private, --ttl only apply when this script creates the sandbox — drop --endpoint (fresh) or drop them (reusing https://e.example)']);
        // --token/--admin-token alone are targeting, not creation, so they stay legal.
        expect(parseArgs(['--endpoint', 'https://e.example', '--token', 't', '--admin-token', 'a', '--keep-on-fail']).errors).toEqual([]);
    });

    it('rejects --token without --endpoint: a fresh run takes its token from createSandbox', () => {
        const TOKEN_IS_TARGETING = '--token only applies when you reuse an endpoint — a fresh run authenticates with the token createSandbox returns; drop --token or add --endpoint <url>';
        // The real fresh run IGNORES a typed --token (the created token wins) while
        // buildPlan would print it, so it is a parse error, not a silent surprise.
        expect(parseArgs(['--token', 't']).errors).toEqual([TOKEN_IS_TARGETING]);
        expect(parseArgs(['--dry-run', '--token', 't']).errors).toEqual([TOKEN_IS_TARGETING]);
        expect(resolveMode(parseArgs(['--token', 't']).flags)).toBe('fresh');
        // Reuse and teardown still accept it: those are the modes that target an endpoint.
        expect(parseArgs(['--endpoint', 'https://e.example', '--token', 't']).errors).toEqual([]);
        expect(parseArgs(['--teardown', '--endpoint', 'https://e.example', '--token', 't']).errors).toEqual([]);
        // teardown WITHOUT an endpoint reports only the missing endpoint, never both.
        expect(parseArgs(['--teardown', '--token', 't']).errors).toEqual(['--teardown needs --endpoint <url> — a fresh sandbox has an empty app registry, and this script never tears down a sandbox it did not create']);
    });

    it('allows --dry-run with --endpoint (the reuse plan must be printable)', () => {
        const { flags, errors } = parseArgs(['--dry-run', '--endpoint', 'https://proxy.example/runner-7777', '--token', 't']);
        expect(errors).toEqual([]);
        expect(resolveMode(flags)).toBe('reuse');
        expect(flags.dryRun).toBe(true);
    });
});

describe('start-daytona: mode resolution', () => {
    it('derives the mode from teardown/endpoint only', () => {
        expect(resolveMode({ teardown: false, endpoint: '' })).toBe('fresh');
        expect(resolveMode({ teardown: false, endpoint: 'https://e' })).toBe('reuse');
        expect(resolveMode({ teardown: true, endpoint: 'https://e' })).toBe('teardown');
        // teardown outranks reuse; fresh+teardown is impossible (the parser refuses it).
        expect(resolveMode({ teardown: true, endpoint: '' })).toBe('teardown');
    });
});

describe('start-daytona: sandbox option mapping', () => {
    it('maps the knobs that were given plus the fixed idle window, and never passes --port as the RUNNER port', () => {
        const options = buildSandboxOptions(flagsOf(['--api-key', 'dtn_key', '--ttl', '30', '--private', '--snapshot', 'snap', '--cpu', '2', '--mem', '4', '--disk', '20', '--port', '9000']), {});
        expect(options).toEqual({ apiKey: 'dtn_key', ttlMinutes: 30, idleMinutes: 30, public: false, snapshot: 'snap', cpu: 2, mem: 4, disk: 20 });
        // createSandbox's `port` is the RUNNER port (7777 = the management endpoint) while
        // --port is the APP's internal port; forwarding it would repoint the deploy.
        expect('port' in options).toBe(false);
    });

    it('defaults the sandbox to the measured minimal footprint: 1 vCPU / 1 GiB RAM / 3 GiB disk', () => {
        const options = buildSandboxOptions(flagsOf([]), {});
        expect(options).toEqual({ idleMinutes: 30, public: true, cpu: 1, mem: 1, disk: 3 }); // hub 7.7 MB RSS / ~0.25 ms per request; 3 GiB = Daytona's own disk default; 30 min = the idle reaper
        expect(options.apiKey).toBeUndefined(); // '' → absent, so DAYTONA_API_KEY/vault still apply
        expect(options.ttlMinutes).toBeUndefined(); // unset = NO wall-clock kill fence
        expect(options.snapshot).toBeUndefined(); // the service's own default snapshot
    });

    it('maps the §12.3 admin credential into the fresh boot options, file form first', () => {
        // Explicit flag → env form handed to createSandbox (SANDBOX_ADMIN_TOKEN).
        expect(buildSandboxOptions(flagsOf(['--admin-token', 'admin-token-value']), {})).toEqual({ idleMinutes: 30, public: true, cpu: 1, mem: 1, disk: 3, adminToken: 'admin-token-value' });
        // The shared alias and the generic name are both read when no flag is typed.
        expect(buildSandboxOptions(flagsOf([]), { NEXUS_SANDBOX_ADMIN_TOKEN: 'nexus-env-token-0123456789' })).toMatchObject({ adminToken: 'nexus-env-token-0123456789' });
        expect(buildSandboxOptions(flagsOf([]), { SANDBOX_ADMIN_TOKEN: 'env-token-0123456789' })).toMatchObject({ adminToken: 'env-token-0123456789' });
        // The preferred file form: only the PATH is passed; the value is read by the
        // shared bring-up (uploaded 0600) and by resolveAdminBearer for the Bearer.
        const filePath = writeAdminTokenFile();
        expect(buildSandboxOptions(flagsOf([]), { SANDBOX_ADMIN_TOKEN_FILE: filePath })).toEqual({ idleMinutes: 30, public: true, cpu: 1, mem: 1, disk: 3, adminTokenFile: filePath });
    });
});

describe('start-daytona: deploy argv per mode', () => {
    it('fresh with a private endpoint carries the preview token, in the documented order, with NO --port', () => {
        expect(buildDeployArgs(flagsOf(['--env', 'NEXUS_TOKEN=xyz']), { endpoint: 'https://proxy.example/runner-7777', token: 'dtn_preview_token' }, 'fresh')).toEqual([
            '--endpoint', 'https://proxy.example/runner-7777',
            '--token', 'dtn_preview_token',
            '--mode', 'local',
            '--env', 'NEXUS_TOKEN=xyz',
        ]);
    });

    it('forwards an explicit --port, --replace and --admin-token; omits --port when auto', () => {
        expect(buildDeployArgs(flagsOf(['--port', '20042', '--replace', '--admin-token', 'at', '--mode', 'remote-build', '--env', 'B=2', '--no-wait']), { endpoint: 'https://e', token: 't' }, 'reuse')).toEqual([
            '--endpoint', 'https://e',
            '--token', 't',
            '--admin-token', 'at',
            '--mode', 'remote-build',
            '--port', '20042',
            '--replace',
            '--env', 'B=2',
            '--no-wait',
        ]);
        expect(buildDeployArgs(flagsOf(['--no-build', '--artifact', '/tmp/nexus-hub']), { endpoint: 'https://e' }, 'fresh')).toEqual([
            '--endpoint', 'https://e',
            '--mode', 'local',
            '--no-build',
            '--artifact', '/tmp/nexus-hub',
        ]);
    });

    it('teardown carries ONLY endpoint/credentials/--teardown (mode/port/env would imply meaning)', () => {
        const flags = flagsOf(['--teardown', '--endpoint', 'https://e', '--token', 't', '--admin-token', 'a', '--port', '9000', '--env', 'A=1']);
        expect(buildDeployArgs(flags, { endpoint: 'https://e', token: 't' }, 'teardown')).toEqual(['--endpoint', 'https://e', '--token', 't', '--admin-token', 'a', '--teardown']);
        expect(buildDeployArgs(flagsOf(['--teardown', '--endpoint', 'https://e']), { endpoint: 'https://e' }, 'teardown')).toEqual(['--endpoint', 'https://e', '--teardown']);
    });

    it('wraps the argv with the vite-node command, the package-root cwd and the untouched env', () => {
        const invocation = buildPlan(flagsOf([]), ENV, { endpoint: 'https://e' }).invocation;
        // AGENTS.md runner policy: the child runs on vite-node (execPath +
        // node_modules/vite-node/dist/cli.mjs), never plain `node`, and the script is
        // the .ts sibling.
        expect(invocation).toEqual({
            command: process.execPath,
            args: [VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, '--endpoint', 'https://e', '--mode', 'local'],
            cwd: PACKAGE_ROOT,
            env: ENV,
        });
        // Identity, not equality: the child must see the caller's env object UNTOUCHED.
        expect(invocation.env).toBe(ENV);
        expect(invocation.args[0]).toBe(VITE_NODE_CLI);
        expect(invocation.args[1]).toBe(DEPLOY_SCRIPT_PATH);
        expect(invocation.command).not.toBe('node');
    });
});

describe('start-daytona: plans', () => {
    it('fresh plan shows the createSandbox options and a derived hub URL (no minting step)', () => {
        const plan = buildPlan(flagsOf(['--ttl', '30', '--private', '--api-key', 'dtn_key']), ENV);
        expect(plan.mode).toBe('fresh');
        expect(plan.sandboxOptions).toEqual({ apiKey: 'dtn_key', ttlMinutes: 30, idleMinutes: 30, public: false, cpu: 1, mem: 1, disk: 3 });
        expect(plan.invocation.args).toEqual([VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, '--endpoint', '<from createSandbox>', '--mode', 'local']);
        expect(plan.invocation.cwd).toBe(PACKAGE_ROOT);
        expect(plan.hub).toEqual({ path: '/app/nexus/', via: 'endpoint + /app/nexus/ (one runner origin; no per-port preview link)' });
    });

    it('reuse plan prints the given endpoint and the same derived hub path', () => {
        const plan = buildPlan(flagsOf(['--endpoint', 'https://e', '--token', 't']), ENV);
        expect(plan.mode).toBe('reuse');
        expect(plan.sandboxOptions).toBeUndefined();
        expect(plan.invocation.args).toEqual([VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, '--endpoint', 'https://e', '--token', 't', '--mode', 'local']);
        expect(plan.hub).toEqual({ path: '/app/nexus/', via: 'endpoint + /app/nexus/ (one runner origin; no per-port preview link)' });
    });

    it('teardown plan has no hub URL', () => {
        const plan = buildPlan(flagsOf(['--teardown', '--endpoint', 'https://e']), ENV);
        expect(plan.mode).toBe('teardown');
        expect(plan.sandboxOptions).toBeUndefined();
        expect(plan.invocation.args).toEqual([VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, '--endpoint', 'https://e', '--teardown']);
        expect(plan.hub).toBeUndefined();
    });
});

describe('start-daytona: the machine-readable line', () => {
    it('is one single line, carries the derived hubUrl, and derives hubToken from token', () => {
        const line = buildSummary({ mode: 'fresh', endpoint: 'https://e', token: 't', hubUrl: 'https://e/app/nexus/', sandboxId: 'sbx-1234', name: 'nexus-1', public: false, hubPort: 20042 });
        expect(line.includes('\n')).toBe(false);
        expect(line).toBe('{"mode":"fresh","endpoint":"https://e","token":"t","hubUrl":"https://e/app/nexus/","hubToken":"t","sandboxId":"sbx-1234","name":"nexus-1","public":false,"hubPort":20042}');
    });

    it('omits hubPort when the internal port was auto-allocated, and omits empty tokens', () => {
        expect(buildSummary({ mode: 'reuse', endpoint: 'https://e', hubUrl: 'https://e/app/nexus/' })).toBe('{"mode":"reuse","endpoint":"https://e","hubUrl":"https://e/app/nexus/"}');
    });

    it('drops the token keys for a PUBLIC sandbox even though Daytona hands a token back', () => {
        // Daytona's PortPreviewUrl.token is a REQUIRED string (api-client
        // port-preview-url.d.ts:22-24) and public sandboxes get one too — measured on a
        // real create (Temp/daytona-sandbox/last-sandbox.json: public:true + a token).
        // Presence is NOT the "send the header" signal, so a bare public hub must not
        // publish token fields an operator would then paste into a curl that needs none.
        const line = buildSummary({ mode: 'fresh', endpoint: 'https://e', token: 'dtn_preview_token', hubUrl: 'https://e/app/nexus/', sandboxId: 'sbx-1234', name: 'nexus-1', public: true });
        expect(line).toBe('{"mode":"fresh","endpoint":"https://e","hubUrl":"https://e/app/nexus/","sandboxId":"sbx-1234","name":"nexus-1","public":true}');
        expect(line).not.toContain('dtn_preview_token');
    });

    it('keeps the operator token when visibility is unknown (reuse mode has no handle)', () => {
        expect(needsPreviewToken(undefined, 't')).toBe(true);
        expect(needsPreviewToken(false, 't')).toBe(true);
        expect(needsPreviewToken(true, 't')).toBe(false);
        expect(needsPreviewToken(true, undefined)).toBe(false);
        expect(needsPreviewToken(false, '')).toBe(false);
        // The two human labels the flow prints, pinned: reuse cannot claim "public".
        expect(previewLabel(undefined, 't')).toBe(' [x-daytona-preview-token required]');
        expect(previewLabel(undefined, '')).toBe(' [no token]');
        expect(previewLabel(false, 't')).toBe(' [x-daytona-preview-token required]');
        expect(previewLabel(true, 't')).toBe(' [public, no token]');
    });
});

describe('start-daytona: fresh mode', () => {
    it('creates once, spawns once, DERIVES the hub URL (no getPreviewLink), ends on the JSON line', async () => {
        const handle = makeHandle();
        const { createSpy, spawnSpy, lines, problems, invocations, deps } = makeDeps(handle, 0);
        expect(await runStartDaytona(flagsOf(['--ttl', '30', '--private', '--api-key', 'dtn_key', '--env', 'NEXUS_TOKEN=xyz']), deps)).toBe(0);
        expect(problems).toEqual([]);

        // ONE create, with the mapped options plus the progress sink.
        expect(createSpy).toHaveBeenCalledTimes(1);
        expect(createSpy.mock.calls[0]![0]).toEqual({ apiKey: 'dtn_key', ttlMinutes: 30, idleMinutes: 30, public: false, snapshot: undefined, cpu: 1, mem: 1, disk: 3, log: expect.any(Function) });

        // ONE child call: the created endpoint, the forwarded token, env untouched.
        expect(spawnSpy).toHaveBeenCalledTimes(1);
        expect(invocations).toEqual([{ command: process.execPath, args: [VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, '--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token', '--mode', 'local', '--env', 'NEXUS_TOKEN=xyz'], cwd: PACKAGE_ROOT, env: ENV }]);
        expect(invocations[0]!.env).toBe(ENV);

        // v2: the hub URL is endpoint + /app/nexus/, and NOTHING is minted per port.
        expect(handle.sandbox.getPreviewLink).not.toHaveBeenCalled();
        expect(handle.sandbox.delete).not.toHaveBeenCalled();

        // Human lines carry BOTH URLs and the billing note; the LAST line is the contract.
        expect(lines).toEqual([
            '[nexus:start] creating a Daytona sandbox (minutes; --ttl unset = it bills until deleted)…',
            '[nexus:start] ✓ sandbox sbx-1234 (nexus-1) ready — management endpoint https://proxy.example/runner-7777',
            '[nexus:start] ✓ nexus app is live at /app/nexus/',
            '[nexus:start]   hub URL (clients connect here) [x-daytona-preview-token required]: https://proxy.example/runner-7777/app/nexus/',
            '[nexus:start]   management endpoint (control plane: /health /apps /apps/nexus/logs) [x-daytona-preview-token required]: https://proxy.example/runner-7777',
            '[nexus:start]   sandbox sbx-1234 — bills until sandbox.delete() or its 30-minute TTL fires',
            '[nexus:start]   internal app port: auto-allocated by the runner (no --port given)',
            '{"mode":"fresh","endpoint":"https://proxy.example/runner-7777","token":"dtn_preview_token","hubUrl":"https://proxy.example/runner-7777/app/nexus/","hubToken":"dtn_preview_token","sandboxId":"sbx-1234","name":"nexus-1","public":false}',
        ]);
    });

    it('a public sandbox prints an unauthenticated hub URL and drops the token fields', async () => {
        // NOT token: '' — a public sandbox gets a preview token from Daytona too (see the
        // buildSummary test above), so this fake carries one to pin the real shape.
        const handle = makeHandle({ public: true });
        const { deps, lines } = makeDeps(handle, 0);
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(0);
        expect(lines[3]).toBe('[nexus:start]   hub URL (clients connect here) [public, no token]: https://proxy.example/runner-7777/app/nexus/');
        expect(lines[4]).toBe('[nexus:start]   management endpoint (control plane: /health /apps /apps/nexus/logs) [public, no token]: https://proxy.example/runner-7777');
        expect(lines[5]).toBe('[nexus:start]   sandbox sbx-1234 — bills until sandbox.delete() (no --ttl fence was set)');
        expect(lines[7]).toBe('{"mode":"fresh","endpoint":"https://proxy.example/runner-7777","hubUrl":"https://proxy.example/runner-7777/app/nexus/","sandboxId":"sbx-1234","name":"nexus-1","public":true}');
    });

    it('--port is forwarded as the APP port and reported, never as part of the URL', async () => {
        const handle = makeHandle();
        const { deps, lines, invocations } = makeDeps(handle, 0);
        expect(await runStartDaytona(flagsOf(['--port', '20042']), deps)).toBe(0);
        expect(invocations[0]!.args).toEqual([VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, '--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token', '--mode', 'local', '--port', '20042']);
        expect(lines[6]).toBe('[nexus:start]   internal app port: 20042 (explicit; still not public — clients use /app/nexus/)');
        expect(lines[7]).toBe('{"mode":"fresh","endpoint":"https://proxy.example/runner-7777","token":"dtn_preview_token","hubUrl":"https://proxy.example/runner-7777/app/nexus/","hubToken":"dtn_preview_token","sandboxId":"sbx-1234","name":"nexus-1","public":false,"hubPort":20042}');
        expect(handle.sandbox.getPreviewLink).not.toHaveBeenCalled();
    });

    it('forwards --replace and the admin token to the child', async () => {
        const handle = makeHandle();
        const { deps, invocations } = makeDeps(handle, 0);
        expect(await runStartDaytona(flagsOf(['--replace', '--admin-token', 'admin-token']), deps)).toBe(0);
        expect(invocations[0]!.args).toEqual([VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, '--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token', '--admin-token', 'admin-token', '--mode', 'local', '--replace']);
    });

    it('FRESH: applies --admin-token to the new runner AND the deploy child, never printing it', async () => {
        const handle = makeHandle({ adminAuth: 'env' });
        const { deps, createSpy, invocations, lines, problems } = makeDeps(handle, 0);
        expect(await runStartDaytona(flagsOf(['--admin-token', 'admin-token-value']), deps)).toBe(0);
        // Boot half: the shared bring-up receives the token (env form even without
        // SANDBOX_ADMIN_TOKEN in this process env).
        expect(createSpy.mock.calls[0]![0]).toMatchObject({ adminToken: 'admin-token-value' });
        expect(createSpy.mock.calls[0]![0].adminTokenFile).toBeUndefined();
        // Deploy half: the child sends the separate Authorization Bearer.
        expect(invocations[0]!.args[invocations[0]!.args.indexOf('--admin-token') + 1]).toBe('admin-token-value');
        // Presence-only line, and no human/JSON summary carries the value.
        expect(lines.some((line) => line.includes('runner admin auth') && line.includes('env form'))).toBe(true);
        const printed = [...lines, ...problems].join('\n');
        expect(printed).not.toContain('admin-token-value');
        expect(JSON.stringify(JSON.parse(lines[lines.length - 1]!))).not.toContain('admin-token-value');
        expect(JSON.parse(lines[lines.length - 1]!)).not.toHaveProperty('adminToken');
    });

    it('FRESH: the file form boots the runner and reaches the child via ENV, never argv or a log', async () => {
        const filePath = writeAdminTokenFile();
        const handle = makeHandle({ adminAuth: 'file' });
        const { deps, createSpy, invocations, lines } = makeDeps(handle, 0);
        deps.env = { ...ENV, SANDBOX_ADMIN_TOKEN_FILE: filePath };
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(0);
        // Only the PATH reaches createSandbox; the shared bring-up uploads it 0600.
        expect(createSpy.mock.calls[0]![0]).toMatchObject({ adminTokenFile: filePath });
        expect(createSpy.mock.calls[0]![0].adminToken).toBeUndefined();
        // The child gets the same credential through its env, so argv stays clean.
        expect(invocations[0]!.env).toEqual({ ...ENV, SANDBOX_ADMIN_TOKEN_FILE: filePath, NEXUS_SANDBOX_ADMIN_TOKEN: ADMIN_TOKEN });
        expect(invocations[0]!.args).not.toContain(ADMIN_TOKEN);
        const printed = lines.join('\n');
        expect(printed).not.toContain(ADMIN_TOKEN);
        expect(lines.some((line) => line.includes('runner admin auth') && line.includes('file form'))).toBe(true);
    });

    it('FRESH: the file form wins over --admin-token for the boot AND the Bearer', async () => {
        const filePath = writeAdminTokenFile();
        const handle = makeHandle({ adminAuth: 'file' });
        const { deps, createSpy, invocations, lines } = makeDeps(handle, 0);
        deps.env = { ...ENV, SANDBOX_ADMIN_TOKEN_FILE: filePath };
        expect(await runStartDaytona(flagsOf(['--admin-token', 'flag-token-should-lose']), deps)).toBe(0);
        // createSandbox gets both; the shared file-first rule makes the file win.
        expect(createSpy.mock.calls[0]![0]).toMatchObject({ adminToken: 'flag-token-should-lose', adminTokenFile: filePath });
        // The child must send the WINNING (file) token, not the losing flag.
        expect(invocations[0]!.args).not.toContain('flag-token-should-lose');
        expect(invocations[0]!.env.NEXUS_SANDBOX_ADMIN_TOKEN).toBe(ADMIN_TOKEN);
        expect(lines.some((line) => line.includes('file form wins'))).toBe(true);
        expect(lines.join('\n')).not.toContain('flag-token-should-lose');
    });

    it('REUSE: sends the Bearer but warns that the boot credential cannot be changed', async () => {
        const { deps, invocations, lines } = makeDeps(undefined, 0);
        expect(await runStartDaytona(flagsOf(['--endpoint', 'https://e.example', '--admin-token', 'admin-token-value']), deps)).toBe(0);
        // Existing setup behaviour: the child still sends the separate Bearer.
        expect(invocations[0]!.args[invocations[0]!.args.indexOf('--admin-token') + 1]).toBe('admin-token-value');
        // …but the run says plainly that boot-time auth was NOT changed.
        expect(lines.some((line) => line.includes('reuse cannot enable or change') && line.includes('SANDBOX_ADMIN_TOKEN'))).toBe(true);
        const printed = lines.join('\n');
        expect(printed).not.toContain('admin-token-value');
        expect(lines[lines.length - 1]).not.toContain('admin-token-value');
    });

    it('DRY RUN: masks the admin token in the printed plan, and says reuse is Bearer-only', async () => {
        const fresh = makeDeps(undefined, 0);
        expect(await runStartDaytona(flagsOf(['--dry-run', '--admin-token', 'admin-token-value']), fresh.deps)).toBe(0);
        expect(fresh.lines.join('\n')).toContain('"adminToken": "***"');
        expect(fresh.lines.join('\n')).not.toContain('admin-token-value');
        expect(fresh.createSpy).not.toHaveBeenCalled();

        const reuse = makeDeps(undefined, 0);
        expect(await runStartDaytona(flagsOf(['--dry-run', '--endpoint', 'https://e.example', '--admin-token', 'admin-token-value']), reuse.deps)).toBe(0);
        expect(reuse.lines.join('\n')).toContain('reuse does not boot or reboot');
        expect(reuse.lines.join('\n')).not.toContain('admin-token-value');
    });

    it('forwards createSandbox progress through the prefixed sink (a quiet library needs a loud host)', async () => {
        const handle = makeHandle();
        const { deps, lines } = makeDeps(handle, 0);
        let sandboxLog: ((message: string) => void) | undefined;
        deps.createSandbox = vi.fn(async (options) => {
            sandboxLog = options.log as (message: string) => void;
            return handle;
        });
        await runStartDaytona(flagsOf([]), deps);
        sandboxLog!('creating sandbox\nstep 2');
        expect(lines[lines.length - 1]).toBe('[nexus:start]   creating sandbox\n  step 2');
    });

    it('deletes the sandbox it created when the deploy fails, and propagates the exit code', async () => {
        const handle = makeHandle();
        const { deps, lines, problems, invocations } = makeDeps(handle, 3);
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(3);
        expect(invocations).toHaveLength(1);
        expect(handle.sandbox.delete).toHaveBeenCalledTimes(1);
        expect(handle.sandbox.getPreviewLink).not.toHaveBeenCalled();
        expect(problems).toEqual(['[nexus:start] ✗ deploy failed (exit 3) — the deploy output above is the diagnosis']);
        expect(lines[lines.length - 1]).toBe('[nexus:start] ✓ deleted sandbox sbx-1234 (a failed deploy does not get to keep burning)');
    });

    it('--keep-on-fail leaves the sandbox running and says it is still BILLING', async () => {
        const handle = makeHandle();
        const { deps, problems } = makeDeps(handle, 1);
        expect(await runStartDaytona(flagsOf(['--keep-on-fail']), deps)).toBe(1);
        expect(handle.sandbox.delete).not.toHaveBeenCalled();
        expect(problems).toEqual([
            '[nexus:start] ✗ deploy failed (exit 1) — the deploy output above is the diagnosis',
            '[nexus:start] --keep-on-fail: sandbox sbx-1234 is LEFT RUNNING and BILLING — delete it when you are done',
        ]);
    });

    it('a failed delete is reported as a billing leak, and still exits with the deploy code', async () => {
        const handle = makeHandle();
        handle.sandbox.delete = vi.fn(async () => {
            throw new Error('sdk offline');
        });
        const { deps, problems } = makeDeps(handle, 2);
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(2);
        expect(problems).toEqual([
            '[nexus:start] ✗ deploy failed (exit 2) — the deploy output above is the diagnosis',
            '[nexus:start] ✗ could NOT delete sandbox sbx-1234: sdk offline',
            '[nexus:start]   it keeps BILLING until deleted: sandbox.delete() or the @services/daytona `down` verb',
        ]);
    });

    it('--no-wait forwards the flag and warns that the app may still be booting', async () => {
        const handle = makeHandle();
        const { deps, lines, invocations } = makeDeps(handle, 0);
        expect(await runStartDaytona(flagsOf(['--no-wait']), deps)).toBe(0);
        expect(invocations[0]!.args).toEqual([VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, '--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token', '--mode', 'local', '--no-wait']);
        expect(lines[7]).toBe('[nexus:start]   (--no-wait: the deploy was accepted, the app may still be booting — probe the hub URL)');
        expect(lines[8]).toBe('{"mode":"fresh","endpoint":"https://proxy.example/runner-7777","token":"dtn_preview_token","hubUrl":"https://proxy.example/runner-7777/app/nexus/","hubToken":"dtn_preview_token","sandboxId":"sbx-1234","name":"nexus-1","public":false}');
    });
});

describe('start-daytona: reuse mode (never touches sandbox lifecycle)', () => {
    it('creates nothing, spawns against the given endpoint, and DERIVES the same hub URL', async () => {
        const { deps, createSpy, lines, invocations } = makeDeps(undefined, 0);
        expect(await runStartDaytona(flagsOf(['--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token']), deps)).toBe(0);
        expect(createSpy).not.toHaveBeenCalled();
        expect(invocations).toEqual([{ command: process.execPath, args: [VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, '--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token', '--mode', 'local'], cwd: PACKAGE_ROOT, env: ENV }]);
        expect(lines).toEqual([
            '[nexus:start] deploying into the existing endpoint https://proxy.example/runner-7777 (not created here — no sandbox lifecycle actions)',
            '[nexus:start] ✓ deploy ready on https://proxy.example/runner-7777',
            '[nexus:start]   hub URL (clients connect here) [x-daytona-preview-token required]: https://proxy.example/runner-7777/app/nexus/',
            '[nexus:start]   management endpoint (control plane: /health /apps /apps/nexus/logs) [x-daytona-preview-token required]: https://proxy.example/runner-7777',
            '{"mode":"reuse","endpoint":"https://proxy.example/runner-7777","token":"dtn_preview_token","hubUrl":"https://proxy.example/runner-7777/app/nexus/","hubToken":"dtn_preview_token"}',
        ]);
    });

    it('propagates a failing deploy without deleting anything', async () => {
        const handle = makeHandle();
        const { deps, problems } = makeDeps(handle, 4);
        // A handle is lying around on purpose: reuse must still never reach for delete.
        expect(await runStartDaytona(flagsOf(['--endpoint', 'https://e.example', '--keep-on-fail']), deps)).toBe(4);
        expect(handle.sandbox.delete).not.toHaveBeenCalled();
        expect(handle.sandbox.getPreviewLink).not.toHaveBeenCalled();
        expect(problems).toEqual(['[nexus:start] ✗ deploy failed (exit 4) — the deploy output above is the diagnosis; nothing was created, so nothing to clean up']);
    });
});

describe('start-daytona: teardown mode', () => {
    it('spawns endpoint/credentials/--teardown only, derives nothing, deletes no sandbox', async () => {
        const { deps, createSpy, lines, problems, invocations } = makeDeps(undefined, 0);
        expect(await runStartDaytona(flagsOf(['--teardown', '--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token', '--admin-token', 'admin-token']), deps)).toBe(0);
        expect(createSpy).not.toHaveBeenCalled();
        expect(invocations).toEqual([{ command: process.execPath, args: [VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, '--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token', '--admin-token', 'admin-token', '--teardown'], cwd: PACKAGE_ROOT, env: ENV }]);
        expect(lines).toEqual(['[nexus:start] tearing down app "nexus" at https://proxy.example/runner-7777 (DELETE /apps/nexus — the SANDBOX is not deleted)']);
        // No hub URL exists in this mode, so the JSON contract does not apply.
        expect(lines.filter((line) => line.startsWith('{'))).toEqual([]);
        expect(problems).toEqual([]);
    });

    it('reports a failing teardown with the child\'s exit code', async () => {
        const { deps, problems } = makeDeps(undefined, 1);
        expect(await runStartDaytona(flagsOf(['--teardown', '--endpoint', 'https://e.example']), deps)).toBe(1);
        expect(problems).toEqual(['[nexus:start] ✗ teardown failed (exit 1) — see the deploy output above']);
    });
});

describe('start-daytona: dry run touches nothing', () => {
    it('prints the fresh plan and calls neither createSandbox, spawn, nor fetch', async () => {
        // Tripwire: any network use in a dry run would throw AND be counted.
        const fetchSpy = vi.fn(() => {
            throw new Error('dry run must not fetch');
        });
        vi.stubGlobal('fetch', fetchSpy);
        try {
            const { deps, createSpy, lines, invocations } = makeDeps(makeHandle(), 0);
            expect(await runStartDaytona(flagsOf(['--dry-run', '--ttl', '30', '--private', '--api-key', 'dtn_key']), deps)).toBe(0);
            expect(createSpy).not.toHaveBeenCalled();
            expect(invocations).toEqual([]);
            expect(fetchSpy).not.toHaveBeenCalled();
            expect(lines).toEqual([
                '[nexus:start] DRY RUN — no sandbox is created, no script is spawned, no network call',
                '[nexus:start] mode: fresh',
                '[nexus:start] sandbox options that WOULD be used:',
                // apiKey/adminToken are masked; everything else is the real option.
                '  {\n    "apiKey": "***",\n    "ttlMinutes": 30,\n    "idleMinutes": 30,\n    "public": false,\n    "cpu": 1,\n    "mem": 1,\n    "disk": 3\n  }',
                '[nexus:start] (unset knobs keep the @services/daytona defaults, except resources and idle: the sandbox defaults to the measured 1 vCPU / 1 GiB RAM hub footprint on 3 GiB disk (the Daytona default — the runner, the staged bundle/artifacts and the Daytona session log all live there), and 30 idle minutes stop+delete it; --ttl unset means NO wall-clock kill fence)',
                `[nexus:start] deploy command that WOULD run (cwd ${PACKAGE_ROOT}):`,
                `  ${process.execPath} ${VITE_NODE_CLI} ${DEPLOY_SCRIPT_PATH} --endpoint <from createSandbox> --mode local`,
                '[nexus:start] hub URL after success: endpoint + /app/nexus/ (one runner origin; no per-port preview link)',
            ]);
        } finally {
            vi.unstubAllGlobals();
        }
    });

    it('prints the reuse and teardown plans with the given endpoint and no create', async () => {
        const reuse = makeDeps(undefined, 0);
        // reuse keeps --token (D2): it is the ONLY mode where the token means anything.
        expect(await runStartDaytona(flagsOf(['--dry-run', '--endpoint', 'https://proxy.example/runner-7777', '--token', 't']), reuse.deps)).toBe(0);
        expect(reuse.lines).toEqual([
            '[nexus:start] DRY RUN — no sandbox is created, no script is spawned, no network call',
            '[nexus:start] mode: reuse',
            `[nexus:start] deploy command that WOULD run (cwd ${PACKAGE_ROOT}):`,
            `  ${process.execPath} ${VITE_NODE_CLI} ${DEPLOY_SCRIPT_PATH} --endpoint https://proxy.example/runner-7777 --token *** --mode local`,
            '[nexus:start] hub URL after success: endpoint + /app/nexus/ (one runner origin; no per-port preview link)',
        ]);
        expect(reuse.createSpy).not.toHaveBeenCalled();

        const teardown = makeDeps(undefined, 0);
        expect(await runStartDaytona(flagsOf(['--dry-run', '--teardown', '--endpoint', 'https://e.example']), teardown.deps)).toBe(0);
        expect(teardown.lines).toEqual([
            '[nexus:start] DRY RUN — no sandbox is created, no script is spawned, no network call',
            '[nexus:start] mode: teardown',
            `[nexus:start] deploy command that WOULD run (cwd ${PACKAGE_ROOT}):`,
            `  ${process.execPath} ${VITE_NODE_CLI} ${DEPLOY_SCRIPT_PATH} --endpoint https://e.example --teardown`,
        ]);
        expect(teardown.createSpy).not.toHaveBeenCalled();
        expect(teardown.invocations).toEqual([]);
    });
});

describe('start-daytona: main() maps results onto exit codes', () => {
    it('--help prints the usage screen and exits 0 even alongside a bad flag', async () => {
        const { deps, lines } = makeDeps(undefined, 0);
        expect(await main(['--help', '--wat'], deps)).toBe(0);
        expect(lines).toEqual([USAGE]);
        expect(USAGE).toContain('start:daytona');
        expect(USAGE).toContain('--keep-on-fail');
        expect(USAGE).toContain('BILLING');
        // D1: the header name operators are told to send must be the real one.
        expect(USAGE).toContain('x-daytona-preview-token');
        expect(USAGE).toContain('--admin-token');
        expect(USAGE).not.toContain('x-preview-token required');
        // v2: the app path replaces the per-port preview story.
        expect(USAGE).toContain('AUTO-ALLOCATES');
        expect(USAGE).toContain('/app/nexus/');
        expect(USAGE).not.toContain('getPreviewLink');
        // R1: the interrupt contract is operator-facing, so it lives in --help.
        expect(USAGE).toContain('exits 130 for SIGINT, 143 for');
        expect(USAGE).toContain('Reuse, teardown and dry-run install no handlers');
    });

    it('a fresh --token (even with --dry-run) is a parse error naming --token, before any work', async () => {
        const { deps, problems, createSpy, spawnSpy } = makeDeps(makeHandle(), 0);
        expect(await main(['--dry-run', '--token', 't'], deps)).toBe(1);
        expect(problems[0]).toBe('[nexus:start] ✗ --token only applies when you reuse an endpoint — a fresh run authenticates with the token createSandbox returns; drop --token or add --endpoint <url>');
        expect(problems[1]).toBe('');
        expect(problems[2]).toBe(USAGE);
        // Rejected at parse time: a dry run that would have lied never happens.
        expect(createSpy).not.toHaveBeenCalled();
        expect(spawnSpy).not.toHaveBeenCalled();
    });

    it('a bad command line prints every error plus the usage and exits 1', async () => {
        const { deps, problems } = makeDeps(undefined, 0);
        expect(await main(['--wat', '--port', '-1'], deps)).toBe(1);
        expect(problems.slice(0, 3)).toEqual(['[nexus:start] ✗ unknown flag "--wat"', '[nexus:start] ✗ --port "-1" is out of range (TCP port 0-65535 (0 = auto))', '']);
        expect(problems[3]).toBe(USAGE);
    });

    it('a thrown createSandbox error (credentials, bad knobs) becomes exit 1 with its message', async () => {
        const problems: string[] = [];
        const deps: StartDaytonaDeps = {
            createSandbox: vi.fn(async () => {
                throw new Error('DaytonaCredentialError: no Daytona API key found');
            }),
            spawn: vi.fn(async () => 0),
            log: () => undefined,
            error: (line) => problems.push(line),
            env: ENV,
        };
        expect(await main([], deps)).toBe(1);
        expect(problems).toEqual(['[nexus:start] ✗ DaytonaCredentialError: no Daytona API key found']);
    });

    it('--dry-run short-circuits before credentials even exist', async () => {
        const deps: StartDaytonaDeps = {
            createSandbox: vi.fn(async () => {
                throw new Error('must not be reached');
            }),
            spawn: vi.fn(async () => 0),
            log: () => undefined,
            error: () => undefined,
            env: ENV,
        };
        expect(await main(['--dry-run'], deps)).toBe(0);
        expect(deps.createSandbox).not.toHaveBeenCalled();
        expect(deps.spawn).not.toHaveBeenCalled();
    });
});

describe('start-daytona: spawnDeploy — the one real side effect (child driven through the SpawnImpl seam)', () => {
    /** A child invocation identical to what buildDeployInvocation produces for teardown. */
    const invocationOf = (): DeployInvocation => ({
        command: process.execPath,
        args: [VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, '--endpoint', 'https://proxy.example/runner-7777', '--token', 'dtn_preview_token', '--teardown'],
        cwd: PACKAGE_ROOT,
        env: ENV,
    });

    it('argvs the resolved script path from the package root with inherited stdio and the caller\'s env', async () => {
        const invocation = invocationOf();
        const { child } = makeChild();
        const impl = vi.fn(() => child) as unknown as SpawnImpl;
        const run = spawnDeploy(invocation, impl);
        expect(impl).toHaveBeenCalledTimes(1);
        // The whole call is the contract: the runner is vite-node (execPath + the
        // hoisted dist/cli.mjs, never plain node), args[0] = that CLI and args[1] the
        // resolved script, cwd = the package root (cargo's target-dir config), env
        // untouched, output inherited so the child's progress reaches the terminal.
        expect((impl as unknown as ReturnType<typeof vi.fn>).mock.calls[0]).toEqual([process.execPath, invocation.args, { cwd: PACKAGE_ROOT, env: ENV, stdio: 'inherit' }]);
        const [command, args, options] = (impl as unknown as ReturnType<typeof vi.fn>).mock.calls[0] as [string, string[], { cwd: string; env: Record<string, string | undefined>; stdio: string }];
        // Identity, not equality: the argv array and the env object are handed over
        // UNTOUCHED, so the child runs exactly what the dry run printed.
        expect(command).toBe(process.execPath);
        expect(command).not.toBe('node');
        expect(args).toBe(invocation.args);
        expect(args[0]).toBe(VITE_NODE_CLI);
        expect(args[1]).toBe(DEPLOY_SCRIPT_PATH);
        expect(options.cwd).toBe(PACKAGE_ROOT);
        expect(options.env).toBe(ENV);
        expect(options.stdio).toBe('inherit');
        child.emit('close', 0);
        expect(await run).toBe(0);
    });

    it('resolves the child\'s exit code, mapping a signal-killed child (code null) to 1', async () => {
        // Every non-zero child outcome must reach the caller as non-zero, including
        // `code === null`, which is what a signal-killed child reports.
        for (const [childCode, expected] of [[0, 0], [3, 3], [127, 127], [null, 1]] as const) {
            const { child } = makeChild();
            const impl = vi.fn(() => child) as unknown as SpawnImpl;
            const run = spawnDeploy(invocationOf(), impl);
            child.emit('close', childCode);
            expect(await run).toBe(expected);
        }
    });

    it('a spawn-level failure (no vite-node CLI on PATH) resolves 1 and says which script failed to start', async () => {
        const { child } = makeChild();
        const impl = vi.fn(() => child) as unknown as SpawnImpl;
        const errors = vi.spyOn(console, 'error').mockImplementation(() => undefined);
        try {
            const run = spawnDeploy(invocationOf(), impl);
            child.emit('error', new Error('spawn vite-node ENOENT'));
            expect(await run).toBe(1);
            expect(errors.mock.calls.map((call) => call[0])).toEqual(['[nexus:start] ✗ could not start deploy-daytona.ts: spawn vite-node ENOENT']);
        } finally {
            errors.mockRestore();
        }
    });

    it('registers exactly the error and close listeners it reads the outcome from', async () => {
        const { child, listeners } = makeChild();
        const impl = vi.fn(() => child) as unknown as SpawnImpl;
        const run = spawnDeploy(invocationOf(), impl);
        expect(Object.keys(listeners).sort()).toEqual(['close', 'error']);
        // The promise settles ONCE: whichever event arrives first is the answer, and
        // a late error must not overwrite an exit code the child already reported.
        child.emit('close', 7);
        child.emit('error', new Error('late'));
        expect(await run).toBe(7);
    });
});

describe('start-daytona: interrupt policy (decideInterruptCleanup, pure)', () => {
    const created = { sandboxId: 'sbx-1234' };
    it('a fresh run holding a sandbox owes a delete, at 130 for SIGINT', () => {
        expect(decideInterruptCleanup({ mode: 'fresh', created, phase: 'deploying' as InterruptPhase, signal: 'SIGINT' })).toEqual({ shouldDelete: true, exitCode: 130 });
        expect(decideInterruptCleanup({ mode: 'fresh', created, phase: 'deploying' as InterruptPhase, signal: 'SIGTERM' })).toEqual({ shouldDelete: true, exitCode: 143 });
    });

    it('owes nothing while createSandbox has not returned (its own failure path deletes)', () => {
        expect(decideInterruptCleanup({ mode: 'fresh', created, phase: 'creating' as InterruptPhase, signal: 'SIGTERM' })).toEqual({ shouldDelete: false, exitCode: 143 });
    });

    it('owes nothing without a handle, and never in reuse/teardown (not our sandbox)', () => {
        expect(decideInterruptCleanup({ mode: 'fresh', phase: 'deploying' as InterruptPhase, signal: 'SIGINT' })).toEqual({ shouldDelete: false, exitCode: 130 });
        expect(decideInterruptCleanup({ mode: 'reuse', created, phase: 'deploying' as InterruptPhase, signal: 'SIGINT' })).toEqual({ shouldDelete: false, exitCode: 130 });
        expect(decideInterruptCleanup({ mode: 'teardown', created, phase: 'deploying' as InterruptPhase, signal: 'SIGTERM' })).toEqual({ shouldDelete: false, exitCode: 143 });
    });
});

describe('start-daytona: the fresh-mode interrupt fence (Ctrl-C must not leave a billing sandbox)', () => {
    it('SIGINT during the deploy deletes the sandbox, prints the outcome, and exits 130', async () => {
        const handle = makeHandle();
        const { deps, lines, problems, trapped } = makeInterruptDeps(handle, { signal: 'SIGINT' });
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(130);
        // The delete is the whole point of the fence.
        expect(handle.sandbox.delete).toHaveBeenCalledTimes(1);
        // No per-port preview call ever happened, even on the interrupted path.
        expect(handle.sandbox.getPreviewLink).not.toHaveBeenCalled();
        expect(problems).toEqual([]);
        expect(lines).toEqual([
            '[nexus:start] creating a Daytona sandbox (minutes; --ttl unset = it bills until deleted)…',
            '[nexus:start] ✓ sandbox sbx-1234 (nexus-1) ready — management endpoint https://proxy.example/runner-7777',
            '[nexus:start] SIGINT received — deleting sandbox sbx-1234 before exit',
            '[nexus:start] interrupted — sandbox sbx-1234 deleted',
        ]);
        // Handlers come off with the run, so a finished command keeps Node's defaults.
        expect(trapped.size).toBe(0);
    });

    it('SIGTERM during the deploy deletes it too and exits 143', async () => {
        const handle = makeHandle();
        const { deps, lines, problems, trapped } = makeInterruptDeps(handle, { signal: 'SIGTERM' });
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(143);
        expect(handle.sandbox.delete).toHaveBeenCalledTimes(1);
        expect(problems).toEqual([]);
        expect(lines).toEqual([
            '[nexus:start] creating a Daytona sandbox (minutes; --ttl unset = it bills until deleted)…',
            '[nexus:start] ✓ sandbox sbx-1234 (nexus-1) ready — management endpoint https://proxy.example/runner-7777',
            '[nexus:start] SIGTERM received — deleting sandbox sbx-1234 before exit',
            '[nexus:start] interrupted — sandbox sbx-1234 deleted',
        ]);
        expect(trapped.size).toBe(0);
    });

    it('a delete that never comes back is cut off by the bounded wait and reported as a leak', async () => {
        const handle = makeHandle();
        const timeouts: number[] = [];
        const { deps, lines, problems } = makeInterruptDeps(handle, {
            signal: 'SIGINT',
            // A delete that never settles: only the bounded wait can end this run.
            deleteImpl: () => new Promise<never>(() => undefined),
            deleteTimeout: (ms) => {
                timeouts.push(ms);
                return Promise.resolve('timeout');
            },
        });
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(130);
        expect(handle.sandbox.delete).toHaveBeenCalledTimes(1);
        // The cap the operator is promised in --help, asserted rather than assumed.
        expect(timeouts).toEqual([INTERRUPT_DELETE_TIMEOUT_MS]);
        expect(INTERRUPT_DELETE_TIMEOUT_MS).toBe(10_000);
        expect(lines[lines.length - 1]).toBe('[nexus:start] SIGINT received — deleting sandbox sbx-1234 before exit');
        expect(problems).toEqual(['[nexus:start] interrupted — sandbox sbx-1234 DELETE FAILED (bills until manual cleanup)']);
    });

    it('a delete that throws is reported the same way instead of escaping the handler', async () => {
        const handle = makeHandle();
        const { deps, problems } = makeInterruptDeps(handle, {
            signal: 'SIGINT',
            deleteImpl: async () => {
                throw new Error('sdk offline');
            },
        });
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(130);
        expect(problems).toEqual(['[nexus:start] interrupted — sandbox sbx-1234 DELETE FAILED (bills until manual cleanup)']);
    });

    it('reuse, teardown and dry-run arm no signal handlers at all', async () => {
        // Reuse/teardown: that endpoint was never ours, so trapping Ctrl-C there would
        // delete someone else's sandbox.
        const reuse = makeInterruptDeps(makeHandle());
        expect(await runStartDaytona(flagsOf(['--endpoint', 'https://proxy.example/runner-7777', '--token', 't']), reuse.deps)).toBe(0);
        expect(reuse.trapped.size).toBe(0);

        const teardown = makeInterruptDeps(makeHandle());
        expect(await runStartDaytona(flagsOf(['--teardown', '--endpoint', 'https://proxy.example/runner-7777']), teardown.deps)).toBe(0);
        expect(teardown.trapped.size).toBe(0);

        const dryRun = makeInterruptDeps(makeHandle());
        expect(await runStartDaytona(flagsOf(['--dry-run']), dryRun.deps)).toBe(0);
        expect(dryRun.trapped.size).toBe(0);
    });

    it('fresh mode arms exactly SIGINT and SIGTERM, and un-arms them when the run ends', async () => {
        const handle = makeHandle();
        const { deps, trapped } = makeInterruptDeps(handle, { fire: false });
        const seen: StartSignal[] = [];
        const original = deps.onSignal!;
        deps.onSignal = (signal, listener) => {
            seen.push(signal);
            return original(signal, listener);
        };
        expect(await runStartDaytona(flagsOf([]), deps)).toBe(0);
        // Exactly the two documented signals, in that order, and nothing else.
        expect(seen).toEqual(['SIGINT', 'SIGTERM']);
        expect(trapped.size).toBe(0);
        expect(handle.sandbox.delete).not.toHaveBeenCalled();
    });
});

describe('start-daytona: wiring and the import-is-a-no-op guard', () => {
    it('the default deps delegate to the real library call and the real spawner', async () => {
        const deps = createDefaultDeps();
        expect(deps.spawn).toBe(spawnDeploy);
        await deps.createSandbox({ public: true });
        expect(createSandboxMock).toHaveBeenCalledTimes(1);
        expect(createSandboxMock).toHaveBeenCalledWith({ public: true });
    });

    it('isCliEntryPoint: vitest never runs, and only the three entry rules count', () => {
        const module = '/repo/packages/typecrate/nexus/scripts/start-daytona.ts';
        // The vitest guard comes first, whatever else the signals say.
        expect(isCliEntryPoint({ vitest: true, argv: ['/usr/bin/node', module], modulePath: module })).toBe(false);
        // Rule 1: the script positional (node/tsx), and vite-node's --script form.
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', module], modulePath: module })).toBe(true);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node', '--script', 'scripts/start-daytona.ts'], modulePath: module })).toBe(true);
        // Rule 2: the yarn lifecycle line ("vite-node scripts/start-daytona.ts").
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], env: { npm_lifecycle_script: 'vite-node scripts/start-daytona.ts' }, modulePath: module })).toBe(true);
        // Rule 3: vite-node evaluated THIS file as its entry — only runner frames above it.
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], modulePath: module, stack: `Error\n    at Object.<anonymous> (${module}:1:1)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(true);
        // NOT an entry: an imported module has the importer's frame in that window.
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node'], modulePath: module, stack: `Error\n    at Object.<anonymous> (${module}:1:1)\n    at consumer (/repo/packages/agentic/provider/src/consumer.ts:2:2)\n    at ViteNodeRunner.executeFile (runner.mjs:1:1)` })).toBe(false);
        // NOT an entry: a bare specifier, this test file itself, or no signals at all.
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'vite-node', '@services/daytona/sandbox'], modulePath: module })).toBe(false);
        expect(isCliEntryPoint({ argv: ['/usr/bin/node', 'start-daytona.test.ts'], modulePath: module })).toBe(false);
        expect(isCliEntryPoint({})).toBe(false);
    });
});
