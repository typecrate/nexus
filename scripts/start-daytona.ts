// ══════════════════════════════════════════════════════════════════════════════
//  start-daytona.ts — ONE COMMAND THAT BOOTS THE NEXUS APP INSIDE A DAYTONA SANDBOX
//  ("spawn a sandbox, then register the nexus bundle in its app registry")
// ══════════════════════════════════════════════════════════════════════════════
//
//  WHAT THIS DOES (the orchestrator the deploy script was missing):
//    deploy-daytona.ts already speaks the sandbox runtime v2 contract (upload to
//    POST /apps, poll GET /apps/nexus), but it needs an `--endpoint` that already
//    exists. This script supplies one:
//
//        createSandbox()  ──►  spawn deploy-daytona.ts  ──►  hubUrlFor(endpoint)
//        (fresh sandbox,      (cargo build + POST bytes      (endpoint + /app/nexus/,
//         runner on :7777      + poll until ready)            the URL clients use)
//         already health-checked)
//
//    It never talks to Daytona or to the sandbox itself: sandbox bring-up goes
//    through the @services/daytona/sandbox LIBRARY (createSandbox, which already
//    waits for `GET /health`), and every byte of upload/poll/teardown is delegated
//    to the sibling script as a CHILD PROCESS (that script has an entry guard, so
//    importing it would be possible — but spawning keeps its CLI/exit-code
//    contract the one surface this orchestrator depends on).
//
//  ONE ORIGIN, TWO URLS — the distinction this script exists to make obvious:
//    · MANAGEMENT endpoint = createSandbox().endpoint = the runner's preview URL
//      (port 7777 inside the sandbox). It is what `--endpoint` is pointed at, what
//      deploy POSTs to, and what GET /health + GET /apps + GET /apps/nexus +
//      DELETE /apps/nexus answer on. Auth, when the sandbox is private, is the
//      `x-daytona-preview-token` header — the orchestrator just forwards `--token`.
//    · HUB URL = the SAME origin + `/app/nexus/`. v2 has NO per-port preview link
//      (§16.1 P-7/P-8): every app is a path under the one runner origin, so there
//      is nothing to mint and no second token. `hubToken` in the summary is only
//      the compatibility alias of that same runner-origin preview token.
//
//  RUN VIA (package.json scripts; `vite-node` is root-hoisted, house convention):
//      yarn workspace @typecrate/nexus start:daytona                       # fresh sandbox + deploy, waits for ready
//      yarn workspace @typecrate/nexus start:daytona --ttl 60 --env NEXUS_TOKEN=xyz
//      yarn workspace @typecrate/nexus start:daytona --private             # token-authed preview URLs
//      yarn workspace @typecrate/nexus start:daytona --endpoint https://… --token …   # REUSE an endpoint you already own
//      yarn workspace @typecrate/nexus start:daytona --teardown --endpoint https://… # DELETE /apps/nexus (NOT the sandbox)
//      yarn workspace @typecrate/nexus start:daytona --dry-run             # print the plan, touch nothing
//    Extra args after the yarn script name are appended to the command, so every
//    flag below works through yarn too. `--help` is the usage screen.
//
//  MODES (mutually exclusive, derived from the flags — see resolveMode):
//    fresh   (default)      createSandbox → deploy → hub URL. Owns the sandbox.
//    reuse   (--endpoint)   deploy only, against someone else's endpoint.
//    teardown (--teardown)  deploy --teardown only; requires --endpoint.
//
//  LIFECYCLE / BILLING — read this before walking away from a running command:
//    · A fresh sandbox is created PER CALL (createSandbox never reuses anything,
//      setup-sandbox.ts:1411-1415) and KEEPS BILLING until it is deleted or its
//      TTL fires. Nothing here deletes a sandbox on SUCCESS — that is the point
//      (the hub you just paid for is meant to keep serving).
//    · Every sandbox gets an explicit 30-minute IDLE window (SANDBOX_IDLE_MINUTES →
//      createSandbox idleMinutes → Daytona autoStopInterval, delete on stop): 30
//      minutes with no external activity stops AND deletes it, while an actively
//      used hub never expires. This is independent of `--ttl`.
//    · `--ttl <minutes>` is the only wall-clock fence; it defaults to UNSET, i.e.
//      "runs forever" (createSandbox's own docs, setup-sandbox.ts:1557-1563).
//    · `public: true` is the default (tokenless preview URLs). `--private` makes
//      both the management URL and the hub URL require their tokens.
//    · On a FAILED deploy the orchestrator deletes the sandbox it just created
//      (a half-deployed sandbox is pure burn); `--keep-on-fail` opts out so the
//      sandbox can be poked at, and then the sandbox IS still billing.
//    · Reuse/teardown NEVER touch sandbox lifecycle: that endpoint was not ours.
//    · createSandbox itself already deletes only on a FAILED bring-up
//      (setup-sandbox.ts:1560-1563) — no double-delete here.
//    · Ctrl-C while a FRESH run owns the sandbox is NOT a free exit: from the moment
//      createSandbox returns until the run ends, SIGINT/SIGTERM are trapped, the
//      sandbox is deleted best-effort (bounded ~10s wait so a hung SDK call can
//      never turn Ctrl-C into a hang), the outcome is printed, and the process exits
//      130 (SIGINT) / 143 (SIGTERM). Without that fence the freshly-created sandbox
//      bills forever. decideInterruptCleanup() is the pure decision; reuse, teardown
//      and dry-run install NO handlers because they never own a sandbox.
//
//  MACHINE-READABLE OUTPUT: after a successful fresh/reuse start, the LAST line
//  of stdout is exactly ONE single-line JSON object (nothing else is on that line;
//  the example below is wrapped only to fit):
//      {"mode":"fresh","endpoint":"https://…","hubUrl":"https://…/app/nexus/",
//       "sandboxId":"…","name":"…","public":true}
//      (a PRIVATE run inserts "token":"…" and "hubToken":"…" in that same order)
//  `token` (runner-origin preview token) is emitted ONLY when the sandbox is
//  PRIVATE (needsPreviewToken). Daytona hands a token back for EVERY preview link —
//  PortPreviewUrl.token is a required string in its API (api-client
//  src/models/port-preview-url.d.ts:22-24) and a public sandbox gets one too — so
//  presence proves nothing; visibility is the whole rule, exactly how
//  @services/daytona reads it (its curlFor drops the header when public,
//  setup-sandbox.test.ts:606-621). On a PUBLIC sandbox (the default) both fields are
//  absent: the hub URL is bare, and a consumer that sent the token would be sending
//  a credential the proxy never asked for.
//  `hubToken` is a COMPATIBILITY ALIAS, not a second credential: v2 mints no
//  per-port preview link, so the hub URL lives on the same origin as the
//  management endpoint and is authenticated by the SAME preview token. Consumers
//  that read hubToken (services/molab's deploy script) keep working unchanged.
//  REUSE mode emits a concrete hubUrl too (endpoint + /app/nexus/), because the
//  app path needs no SDK handle; it carries no `sandboxId`/`name`/`public`.
//  `hubPort` appears only when --port was explicitly typed: the internal port is
//  auto-allocated by the runner otherwise, and it is never part of a public URL.
//  Automation chains off `hubUrl` (client-facing) and, for a later
//  `--teardown`/`--endpoint` run, off `endpoint` + `token`. Teardown and dry-run
//  print no JSON line.
//
//  IMPORT IS A NO-OP: everything meaningful is an exported pure function over an
//  explicit input (parseArgs / resolveMode / buildSandboxOptions /
//  buildDeployArgs / runStartDaytona), and the only I/O lives in main()'s default
//  deps (real createSandbox + real child_process.spawn). The colocated
//  start-daytona.test.ts drives the whole flow with fakes, with zero network.
//  The auto-run guard at the bottom mirrors the `ENTRY POINT` block in
//  services/daytona/sandbox/setup-sandbox.ts (the same three entry rules, because
//  plain `vite-node <file>` DROPS the file from process.argv — see the rule comments).
// ══════════════════════════════════════════════════════════════════════════════

import { spawn } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
// The curated barrel (index.ts) is the ONLY public surface of the service; it has
// no import side effects, which is what keeps this module importable in tests.
// The two ADMIN_*_ENV names are the shared module's own contract names (§12.3), so
// this script never re-spells a variable the runner/api may rename.
import { ADMIN_TOKEN_ENV, ADMIN_TOKEN_FILE_ENV, createSandbox } from '@services/daytona/sandbox';
import type { CreateSandboxOptions, SandboxLog } from '@services/daytona/sandbox';
// ONE origin, one app path: the hub URL is derived, never minted. See the header.
import { APP_NAME, APP_PATH, hubUrlFor } from './sandbox-contract';

// Re-exported so a consumer (or the test suite) can NAME the shapes this module's
// plan types carry, without reaching past the daytona barrel on its own.
export type { CreateSandboxOptions, SandboxLog };

// ══════════════════════════════════════════════════════════════════════════════
//  PINS — where the sibling script lives, resolved from THIS module (never cwd)
// ══════════════════════════════════════════════════════════════════════════════

/** The zero-dependency sibling this script spawns; it owns the POST/poll contract. */
const DEPLOY_SCRIPT_NAME = 'deploy-daytona.ts';
/**
 * Same candidate-list discipline the daytona library uses for module-relative
 * paths (setup-sandbox.ts:161-195): under this repo's vitest env `import.meta.url`
 * is not always a file:// URL, while vite-node (the real runner) always gives one.
 * Each candidate must actually CONTAIN the deploy script, which is what makes the
 * cwd fallbacks safe from the three possible launch cwds (pkg root / repo root /
 * scripts dir).
 */
const SCRIPTS_DIR_CANDIDATES = (): string[] => {
    const fromImportMeta = (() => {
        try {
            const url = import.meta.url;
            if (typeof url === 'string' && url.startsWith('file://')) return dirname(fileURLToPath(url));
        } catch {
            /* non-file module url under vitest → fall through to the cwd candidates */
        }
        return undefined;
    })();
    return [
        fromImportMeta, //                                   vite-node / node / tsx
        resolve(process.cwd(), 'scripts'), //                cwd = the nexus package root (yarn lifecycle)
        resolve(process.cwd(), 'packages/typecrate/nexus/scripts'), // cwd = repo root
        resolve(process.cwd()), //                           cwd = the scripts dir itself
    ].filter((candidate): candidate is string => Boolean(candidate));
};
/** A candidate is only usable when the sibling script really is sitting next to it. */
const isScriptsDir = (dir: string): boolean => existsSync(join(dir, DEPLOY_SCRIPT_NAME));
/**
 * Resolved ONCE at module load. A non-null deploy script is guaranteed in every
 * real launch context; under vitest the import.meta.url candidate resolves too,
 * so the plan/invocation paths tests assert are the real ones.
 */
const SCRIPTS_DIR = SCRIPTS_DIR_CANDIDATES().find(isScriptsDir) ?? resolve(process.cwd(), 'scripts');
/** Absolute path of the child script — spawn does NOT resolve it against cwd. */
export const DEPLOY_SCRIPT_PATH = join(SCRIPTS_DIR, DEPLOY_SCRIPT_NAME);
/** cargo runs from the package root so the repo-root .cargo/config.toml is found (deploy-daytona.ts:57-60). */
export const PACKAGE_ROOT = resolve(SCRIPTS_DIR, '..');

/**
 * vite-node's own CLI, found by walking up from the package root: this repo hoists it
 * to <root>/node_modules/vite-node, a nested install is covered by the same walk, and
 * a total miss degrades to the bare `vite-node` name on PATH.
 */
const findViteNodeCli = (start: string): string | undefined => {
    for (let dir = start, depth = 0; depth < 8; depth += 1) {
        const candidate = join(dir, 'node_modules', 'vite-node', 'dist', 'cli.mjs');
        if (existsSync(candidate)) return candidate;
        const parent = dirname(dir);
        if (parent === dir) return undefined;
        dir = parent;
    }
    return undefined;
};
/** Exported so the test can name the exact argv[0] the child is spawned with. */
export const VITE_NODE_CLI = findViteNodeCli(PACKAGE_ROOT) ?? 'vite-node';

/**
 * The app's public path (v2 §4.2). Re-exported so the test suite can name the one
 * fact the summary's hubUrl is built from without importing the helper module.
 */
export const HUB_PATH = APP_PATH;
/** Every line this script itself writes is prefixed, so it is greppable next to the child's `>>>`/`✓` lines. */
const PREFIX = '[nexus:start]';
/**
 * dry-run placeholder for an endpoint that only exists after createSandbox. It is
 * deliberately NOT a URL: dry-run never spawns, so it can never reach a fetch call.
 */
const PLACEHOLDER_ENDPOINT = '<from createSandbox>';

/**
 * Masks the value of every `--token`/`--admin-token` pair for the dry-run print.
 * Only the RENDERED argv changes; the spawned invocation keeps the real values.
 */
const redactSecretArgs = (args: readonly string[]): string[] => {
    const out = [...args];
    for (let i = 0; i < out.length - 1; i++) {
        if (out[i] === '--token' || out[i] === '--admin-token') out[i + 1] = '***';
    }
    return out;
};

// ══════════════════════════════════════════════════════════════════════════════
//  USAGE
// ══════════════════════════════════════════════════════════════════════════════

export const USAGE = `usage: yarn workspace @typecrate/nexus start:daytona [flags]
     (or: npx vite-node scripts/start-daytona.ts [flags])

  spawns a Daytona sandbox, hands it to scripts/deploy-daytona.ts, and prints the
  app URL. The app is registered as "${APP_NAME}" in the runner's v2 registry and
  served at ${APP_PATH} on the ONE runner origin. Three modes, derived from the
  flags (never combined):
    fresh     default            createSandbox → deploy → hub URL
    reuse     --endpoint <url>   deploy into an endpoint you already own
    teardown  --teardown         deploy --teardown (needs --endpoint)

  sandbox creation (fresh mode only — illegal together with --endpoint):
    --api-key <key>   Daytona API key (else DAYTONA_API_KEY, else @config/vault)
    --ttl <minutes>   server-side kill fence; DEFAULT = unset = no wall-clock fence
                      (the fixed 30-minute idle reaper still applies)
    --public          tokenless preview URLs (default)
    --private         token-authenticated preview URLs (last of --public/--private wins)
    --snapshot <n>    base snapshot to boot from (default: the service's own)
    --cpu <n>         vCPU, >= 1 (default 1 — the hub measures 7.7 MB RSS / ~0.25 ms per request)
    --mem <gb>        GiB RAM, >= 1 (default 1)
    --disk <gb>       GiB disk, >= 1 (default 3 — Daytona's own default; disk is billed
                      at $0.000108/GiB-h with the first 5 GiB free, and the runner, the
                      staged bundle/artifacts and Daytona's session-command log all live
                      on it. --mode remote-build wants more)

  targeting (reuse / teardown modes ONLY — a fresh run has no endpoint to target):
    --endpoint <url>  the sandbox RUNNER preview URL (management port 7777)
    --token <t>       that endpoint's PREVIEW token, if it is private; the child
                      sends it as the x-daytona-preview-token header on every request.
                      A FRESH run takes its token from createSandbox instead, so
                      --token without --endpoint is an error.
    --admin-token <t> the runner's OPTIONAL admin credential (§12.3), NOT the preview
                      token. In FRESH mode it is applied to the new runner at boot as
                      SANDBOX_ADMIN_TOKEN, and the deploy child sends it as
                      Authorization: Bearer for named deploy/log operations. Set
                      SANDBOX_ADMIN_TOKEN_FILE=<host path> instead for the preferred
                      file form (uploaded 0600; only the path reaches the runner; the
                      value never reaches argv or a log line). REUSE/TEARDOWN can only
                      send the Bearer — the runner's boot-time credential cannot be
                      changed without restarting the sandbox. Env fallbacks for the
                      flag: NEXUS_SANDBOX_ADMIN_TOKEN, then SANDBOX_ADMIN_TOKEN.
                      Never printed.

  deploy passthrough — forwarded verbatim to scripts/deploy-daytona.ts:
    --mode <m>        local (default) | remote-build
    --port <n>        OPTIONAL internal loopback port; omitted = the runner
                      AUTO-ALLOCATES one (v2 default). Never public: clients always
                      use ${APP_PATH}, so there is no preview link per port.
    --replace         PUT /apps/${APP_NAME} (blue-green) instead of POST /apps. Only
                      legal while the app is ready or failed; the current revision
                      keeps serving until the new one is ready.
    --env K=V         extra runtime env var; repeatable
    --no-build        ship an already-built artifact instead of running cargo
    --artifact <path> ship exactly this file (implies --no-build)
    --no-wait         return after the deploy is accepted, without polling
                      GET /apps/${APP_NAME}

  behavior:
    --keep-on-fail    fresh mode: leave the sandbox running when the deploy fails
                      (it keeps BILLING — remember to delete it)
    --dry-run         print the plan; no createSandbox, no spawn, no network
    -h, --help        this text

  signals (fresh mode only):
    Ctrl-C / SIGTERM from the moment the sandbox exists until the run ends DELETES
    that sandbox (best effort, capped at 10s) and exits 130 for SIGINT, 143 for
    SIGTERM. Reuse, teardown and dry-run install no handlers — that sandbox was
    never ours to delete.

  billing: a fresh sandbox is created per call and never deleted on success; only
  the 30-minute idle reaper, --ttl or an explicit delete stops the meter. On deploy
  failure the sandbox IS deleted unless --keep-on-fail. Reuse/teardown never touch a
  sandbox's lifecycle.`;

// ══════════════════════════════════════════════════════════════════════════════
//  FLAGS — pure parser: it returns errors instead of printing or exiting
// ══════════════════════════════════════════════════════════════════════════════

/** Which mode a flag set means. Derived in ONE place so the rules are testable. */
export type StartMode = 'fresh' | 'reuse' | 'teardown';

/** Every flag value after parsing; defaults are the values the underlying tools already use. */
export type StartDaytonaFlags = {
    help: boolean;
    dryRun: boolean;
    teardown: boolean;
    keepOnFail: boolean;
    // ── sandbox creation (fresh) ──
    /** '' = not given → the service falls back to DAYTONA_API_KEY, then @config/vault. */
    apiKey: string;
    /** undefined = NO wall-clock fence (the billing caveat in the header). */
    ttl?: number;
    /** undefined = the service's own default snapshot. */
    snapshot?: string;
    cpu?: number;
    mem?: number;
    disk?: number;
    /** true = tokenless preview URLs (createSandbox's default); --private flips it. */
    public: boolean;
    // ── targeting (reuse / teardown) ──
    /** '' = not given → fresh mode. */
    endpoint: string;
    token: string;
    /** '' = the runner has no admin auth (or the operator did not supply it). */
    adminToken: string;
    // ── deploy passthrough ──
    mode: 'local' | 'remote-build';
    /**
     * The APP's internal loopback port — not the runner's 7777. `undefined` is the
     * v2 default: the runner auto-allocates (§6.2 step 4). Only an explicit --port
     * is forwarded; it never appears in a public URL.
     */
    port?: number;
    /** true = PUT /apps/nexus (blue-green); false/absent = POST /apps (create). */
    replace: boolean;
    env: Record<string, string>;
    noBuild: boolean;
    artifact: string;
    wait: boolean;
};

/** Parsed flags plus the problems found — main() decides how to report them. */
export type ParsedStartArgs = { flags: StartDaytonaFlags; errors: string[] };

/** Numeric flags and the range each accepts — same bounds/wording as the service CLI (setup-sandbox.ts:553-562). */
const NUMERIC_FLAGS: Record<string, { min: number; max?: number; unit: string }> = {
    // 0 is meaningful for ttl ("no fence"), unlike the resource knobs.
    '--ttl': { min: 0, unit: 'minutes, >= 0 (0 = disabled)' },
    '--cpu': { min: 1, unit: 'vCPU, >= 1' },
    '--mem': { min: 1, unit: 'GiB, >= 1' },
    '--disk': { min: 1, unit: 'GiB, >= 1' },
    // The app port: validated HERE so a bad value is a usage error, not a
    // deploy-script death after a multi-minute cargo build. 0 is the explicit
    // spelling of "auto" (§8.3) and lands as `undefined` in the flags.
    '--port': { min: 0, max: 65535, unit: 'TCP port 0-65535 (0 = auto)' },
};

/** Range check with the service's exact message shape (setup-sandbox.ts:575-579). */
const numericError = (label: string, value: unknown, spec: { min: number; max?: number; unit: string }): string | undefined => {
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed < spec.min || (spec.max !== undefined && parsed > spec.max))
        return `${label} ${JSON.stringify(value)} is out of range (${spec.unit})`;
    return undefined;
};

/**
 * Strict flat-flag parser in the deploy script's shape (deploy-daytona.ts:130-190):
 * `--env` repeats and accumulates, a `--`-leading token after a value flag counts
 * as a missing value, unknown flags are errors, and LAST flag wins for the
 * --public/--private pair (the service's own rule, setup-sandbox.ts:581-592).
 * It never throws and never prints — the errors it returns are reported by main().
 */
export const parseArgs = (argv: readonly string[]): ParsedStartArgs => {
    const flags: StartDaytonaFlags = {
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
        // undefined = auto-allocation; the v1 8085 default is deliberately gone.
        port: undefined,
        replace: false,
        env: {},
        noBuild: false,
        artifact: '',
        wait: true,
    };
    const errors: string[] = [];
    // Which creation flags were ACTUALLY typed — needed to name the offenders in the
    // "--endpoint + creation flag" conflict message instead of listing all eight. The
    // set is exactly: --api-key --ttl --public --private --snapshot --cpu --mem --disk,
    // and each of those call sites registers itself here (targeting/deploy flags do not).
    const creationGiven: string[] = [];
    const given = (flag: string): void => {
        if (!creationGiven.includes(flag)) creationGiven.push(flag);
    };
    for (let i = 0; i < argv.length; i++) {
        const flag = argv[i]!;
        // Value flags share one branch so the "needs a value" wording stays identical.
        if (flag === '--api-key' || flag === '--ttl' || flag === '--snapshot' || flag === '--cpu' || flag === '--mem' || flag === '--disk' || flag === '--endpoint' || flag === '--token' || flag === '--admin-token' || flag === '--mode' || flag === '--port' || flag === '--env' || flag === '--artifact') {
            const value = argv[++i];
            if (value === undefined || value.startsWith('--')) {
                errors.push(`${flag} needs a value`);
                continue;
            }
            if (flag === '--endpoint') flags.endpoint = value;
            else if (flag === '--token') flags.token = value;
            else if (flag === '--admin-token') flags.adminToken = value;
            else if (flag === '--api-key') {
                flags.apiKey = value;
                given(flag);
            } else if (flag === '--snapshot') {
                flags.snapshot = value;
                given(flag);
            } else if (flag === '--artifact') flags.artifact = value;
            else if (flag === '--mode') flags.mode = value as StartDaytonaFlags['mode'];
            else if (flag === '--env') {
                // Split on the FIRST '=' so values may contain '=' (base64, K=a=b).
                const eq = value.indexOf('=');
                if (eq <= 0) errors.push(`--env expects KEY=VALUE, got "${value}"`);
                else flags.env[value.slice(0, eq)] = value.slice(eq + 1);
            } else {
                // --ttl / --cpu / --mem / --disk / --port
                const spec = NUMERIC_FLAGS[flag]!;
                const message = numericError(flag, value, spec);
                if (message !== undefined) errors.push(message);
                else if (flag === '--ttl') {
                    flags.ttl = Number(value);
                    given(flag);
                } else if (flag === '--cpu') {
                    flags.cpu = Number(value);
                    given(flag);
                } else if (flag === '--mem') {
                    flags.mem = Number(value);
                    given(flag);
                } else if (flag === '--disk') {
                    flags.disk = Number(value);
                    given(flag);
                } else {
                    // --port 0 is explicit auto (§8.3): it must not reach the child
                    // as a literal 0 in config, and `undefined` is what omits it.
                    const port = Number(value);
                    flags.port = port === 0 ? undefined : port;
                }
            }
            continue;
        }
        if (flag === '--public') {
            flags.public = true;
            given(flag);
        } else if (flag === '--private') {
            flags.public = false;
            given(flag);
        } else if (flag === '--no-build') flags.noBuild = true;
        else if (flag === '--no-wait') flags.wait = false;
        else if (flag === '--teardown') flags.teardown = true;
        else if (flag === '--replace') flags.replace = true;
        else if (flag === '--dry-run') flags.dryRun = true;
        else if (flag === '--keep-on-fail') flags.keepOnFail = true;
        else if (flag === '-h' || flag === '--help') flags.help = true;
        else errors.push(`unknown flag "${flag}"`);
    }
    // --help wins outright: a broken command line still gets the usage screen.
    if (flags.help) return { flags, errors };
    if (flags.mode !== 'local' && flags.mode !== 'remote-build') {
        errors.push(`--mode must be "local" or "remote-build", got "${flags.mode}"`);
    }
    // Mirrors the deploy script's own rule (deploy-daytona.ts:179-184) so the
    // contradiction dies BEFORE a sandbox is paid for.
    if (flags.mode !== 'local' && (flags.noBuild || flags.artifact)) {
        errors.push('--no-build and --artifact apply to --mode local only (remote-build ships a source tarball)');
    }
    // --teardown and --replace are different verbs; accepting both would leave the
    // operator guessing which one won (mirrors deploy-daytona.ts's own rule).
    if (flags.teardown && flags.replace) errors.push('--teardown and --replace are different verbs — pick one');
    // teardown needs an endpoint: a FRESH sandbox has an empty slot, so creating one
    // only to tear it down would burn minutes and delete nothing.
    if (flags.teardown && !flags.endpoint) {
        errors.push('--teardown needs --endpoint <url> — a fresh sandbox has an empty app registry, and this script never tears down a sandbox it did not create');
    }
    // Sandbox-creation knobs + --endpoint contradict each other: an endpoint already
    // names a sandbox, so there is nothing left to configure.
    if (flags.endpoint && creationGiven.length) {
        errors.push(`${creationGiven.join(', ')} only apply when this script creates the sandbox — drop --endpoint (fresh) or drop ${creationGiven.length > 1 ? 'them' : 'it'} (reusing ${flags.endpoint})`);
    }
    // The mirror case (D2): --token is TARGETING, and a fresh run has nothing to
    // target. The real fresh run always authenticates with the token createSandbox
    // returns, so a typed --token would be silently ignored by the run while
    // buildPlan would happily print a child command that can never happen. Making it
    // a parse error keeps --dry-run honest. Gated on resolveMode so `--teardown
    // --token t` (no endpoint) reports only the missing --endpoint.
    if (resolveMode(flags) === 'fresh' && flags.token) {
        errors.push('--token only applies when you reuse an endpoint — a fresh run authenticates with the token createSandbox returns; drop --token or add --endpoint <url>');
    }
    return { flags, errors };
};

/** Mode resolution, the single source of truth for "whose sandbox is this". */
export const resolveMode = (flags: Pick<StartDaytonaFlags, 'teardown' | 'endpoint'>): StartMode => (flags.teardown ? 'teardown' : flags.endpoint ? 'reuse' : 'fresh');

// ══════════════════════════════════════════════════════════════════════════════
//  PLAN BUILDERS — pure data in, data out (asserted exactly by the test suite)
// ══════════════════════════════════════════════════════════════════════════════

/**
 * The deploy child's nexus-specific admin env alias (deploy-daytona.ts parseArgs:
 * `NEXUS_SANDBOX_ADMIN_TOKEN || SANDBOX_ADMIN_TOKEN`). Named here because the
 * shared barrel exports only the generic SANDBOX_ADMIN_TOKEN name.
 */
export const NEXUS_ADMIN_TOKEN_ENV = 'NEXUS_SANDBOX_ADMIN_TOKEN';

/**
 * The env-form token this run would hand the runner at boot: the explicit flag,
 * then the deploy child's nexus-specific alias, then the generic name. Empty when
 * none is configured. Never log this value.
 */
export const adminTokenFrom = (flags: Pick<StartDaytonaFlags, 'adminToken'>, env: Readonly<Record<string, string | undefined>>): string =>
    flags.adminToken || env[NEXUS_ADMIN_TOKEN_ENV] || env[ADMIN_TOKEN_ENV] || '';

/** Which credential (if any) a Bearer request should carry, and where it came from. */
export type AdminBearerSource = 'none' | 'file' | 'flag' | 'env';
export type AdminBearer = { source: AdminBearerSource; token: string; filePath?: string };

/**
 * Resolves ONE effective admin credential for this run, in the SAME precedence the
 * shared bring-up uses (`resolveAdminAuth`): SANDBOX_ADMIN_TOKEN_FILE first, then an
 * explicit token, then the token env vars. Fresh mode passes this to createSandbox
 * (so the new runner boots with it) and to the deploy child (so its named deploy and
 * log reads carry the Bearer). Reuse/teardown can only SEND it — the runner's
 * boot-time credential cannot be changed without restarting/rebooting the sandbox.
 * Errors name the path only; the file's contents never appear in a message or log.
 */
export const resolveAdminBearer = (flags: Pick<StartDaytonaFlags, 'adminToken'>, env: Readonly<Record<string, string | undefined>>): AdminBearer => {
    const filePath = env[ADMIN_TOKEN_FILE_ENV];
    if (filePath) {
        let raw: string;
        try {
            raw = readFileSync(filePath, 'utf8');
        } catch (error) {
            throw new Error(`${ADMIN_TOKEN_FILE_ENV} points at ${filePath}, which cannot be read (${error instanceof Error ? error.message : error}) — fix the path or unset it`);
        }
        const token = raw.trim();
        if (!token) throw new Error(`${ADMIN_TOKEN_FILE_ENV} points at ${filePath}, which is empty — the runner requires an admin token (spec §12.3) and refuses to boot without one`);
        return { source: 'file', token, filePath };
    }
    if (flags.adminToken) return { source: 'flag', token: flags.adminToken };
    const envToken = env[NEXUS_ADMIN_TOKEN_ENV] || env[ADMIN_TOKEN_ENV];
    if (envToken) return { source: 'env', token: envToken };
    return { source: 'none', token: '' };
};

/**
 * Idle window for a sandbox this orchestrator creates, in minutes. createSandbox
 * maps it to Daytona's `autoStopInterval` with `autoDeleteInterval: 0` (stop →
 * delete), so half an hour without external activity reaps an abandoned hub
 * instead of leaving it to the service's 120-minute default.
 */
export const SANDBOX_IDLE_MINUTES = 30;

/**
 * Maps orchestrator flags onto createSandbox's option shape (sandbox/index.ts:8-12).
 * The idle window is always explicit (SANDBOX_IDLE_MINUTES); every other knob is set
 * only when given, so its default stays the service's own — and `port` is
 * deliberately NEVER set: createSandbox's `port` is the RUNNER's port
 * (7777, the management endpoint), while our `--port` is the HUB's port. Passing it
 * through would repoint the management endpoint at the hub and break the deploy.
 */
export const buildSandboxOptions = (flags: StartDaytonaFlags, env: Readonly<Record<string, string | undefined>> = process.env): CreateSandboxOptions => {
    // §12.3 admin auth is applied to the NEW runner at boot. The file env form
    // wins inside createSandbox (resolveAdminAuth: file first, then token), which
    // is why resolveAdminBearer resolves in the SAME order for the Bearer the
    // deploy child sends. Only present knobs are spread, so a run with no admin
    // material keeps the exact option record it had before.
    const adminToken = adminTokenFrom(flags, env);
    return {
        // '' means "not given" (the parser's absent marker) → env/vault fallback.
        apiKey: flags.apiKey || undefined,
        ttlMinutes: flags.ttl,
        // Explicit even though --ttl is optional: an idle sandbox must not sit on the
        // service's 2-hour default. 0 would disable the reaper, so this is always > 0.
        idleMinutes: SANDBOX_IDLE_MINUTES,
        public: flags.public,
        snapshot: flags.snapshot,
        ...(adminToken ? { adminToken } : {}),
        ...(env[ADMIN_TOKEN_FILE_ENV] ? { adminTokenFile: env[ADMIN_TOKEN_FILE_ENV] as string } : {}),
        // cpu/mem stay at the CPU-class FLOOR (1 vCPU / 1 GiB) on purpose — NOT the
        // service's 4/8/10 (setup-sandbox.ts:231, which is what a bare createSandbox would
        // inherit through `undefined`). Measured on the deployed hub 2026-11-12: 7.7 MB RSS,
        // ~0.25 ms CPU per request, dispatch latency 0 ms, so the smallest legal CPU class
        // serves the hub; this org's CPU class caps at 4 vCPU / 8 GiB / 10 GiB anyway
        // (setup-sandbox.ts:117-123). RAM stays there because Daytona enforces `memory` as a
        // HARD limit with Memory == MemorySwap (no swap headroom) AND a larger class is the
        // only thing that raises the bill: Daytona charges reserved class × wall-clock.
        //
        // DISK moved to 3 GiB (from 1) because the old comment sized it from that same
        // 7.7 MB hub measurement, which never measured the disk at all. What actually
        // occupies the sandbox disk is everything BESIDES the hub's RSS: the runner process
        // beside it (measured 67.9 MB baseline, 171–228 MB after real upload churn), the
        // staged bundle plus artifacts under NEXUS_ARTIFACT_ROOT in the runtime dir, and
        // Daytona's own session-command log — ~/.daytona/sessions/<sid>/<cmd>/output.log,
        // unrotated and ON that disk. 3 GiB is Daytona's documented default (min disk class
        // 1, org max 10), and it is effectively free: $0.000108/GiB-h with the first 5 GiB
        // included, and usage never affects the bill — only the reserved class does.
        // Explicit --cpu/--mem/--disk still win, which is what --mode remote-build needs: an
        // in-sandbox cargo build (fat LTO, 149 crates) wants a ~1.9 GiB target dir and more
        // than one core. NOTE: resource knobs apply per-create only on the inline-image
        // path; a create-from-snapshot inherits the SNAPSHOT's baked resources and merely
        // warns on a mismatch (setup-sandbox.ts:979-986). This account hits the fallback
        // (snapshot writes answer 403), so these numbers are what actually lands on CREATE.
        cpu: flags.cpu ?? 1,
        mem: flags.mem ?? 1,
        disk: flags.disk ?? 3,
    };
};

/** The endpoint + token a deploy run is pointed at (fresh fills these in after createSandbox). */
export type DeployTarget = { endpoint: string; token?: string };

/**
 * The exact argv handed to the child. Ordering is fixed and documented so the
 * tests assert the whole array:
 *   --endpoint, --token?, --admin-token?, --mode, --port?, --replace?, --env…,
 *   --no-build?, --artifact?, --no-wait?, --teardown?
 * `--token`/`--admin-token` are OMITTED when empty — the deploy script sends no
 * auth header without them, which is what keeps public endpoints working.
 * `--port` is OMITTED when the operator did not type one: absent is how the child
 * (and the runner) expresses "auto-allocate" (§6.2 step 4).
 * Teardown carries ONLY endpoint/credentials/--teardown: mode, port and env are
 * irrelevant to a DELETE /apps/nexus, and forwarding them would imply they do
 * something.
 */
export const buildDeployArgs = (flags: StartDaytonaFlags, target: DeployTarget, mode: StartMode): string[] => {
    const args = ['--endpoint', target.endpoint];
    if (target.token) args.push('--token', target.token);
    if (flags.adminToken) args.push('--admin-token', flags.adminToken);
    if (mode === 'teardown') {
        args.push('--teardown');
        return args;
    }
    args.push('--mode', flags.mode);
    if (flags.port !== undefined) args.push('--port', String(flags.port));
    if (flags.replace) args.push('--replace');
    // Object insertion order == the order --env was typed (JS string-key guarantee).
    for (const [key, value] of Object.entries(flags.env)) args.push('--env', `${key}=${value}`);
    if (flags.noBuild) args.push('--no-build');
    if (flags.artifact) args.push('--artifact', flags.artifact);
    // The child defaults to waiting; only the deviation has to be spelled out.
    if (!flags.wait) args.push('--no-wait');
    return args;
};

/** A fully-resolved child-process call: what to run, with what, from where. */
export type DeployInvocation = {
    /**
     * The runner: `process.execPath` (the node already running THIS script) execs
     * vite-node's CLI, which then loads the child. AGENTS.md 'Runtime / runner policy'
     * forbids launching anything inside this monorepo with raw `node`, and the child is
     * a .ts file — this Node build reports process.features.typescript === false, so it
     * cannot even load one. `node <vite-node>/dist/cli.mjs` IS vite-node (exactly what
     * node_modules/.bin/vite-node execs, minus the shim) and is the same shape the
     * daytona suite uses to drive vite-node from inside vitest
     * (services/daytona/sandbox/setup-sandbox.test.ts:2109). The bare `vite-node` name
     * is NOT used because PATH only carries node_modules/.bin inside a yarn lifecycle.
     */
    command: string;
    /** Full child argv: args[0] is the vite-node CLI, args[1] DEPLOY_SCRIPT_PATH, then the deploy flags. */
    args: string[];
    /** The nexus package root: cargo's target-dir config is found by walking up from here. */
    cwd: string;
    /** Passed through UNTOUCHED — cargo needs HTTPS_PROXY etc. from the caller's shell. */
    env: Record<string, string | undefined>;
};

/** Wraps buildDeployArgs with the command, the script path, the cwd and the env that make it runnable. */
export const buildDeployInvocation = (flags: StartDaytonaFlags, target: DeployTarget, mode: StartMode, env: Record<string, string | undefined>): DeployInvocation => ({
    command: process.execPath,
    // args[0] is vite-node's CLI and args[1] the script itself, so `spawn(command, args)`
    // runs exactly what the dry run prints, and both paths are resolved from THIS module
    // (never from cwd), so cargo/yarn can be invoked from anywhere.
    args: [VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, ...buildDeployArgs(flags, target, mode)],
    cwd: PACKAGE_ROOT,
    env,
});

/** Everything a dry run needs to show, and what the real run will do with it. */
export type StartPlan = {
    mode: StartMode;
    /** Absent for reuse/teardown — no sandbox is created, so nothing is configured. */
    sandboxOptions?: CreateSandboxOptions;
    invocation: DeployInvocation;
    /** How the hub URL will be obtained (teardown has none). */
    hub?: { path: string; via: string };
};

/**
 * The plan for a flag set. `target` is optional: for a fresh run the endpoint does
 * not exist yet, so the placeholder is printed instead (and never spawned).
 */
export const buildPlan = (flags: StartDaytonaFlags, env: Record<string, string | undefined>, target?: DeployTarget): StartPlan => {
    const mode = resolveMode(flags);
    const resolved: DeployTarget = target ?? { endpoint: mode === 'fresh' ? PLACEHOLDER_ENDPOINT : flags.endpoint, token: flags.token || undefined };
    const plan: StartPlan = {
        mode,
        sandboxOptions: mode === 'fresh' ? buildSandboxOptions(flags, env) : undefined,
        invocation: buildDeployInvocation(flags, resolved, mode, env),
        // v2 has no per-port preview link: the hub URL is the SAME endpoint plus
        // this path, in fresh AND reuse mode. Nothing is minted, so nothing can
        // fail at mint time either.
        hub: mode === 'teardown' ? undefined : { path: HUB_PATH, via: `endpoint + ${HUB_PATH} (one runner origin; no per-port preview link)` },
    };
    return plan;
};

/**
 * THE rule for "does this preview URL need the x-daytona-preview-token header":
 * the sandbox's VISIBILITY, never whether a token string came back.
 *
 * WHY THIS EXISTS: Daytona's PortPreviewUrl declares `token` as a REQUIRED string
 * (api-client src/models/port-preview-url.d.ts:22-24) and populates it for PUBLIC
 * sandboxes too — recorded verbatim on a public sandbox this repo created
 * (Temp/daytona-sandbox/last-sandbox.json: `"public": true` next to a non-empty
 * `"token"`). Inferring "required" from presence therefore labelled every PUBLIC
 * hub URL "[x-daytona-preview-token required]" and sent consumers to attach a
 * credential the proxy never asked for. @services/daytona never makes that
 * inference: it reads sandbox.public and drops the header when public
 * (setup-sandbox.ts:1601-1616, and its curlFor test pins a NON-EMPTY token + public
 * producing NO header — setup-sandbox.test.ts:606-621). `public === undefined`
 * means the caller cannot see visibility — REUSE mode, where the token is whatever
 * the operator typed — so the token is kept there (conservative: an extra valid
 * header is harmless, a missing one is a 401).
 */
export const needsPreviewToken = (publicFlag: boolean | undefined, token: string | undefined): boolean => publicFlag !== true && Boolean(token);

/**
 * The suffix both URL lines carry. `publicFlag === undefined` (REUSE mode) cannot
 * claim "public", so a missing token reads as "[no token]" rather than a promise
 * the script cannot keep.
 */
export const previewLabel = (publicFlag: boolean | undefined, token: string | undefined): string =>
    needsPreviewToken(publicFlag, token) ? ' [x-daytona-preview-token required]' : publicFlag === true ? ' [public, no token]' : ' [no token]';

/**
 * The machine-readable line. Key order is insertion order, and a token key is
 * emitted ONLY when needsPreviewToken says the header is actually required, so a
 * consumer's `token`/`hubToken` check means "send this header" rather than "Daytona
 * happened to hand back a string".
 *
 * `hubToken` is DERIVED from `token`, never passed separately: v2 mints no
 * second per-port credential, and the compatibility name must be structurally
 * incapable of drifting from the credential that actually works.
 *
 * `hubUrl` is always a concrete URL now (fresh and reuse alike), because it is
 * endpoint + `/app/nexus/` — no SDK handle is needed. `hubPort` is emitted only
 * when the operator explicitly chose one; absent means the runner auto-allocated
 * and the port is not public contract anyway.
 */
export const buildSummary = (input: {
    mode: StartMode;
    endpoint: string;
    token?: string;
    hubUrl: string;
    sandboxId?: string;
    name?: string;
    public?: boolean;
    hubPort?: number;
}): string =>
    JSON.stringify({
        mode: input.mode,
        endpoint: input.endpoint,
        ...(needsPreviewToken(input.public, input.token) ? { token: input.token } : {}),
        hubUrl: input.hubUrl,
        ...(needsPreviewToken(input.public, input.token) ? { hubToken: input.token } : {}),
        ...(input.sandboxId ? { sandboxId: input.sandboxId } : {}),
        ...(input.name ? { name: input.name } : {}),
        ...(input.public === undefined ? {} : { public: input.public }),
        ...(input.hubPort === undefined ? {} : { hubPort: input.hubPort }),
    });

/**
 * The dry-run view of createSandbox options: every non-secret knob verbatim, but
 * `adminToken` (the runner's §12.3 credential) and `apiKey` (the Daytona API key)
 * masked. A plan must be pasteable into a terminal/log without leaking either
 * credential; the `adminTokenFile` PATH is kept — it is a location, not a secret.
 */
export const redactSandboxOptions = (options: CreateSandboxOptions): CreateSandboxOptions => ({
    ...options,
    ...(options.adminToken ? { adminToken: '***' } : {}),
    ...(options.apiKey ? { apiKey: '***' } : {}),
});

// ══════════════════════════════════════════════════════════════════════════════
//  INTERRUPT POLICY — pure decision first, then the thin signal wiring that uses it
// ══════════════════════════════════════════════════════════════════════════════

/** The signals trapped while this process owns a BILLING sandbox. */
export type StartSignal = 'SIGINT' | 'SIGTERM';

/**
 * How far a fresh run got — decides whether an interrupt owes a sandbox delete.
 * There is no 'minting' phase in v2: the hub URL is derived from the endpoint, so
 * nothing can fail after a successful deploy except lifecycle work itself.
 */
export type InterruptPhase = 'creating' | 'deploying';

/** What an interrupt is worth: the exit code to use, and whether a delete is owed. */
export type InterruptDecision = {
    /** true = this process holds the only handle to a live sandbox, so it must try to delete it. */
    shouldDelete: boolean;
    /** Shell convention for signal deaths: 128 + signo, so a wrapper can tell them apart. */
    exitCode: number;
};

/** 128 + signal number: SIGINT(2) → 130, SIGTERM(15) → 143. */
const SIGNAL_EXIT_CODES: Record<StartSignal, number> = { SIGINT: 130, SIGTERM: 143 };

/** Hard cap on the interrupt-time delete: Ctrl-C must never trade a sandbox for a hung terminal. */
export const INTERRUPT_DELETE_TIMEOUT_MS = 10_000;

/**
 * THE decision, with no I/O, so it can be pinned exactly. Three ways an interrupt
 * is NOT allowed to delete:
 *   · not fresh mode — reuse/teardown were handed someone else's endpoint;
 *   · no `created` handle — there is nothing to send the DELETE to;
 *   · phase 'creating' — createSandbox has not returned yet, and its own failure
 *     path already deletes on a failed bring-up, so deleting here would double-fire.
 * Anything else (deploying with a handle in hand) is a sandbox this command
 * created seconds ago that would otherwise bill forever.
 */
export const decideInterruptCleanup = (input: {
    mode: StartMode;
    created?: { sandboxId: string };
    phase: InterruptPhase;
    signal: StartSignal;
}): InterruptDecision => ({
    shouldDelete: input.mode === 'fresh' && input.created !== undefined && input.phase !== 'creating',
    exitCode: SIGNAL_EXIT_CODES[input.signal],
});

/** Real bounded wait. The timer is unref'd so it can never be the thing keeping the process alive. */
const timeoutAfter = (ms: number): Promise<'timeout'> =>
    new Promise((resolveTimeout) => {
        const timer = setTimeout(() => resolveTimeout('timeout'), ms);
        if (typeof timer === 'object' && typeof timer.unref === 'function') timer.unref();
    });

/** Real signal wiring; hands back the dispose so the happy path un-traps everything. */
const onProcessSignal = (signal: StartSignal, listener: () => void): (() => void) => {
    process.on(signal, listener);
    return () => process.off(signal, listener);
};

/** A trapped-signal window: a promise that ONLY settles on a signal, plus its teardown. */
type InterruptGuard = {
    /** Resolves with 130/143 when a signal lands; otherwise it just never settles. */
    promise: Promise<number>;
    /** true from the first signal on — the flow stops doing lifecycle work once set. */
    interrupted: () => boolean;
    /** The exit code the signal dictates, or undefined while no signal has landed. */
    code: () => number | undefined;
    /** Removes the listeners (called in a finally, so a successful run leaves the process's default Ctrl-C behavior intact). */
    dispose: () => void;
};

/**
 * The whole signal wiring, deliberately thin: it registers the two handlers, and on
 * a signal it asks decideInterruptCleanup what is owed, pays the delete best effort
 * within INTERRUPT_DELETE_TIMEOUT_MS, prints the outcome, and settles the race with
 * the signal's exit code. Nothing here decides policy by itself.
 */
const armInterruptGuard = (input: {
    mode: StartMode;
    created: SandboxHandle;
    /** Live read of where the flow is, so a signal during create is classified correctly. */
    phase: () => InterruptPhase;
    deps: StartDaytonaDeps;
    log: (line: string) => void;
    error: (line: string) => void;
}): InterruptGuard => {
    const { mode, created, phase, deps, log, error } = input;
    const onSignal = deps.onSignal ?? onProcessSignal;
    const deleteTimeout = deps.deleteTimeout ?? timeoutAfter;
    let interrupted = false;
    let signalCode: number | undefined;
    let settle!: (code: number) => void;
    const promise = new Promise<number>((resolveSignal) => {
        settle = resolveSignal;
    });
    const handle = (signal: StartSignal): void => {
        void (async () => {
            // FIRST signal wins. A second Ctrl-C while the delete is still in flight
            // must not fire a second delete nor change the exit code being reported.
            if (interrupted) return;
            interrupted = true;
            const decision = decideInterruptCleanup({ mode, created, phase: phase(), signal });
            // Recorded BEFORE any await: the flow reads this to make the exit code
            // deterministic even when it finishes the race first.
            signalCode = decision.exitCode;
            log(`${PREFIX} ${signal} received — ${decision.shouldDelete ? `deleting sandbox ${created.sandboxId} before exit` : 'no sandbox of ours to clean up'}`);
            if (decision.shouldDelete) {
                // Racing the delete against a timer covers BOTH failure modes at once:
                // a throwing SDK call and one that simply never returns.
                const deleted = await Promise.race([
                    Promise.resolve()
                        .then(() => created.sandbox.delete())
                        .then(
                            () => true,
                            () => false,
                        ),
                    deleteTimeout(INTERRUPT_DELETE_TIMEOUT_MS).then(() => false),
                ]);
                if (deleted) log(`${PREFIX} interrupted — sandbox ${created.sandboxId} deleted`);
                else error(`${PREFIX} interrupted — sandbox ${created.sandboxId} DELETE FAILED (bills until manual cleanup)`);
            }
            settle(decision.exitCode);
        })();
    };
    const disposers = (['SIGINT', 'SIGTERM'] as const).map((signal) => onSignal(signal, () => handle(signal)));
    return {
        promise,
        interrupted: () => interrupted,
        code: () => signalCode,
        dispose: () => disposers.forEach((dispose) => dispose()),
    };
};

// ══════════════════════════════════════════════════════════════════════════════
//  ORCHESTRATION CORE — all side effects come from the injected deps
// ══════════════════════════════════════════════════════════════════════════════

/**
 * The slice of CreateSandboxResult this script touches, written as a structural
 * type so a test fake can satisfy it without implementing the Daytona SDK class.
 * The real result (setup-sandbox.ts:1456-1483) is assignable to it.
 */
export type SandboxHandle = {
    endpoint: string;
    token?: string;
    sandboxId: string;
    name?: string;
    public?: boolean;
    /** Presence-only form of the runner's boot admin auth (CreateSandboxResult.adminAuth). */
    adminAuth?: 'file' | 'env' | 'none';
    sandbox: {
        delete: () => Promise<unknown>;
        // NO getPreviewLink: v2 exposes one origin and every app as a path under
        // it, so there is no per-port link to mint (§16.1 P-7/P-8). Keeping it out
        // of this structural type is what makes an accidental reintroduction a
        // compile error rather than a silent second-credential bug.
    };
};

/** Injectable side effects — the seams the test suite drives. */
export type StartDaytonaDeps = {
    /** real: @services/daytona/sandbox's createSandbox. */
    createSandbox: (options: CreateSandboxOptions) => Promise<SandboxHandle>;
    /** real: child_process.spawn with stdio inherit; resolves the child's exit code. */
    spawn: (invocation: DeployInvocation) => Promise<number>;
    /** stdout sink. Default console.log. */
    log?: (line: string) => void;
    /** stderr sink (usage, failures, billing warnings). Default console.error. */
    error?: (line: string) => void;
    /** env handed to the child. Default process.env, untouched. */
    env?: Record<string, string | undefined>;
    /**
     * Signal wiring for the fresh-mode interrupt fence. Real: process.on/off, and the
     * returned dispose un-traps. Injected so a test can fire SIGINT mid-deploy.
     */
    onSignal?: (signal: StartSignal, listener: () => void) => () => void;
    /**
     * Bounded wait used by the interrupt delete. Real: an unref'd 10s timer. Injected
     * so the "delete never comes back" branch is testable without waiting or fake clocks.
     */
    deleteTimeout?: (ms: number) => Promise<'timeout'>;
};

/**
 * Runs the whole flow and returns the process exit code (0 = hub is up / plan
 * printed). Never calls process.exit itself, so a caller can wrap it.
 */
export const runStartDaytona = async (flags: StartDaytonaFlags, deps: StartDaytonaDeps): Promise<number> => {
    const log = deps.log ?? ((line: string) => console.log(line));
    const error = deps.error ?? ((line: string) => console.error(line));
    const env = deps.env ?? process.env;
    const mode = resolveMode(flags);
    // §12.3 admin auth has TWO consumers: createSandbox applies it to a fresh
    // runner at boot; the deploy child sends it as Authorization: Bearer for the
    // named POST/PUT/DELETE/log routes. resolveAdminBearer is the one precedence
    // (file form first, matching the shared bring-up) so the two can never
    // disagree. A file-sourced token reaches the child through its ENV — never
    // argv — so it cannot appear in a process listing or a masked-argv print.
    const admin = resolveAdminBearer(flags, env);
    const childEnv: Record<string, string | undefined> = admin.source === 'file' ? { ...env, [NEXUS_ADMIN_TOKEN_ENV]: admin.token } : env;
    // The file form WINS at boot (shared resolveAdminAuth). If --admin-token is also
    // present, leaving it in the child argv would make the child send a DIFFERENT
    // Bearer than the runner was booted with (the flag overrides the env in
    // deploy-daytona's parseArgs). Blank it for the child; the env above carries the
    // winning file token instead — same credential, never argv.
    const childFlags: StartDaytonaFlags = admin.source === 'file' ? { ...flags, adminToken: '' } : flags;

    // ── dry run: plan only, and provably nothing else (no createSandbox, no spawn) ──
    if (flags.dryRun) {
        const plan = buildPlan(flags, env);
        log(`${PREFIX} DRY RUN — no sandbox is created, no script is spawned, no network call`);
        log(`${PREFIX} mode: ${plan.mode}`);
        if (plan.sandboxOptions) {
            log(`${PREFIX} sandbox options that WOULD be used:`);
            // JSON.stringify drops the undefined knobs, so the print is the effective
            // config; redactSandboxOptions masks the API key and the admin token so a
            // pasted plan never leaks either credential.
            log(`  ${JSON.stringify(redactSandboxOptions(plan.sandboxOptions), null, 2).split('\n').join('\n  ')}`);
            log(`${PREFIX} (unset knobs keep the @services/daytona defaults, except resources and idle: the sandbox defaults to the measured 1 vCPU / 1 GiB RAM hub footprint on 3 GiB disk (the Daytona default — the runner, the staged bundle/artifacts and the Daytona session log all live there), and ${SANDBOX_IDLE_MINUTES} idle minutes stop+delete it; --ttl unset means NO wall-clock kill fence)`);
        }
        log(`${PREFIX} deploy command that WOULD run (cwd ${plan.invocation.cwd}):`);
        // Mask the value of every --token/--admin-token pair: a dry run must be
        // pasteable without leaking either credential (the admin bearer especially).
        log(`  ${plan.invocation.command} ${redactSecretArgs(plan.invocation.args).join(' ')}`);
        if (plan.hub) log(`${PREFIX} hub URL after success: ${plan.hub.via}`);
        // Reuse never boots the runner, so it cannot apply a boot-time credential.
        // Say that here, where the operator is looking at the plan.
        if (plan.mode === 'reuse' && admin.source !== 'none')
            log(`${PREFIX} admin auth: the ${admin.source}-form credential is sent as the deploy Bearer ONLY — reuse does not boot or reboot the runner, so its ${ADMIN_TOKEN_ENV}/${ADMIN_TOKEN_FILE_ENV} must already be configured there`);
        return 0;
    }

    // ── teardown: remove the APP; the sandbox itself keeps billing ──
    if (mode === 'teardown') {
        log(`${PREFIX} tearing down app "${APP_NAME}" at ${flags.endpoint} (DELETE /apps/${APP_NAME} — the SANDBOX is not deleted)`);
        const code = await deps.spawn(buildDeployInvocation(childFlags, { endpoint: flags.endpoint, token: flags.token || undefined }, mode, childEnv));
        if (code !== 0) error(`${PREFIX} ✗ teardown failed (exit ${code}) — see the deploy output above`);
        return code;
    }

    // ── reuse: someone else's sandbox, so no lifecycle work ever ──
    if (mode === 'reuse') {
        log(`${PREFIX} deploying into the existing endpoint ${flags.endpoint} (not created here — no sandbox lifecycle actions)`);
        // Reuse cannot change the runner's BOOT credential. The Bearer still goes to
        // the child so an already-configured runner authorizes this deploy; do not
        // imply the --admin-token flag reconfigured anything here.
        if (admin.source !== 'none')
            log(`${PREFIX} ! admin auth: the ${admin.source}-form credential is sent as the deploy Bearer only — reuse cannot enable or change the runner's boot-time ${ADMIN_TOKEN_ENV}/${ADMIN_TOKEN_FILE_ENV}; restart/reboot the sandbox or use fresh mode to apply it`);
        const code = await deps.spawn(buildDeployInvocation(childFlags, { endpoint: flags.endpoint, token: flags.token || undefined }, mode, childEnv));
        if (code === 0) {
            // v2 mints nothing: every app is a path under the one runner origin, so
            // the hub URL is derivable from the endpoint this mode was GIVEN. No SDK
            // handle, no second token, and nothing that can fail after a good deploy.
            const hubUrl = hubUrlFor(flags.endpoint);
            log(`${PREFIX} ✓ deploy ready on ${flags.endpoint}`);
            log(`${PREFIX}   hub URL (clients connect here)${previewLabel(undefined, flags.token)}: ${hubUrl}`);
            log(`${PREFIX}   management endpoint (control plane: /health /apps /apps/${APP_NAME}/logs)${previewLabel(undefined, flags.token)}: ${flags.endpoint}`);
            // LAST line, single-line JSON; `hubToken` is the same runner-origin token.
            log(buildSummary({ mode, endpoint: flags.endpoint, token: flags.token || undefined, hubUrl, hubPort: flags.port }));
        } else error(`${PREFIX} ✗ deploy failed (exit ${code}) — the deploy output above is the diagnosis; nothing was created, so nothing to clean up`);
        return code;
    }

    // ── fresh: create → deploy → derive the hub URL ──
    // createSandbox is QUIET by default, and a bring-up takes minutes, so progress is
    // forwarded through the prefixed sink (multi-line chunks are indented, matching
    // the CLI sink's treatment of raw Daytona build output, setup-sandbox.ts:420-421).
    log(`${PREFIX} creating a Daytona sandbox (minutes; --ttl unset = it bills until deleted)…`);
    // The (message, level) level is deliberately dropped: createSandbox routes the raw
    // Daytona build output through the same callback, so level-based filtering would
    // hide exactly the progress an operator wants while a create takes minutes.
    const sandboxLog: SandboxLog = (message) => log(`${PREFIX}   ${String(message).replace(/\n+/g, '\n  ')}`);
    // The file form wins inside createSandbox (§12.3 resolveAdminAuth), so an
    // explicit --admin-token next to SANDBOX_ADMIN_TOKEN_FILE would otherwise be a
    // silent mismatch: the boot credential is the file's, and the deploy Bearer is
    // resolved from that SAME file by resolveAdminBearer. Say which one won.
    if (admin.source === 'file' && flags.adminToken)
        log(`${PREFIX} ! both --admin-token and ${ADMIN_TOKEN_FILE_ENV} are set — the file form wins for the runner boot (§12.3), and the deploy Bearer is that same file token`);
    const created = await deps.createSandbox({ ...buildSandboxOptions(flags, env), log: sandboxLog });
    log(`${PREFIX} ✓ sandbox ${created.sandboxId}${created.name ? ` (${created.name})` : ''} ready — management endpoint ${created.endpoint}`);
    // Presence-only (never the value): what the new runner was actually booted with.
    // Printed only when admin material exists, so the ordinary unauthenticated run
    // keeps its exact line sequence.
    if (admin.source !== 'none' || (created.adminAuth !== undefined && created.adminAuth !== 'none'))
        log(`${PREFIX}   runner admin auth (${ADMIN_TOKEN_ENV}/${ADMIN_TOKEN_FILE_ENV}): ${created.adminAuth === undefined ? 'not reported' : `${created.adminAuth} form`}`);

    // ── R1 — the interrupt fence (fresh mode ONLY, and only for the window in which
    // this process really owns a BILLING sandbox: created → flow end). Before this,
    // Ctrl-C killed parent+child and left the sandbox running forever with no delete,
    // no summary and no TTL. Handlers come off in the finally below, so a completed
    // run leaves Node's default Ctrl-C behavior intact for whatever runs next.
    const phase: { current: InterruptPhase } = { current: 'deploying' };
    const guard = armInterruptGuard({ mode, created, phase: () => phase.current, deps, log, error });

    // Everything after the create, as ONE unit the guard can abandon mid-flight.
    const finish = async (): Promise<number> => {
        const code = await deps.spawn(buildDeployInvocation(childFlags, { endpoint: created.endpoint, token: created.token }, mode, childEnv));
        // A signal during the deploy means the guard already owns the cleanup; running
        // the deploy-failure delete here too would delete twice and print a lie.
        if (guard.interrupted()) return code;
        if (code !== 0) {
            // The child already printed everything it knows (including /logs on a failed
            // slot), so no re-diagnosis here — just the lifecycle decision.
            error(`${PREFIX} ✗ deploy failed (exit ${code}) — the deploy output above is the diagnosis`);
            if (flags.keepOnFail) {
                error(`${PREFIX} --keep-on-fail: sandbox ${created.sandboxId} is LEFT RUNNING and BILLING — delete it when you are done`);
            } else {
                try {
                    await created.sandbox.delete();
                    log(`${PREFIX} ✓ deleted sandbox ${created.sandboxId} (a failed deploy does not get to keep burning)`);
                } catch (deleteError) {
                    // The one failure mode where money keeps leaking — say it loudly.
                    error(`${PREFIX} ✗ could NOT delete sandbox ${created.sandboxId}: ${describe(deleteError)}`);
                    error(`${PREFIX}   it keeps BILLING until deleted: sandbox.delete() or the @services/daytona \`down\` verb`);
                }
            }
            return code;
        }

        // The deploy SUCCEEDED, so the hub URL is a pure function of the endpoint:
        // endpoint + /app/nexus/. There is no second preview link and no mint
        // failure path — the one credential is the runner-origin preview token.
        const hubUrl = hubUrlFor(created.endpoint);
        const hubToken = created.token;

        // An interrupt that landed while the deploy was finishing still wins: the
        // run is about to delete the sandbox and exit 130/143, so printing a
        // success line and the machine-readable contract would send a consumer to
        // a dead sandbox.
        if (guard.interrupted()) return 0;

        log(`${PREFIX} ✓ nexus app is live at ${APP_PATH}`);
        // One origin, one preview token. Naming a header that does not exist sends
        // operators on a wild goose chase; whether the header is NEEDED is
        // visibility, not token presence (needsPreviewToken): a public sandbox —
        // this script's default — is handed a token by Daytona as well.
        log(`${PREFIX}   hub URL (clients connect here)${previewLabel(created.public, hubToken)}: ${hubUrl}`);
        log(`${PREFIX}   management endpoint (control plane: /health /apps /apps/${APP_NAME}/logs)${previewLabel(created.public, created.token)}: ${created.endpoint}`);
        log(`${PREFIX}   sandbox ${created.sandboxId} — bills until sandbox.delete()${flags.ttl === undefined ? ' (no --ttl fence was set)' : ` or its ${flags.ttl}-minute TTL fires`}`);
        log(`${PREFIX}   internal app port: ${flags.port === undefined ? 'auto-allocated by the runner (no --port given)' : `${flags.port} (explicit; still not public — clients use ${APP_PATH})`}`);
        if (!flags.wait) log(`${PREFIX}   (--no-wait: the deploy was accepted, the app may still be booting — probe the hub URL)`);
        // LAST line, single-line JSON — the machine-readable contract in the header.
        log(
            buildSummary({
                mode,
                endpoint: created.endpoint,
                token: created.token,
                hubUrl,
                sandboxId: created.sandboxId,
                name: created.name,
                public: created.public,
                // Only an EXPLICIT port is reported: absent means auto, and the
                // internal port is never part of the public contract.
                hubPort: flags.port,
            }),
        );
        return 0;
    };

    try {
        // The guard only settles on a signal, so the deploy normally wins the race.
        // The signal's code is then read from the guard rather than taken from the
        // race, because a signal that lands while finish() is unwinding would
        // otherwise lose a coin-flip on microtask ordering — the operator interrupted
        // this run, so 130/143 must win regardless.
        const raced = await Promise.race([finish(), guard.promise]);
        // A signal in flight means the cleanup (and its outcome line) is still
        // running; waiting for it is bounded by the delete timeout and keeps the
        // billing warning from being truncated by the exit.
        if (guard.interrupted()) await guard.promise;
        return guard.code() ?? raced;
    } finally {
        guard.dispose();
    }
};

/** Error → one-line text, so a failure never prints a bare [object Object]. */
const describe = (error: unknown): string => (error instanceof Error ? error.message : String(error));

// ══════════════════════════════════════════════════════════════════════════════
//  REAL DEPS + main()
// ══════════════════════════════════════════════════════════════════════════════

/**
 * The slice of child_process.spawn this module needs, as an injectable seam.
 * WHY A SEAM INSTEAD OF `vi.mock('node:child_process')`: in this repo's vitest setup
 * a builtin mock rebinds the binding for the TEST FILE but leaves the module under
 * test calling the real builtin, so a mocked spawn would never be seen by the code
 * being pinned (measured: the real deploy child really did start, doing a DELETE
 * against the fake endpoint). An explicit default-parameter seam is the only way to
 * exercise the child wiring — argv, cwd, stdio, env identity, exit-code mapping —
 * without starting a cargo build.
 */
export type SpawnChild = {
    on: (event: 'error' | 'close', listener: (arg?: unknown) => void) => unknown;
};

/** Injected spawner: same shape as `child_process.spawn` for the arguments we pass. */
export type SpawnImpl = (command: string, args: readonly string[], options: { cwd: string; env: Record<string, string | undefined>; stdio: 'inherit' }) => SpawnChild;

/**
 * The real child-process dep: stdio INHERIT so the deploy script's `>>>`/`✓`
 * progress and its final AppView JSON reach the terminal untouched, and the child's
 * exit code becomes this command's exit code. A spawn-level failure (no vite-node CLI on
 * PATH) resolves 1 rather than rejecting, so the caller still exits 1 with a line.
 */
export const spawnDeploy = (invocation: DeployInvocation, spawnImpl: SpawnImpl = spawn): Promise<number> =>
    new Promise<number>((resolveRun) => {
        const child = spawnImpl(invocation.command, invocation.args, { cwd: invocation.cwd, env: invocation.env, stdio: 'inherit' });
        child.on('error', (cause) => {
            console.error(`${PREFIX} ✗ could not start ${DEPLOY_SCRIPT_NAME}: ${describe(cause)}`);
            resolveRun(1);
        });
        // Signal-killed children report code null (and a shim may report undefined);
        // anything that is not a number becomes 1 so "non-zero = failed" always holds.
        child.on('close', (code) => resolveRun(typeof code === 'number' ? code : 1));
    });

/** The default dep set: the real library call and the real child process. */
export const createDefaultDeps = (): StartDaytonaDeps => ({
    createSandbox: (options) => createSandbox(options),
    spawn: spawnDeploy,
});

/**
 * Parses, validates, runs, and maps the result onto an exit code. Returns it
 * instead of calling process.exit so the flow stays testable (the entry guard at
 * the bottom is the only place that touches process state).
 */
export const main = async (argv: readonly string[] = process.argv.slice(2), deps: StartDaytonaDeps = createDefaultDeps()): Promise<number> => {
    const log = deps.log ?? ((line: string) => console.log(line));
    const error = deps.error ?? ((line: string) => console.error(line));
    const { flags, errors } = parseArgs(argv);
    if (flags.help) {
        log(USAGE);
        return 0;
    }
    if (errors.length) {
        for (const problem of errors) error(`${PREFIX} ✗ ${problem}`);
        error('');
        error(USAGE);
        return 1;
    }
    try {
        return await runStartDaytona(flags, deps);
    } catch (failure) {
        // createSandbox throws DaytonaCredentialError / CreateSandboxOptionsError by
        // name; both are "your command line / your credentials", so say what to fix.
        error(`${PREFIX} ✗ ${describe(failure)}`);
        return 1;
    }
};

// ══════════════════════════════════════════════════════════════════════════════
//  ENTRY POINT — importing this module must never start anything
// ══════════════════════════════════════════════════════════════════════════════
//
//  Copied in shape from the `ENTRY POINT` block of services/daytona/sandbox/
//  setup-sandbox.ts, because the same constraint applies here: `yarn start:daytona`
//  must auto-run, `vitest` importing this file must not. Rule 3 exists because a
//  PLAIN `vite-node <file>` rewrites process.argv and DROPS the file (measured in
//  that file's own entry-point comment), so neither argv nor the lifecycle line can
//  see the entry. A stack probe is only evidence inside vite-node's module runner,
//  which is exactly where that gap is.

/** Extension-bearing token that identifies THIS file as the runnable script. */
const ENTRY_SCRIPT_PATTERN = /(?:^|[/\\])start-daytona\.[cm]?[jt]sx?$/;
/** vite/vite-node append module ids and query strings; strip them before matching. */
const stripModuleQuery = (token: string): string => String(token).split('?')[0].split('#')[0];
const isScriptToken = (token: unknown): boolean => ENTRY_SCRIPT_PATTERN.test(stripModuleQuery(String(token)));

/** Signals the entry detector reads — every field is process state, gathered once at import. */
export type EntryPointSignals = {
    argv?: readonly string[];
    env?: Readonly<Record<string, string | undefined>>;
    modulePath?: string;
    /** Only meaningful while this module is still being evaluated (rule 3). */
    stack?: string;
    /** true under vitest, where nothing may run. */
    vitest?: boolean;
};

/** Rule 1: argv's SCRIPT POSITIONAL is this file (`node|tsx … start-daytona.ts`, `--script` form). */
const namesScriptPositional = (argv: readonly string[] | undefined): boolean => {
    const tokens = argv ?? [];
    if (isScriptToken(tokens[1])) return true;
    const flag = tokens.indexOf('--script');
    return flag !== -1 && isScriptToken(tokens[flag + 1]);
};

/** Rule 2: the npm/yarn lifecycle line names it (`start:daytona` = "vite-node scripts/start-daytona.ts"). */
const namesLifecycleEntryPoint = (env: Readonly<Record<string, string | undefined>> | undefined): boolean =>
    String(env?.npm_lifecycle_script ?? '')
        .split(/\s+/)
        .some(isScriptToken);

/** Rule 3: vite-node evaluated THIS file as its entry — only runner frames sit above it. */
const isRunnerEntryPoint = (stack: string | undefined, modulePath: string | undefined): boolean => {
    if (!stack || !modulePath) return false;
    const frames = stack.split('\n').map((frame) => frame.trim().replace('file://', ''));
    if (!frames.some((frame) => frame.includes('ViteNodeRunner'))) return false;
    const own = frames.reduce((found, frame, index) => (frame.includes(modulePath) ? index : found), -1);
    if (own < 0) return false;
    const callers = frames.slice(own + 1);
    const executeFile = callers.findIndex((frame) => frame.includes('ViteNodeRunner.executeFile'));
    const upTo = executeFile === -1 ? callers.length : executeFile;
    return callers.slice(0, upTo).every((frame) => frame.includes('ViteNodeRunner'));
};

/** True when — and only when — this module is the process entry point. */
export const isCliEntryPoint = (signals: EntryPointSignals): boolean => {
    if (signals.vitest) return false;
    if (namesScriptPositional(signals.argv)) return true;
    if (namesLifecycleEntryPoint(signals.env)) return true;
    return isRunnerEntryPoint(signals.stack, signals.modulePath);
};

/** This file's own path (rule 3 needs to identify its own stack frames). */
const MODULE_FILE_PATH = (() => {
    try {
        return fileURLToPath(import.meta.url);
    } catch {
        // vitest may hand a non-file import.meta.url (see SCRIPTS_DIR_CANDIDATES).
        return join(SCRIPTS_DIR, 'start-daytona.ts');
    }
})();

// The stack must be captured during module evaluation; inside main() the evidence
// for rule 3 is already gone.
if (isCliEntryPoint({ argv: process.argv, env: process.env, modulePath: MODULE_FILE_PATH, stack: new Error().stack, vitest: Boolean(process.env.VITEST) })) {
    main().then(
        (code) => {
            process.exitCode = code;
        },
        (failure) => {
            console.error(`${PREFIX} ✗ unexpected failure: ${failure instanceof Error ? (failure.stack ?? failure.message) : String(failure)}`);
            process.exitCode = 1;
        },
    );
}
