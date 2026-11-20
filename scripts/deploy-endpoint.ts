// ══════════════════════════════════════════════════════════════════════════════
//  deploy-endpoint.ts — DEPLOY THE NEXUS APP TO ANY SANDBOX RUNTIME V2 ENDPOINT
//  YOU HAND IT, REPLACING THE `nexus` APP IN PLACE (blue-green) WHEN IT IS THERE
//
//      yarn workspace @typecrate/nexus deploy:endpoint https://<your-endpoint>
// ══════════════════════════════════════════════════════════════════════════════
//
//  WHAT THIS DOES:
//
//      GET /health  → v2 gate → GET /apps/nexus → [app absent: POST /apps
//                    (apiVersion>=2     (the registry          app ready|failed: PUT /apps/nexus
//                     or capabilities     record, NEVER        app queued/building/…: DELETE /apps/nexus
//                     [apps])             the v1 /status)        then POST /apps
//                                                        → spawn deploy-daytona.ts
//                                                          (cargo build + upload + poll /apps/nexus)
//
//  IT IS PLATFORM-AGNOSTIC ON PURPOSE. The only thing it depends on is the
//  sandbox runtime v2 contract (services/daytona/sandbox-specs.md) — a Daytona
//  runner preview URL, a Modal box URL, or the stdlib runner on loopback. So the
//  URL is an ARGUMENT, not a constant, and nothing here CREATES a platform object.
//
//  WHY IT EXISTS ALONGSIDE deploy-daytona.ts (which it drives, as a child):
//    deploy-daytona.ts registers the app and deliberately refuses to resolve an
//    existing one: a 409 app_exists prints "--replace or --teardown" and exits 1,
//    because silently killing a hub someone else is using is worse than one extra
//    flag. That is right when the endpoint is a sandbox you just created
//    (start-daytona.ts's fresh mode: the registry is empty by construction) and
//    wrong when you point at a long-lived endpoint you own, where "yesterday's
//    nexus is still registered" is simply the normal case. This script is that
//    other posture: replace-on-arrival, by default, with `--no-teardown` to opt
//    back into the strict behaviour.
//
//      fresh sandbox  →  start:daytona   (creates the sandbox; registry is empty)
//      any endpoint   →  deploy:endpoint (v2 gate, app-scoped replace, deploys)  ← HERE
//      ship only      →  deploy:daytona  (POST/PUT + poll; an existing app is an error)
//      stop it        →  deploy:endpoint <url> --teardown   (DELETE /apps/nexus)
//
//  REPLACEMENT IS APP-SCOPED. The registry is a multi-app one, so routine
//  replacement of nexus is ALWAYS `PUT /apps/nexus` (blue-green, §6.3): the old
//  revision keeps serving until the new one is ready, and no peer app is touched.
//  A `queued`/`building`/`booting`/`draining` app cannot be PUT (§6.2 step 3), so
//  the replace posture deletes THAT APP and re-creates it — again app-scoped.
//  `--no-teardown` turns any registered app into a conflict error with the fix.
//
//  MODAL CONTAINER RESET IS OPT-IN AND DESTRUCTIVE ACROSS APPS. `DELETE
//  /_provider/sandbox` ends the runner and the container, which discards EVERY
//  app on that box, not just nexus. It is therefore NEVER used as ordinary
//  conflict resolution and never implied by the replace posture: only an explicit
//  `--recycle-container` sends it, and only when GET /health's `service` marker
//  plus a `*.modal.run` host prove the endpoint is a Modal box. A Modal SANDBOX
//  tunnel (`*.modal.host`) is refused for the reset, because its URL dies with
//  the container and there is no stable address to relaunch into.
//
//  AUTH: two independent credentials, both optional.
//    · `--token` is the Daytona PREVIEW token, sent as `x-daytona-preview-token`
//      on every request — the header a private preview proxy requires.
//    · `--admin-token` is the runner's §12.3 admin bearer, sent as
//      `Authorization: Bearer` so mutations and log reads work when the runner has
//      SANDBOX_ADMIN_TOKEN set. It is NEVER printed. The child receives both via
//      its own flags/env, so its mutations carry them too.
//    An endpoint with no secret simply gets no header, which keeps public
//    endpoints and the loopback stub working.
//
//  THE HUB'S URL IS COMPUTED, NOT MINTED. v2 has ONE origin and every app lives
//  under `/app/<name>/` (§4.1/§4.2, P-7/P-8): the hub URL is
//  `endpoint + AppView.path` (with `AppView.publicUrl` as the fallback when a
//  body carries no path). There is no per-port preview link to read or guess.
//
//  WHY THE PREFLIGHT WAITS (not a Daytona/Modal special case — just cold starts):
//    a freshly created or recently recycled container answers 502/503 or drops the
//    connection until its runner binds the port, so a miss is EXPECTED and is
//    retried for ENDPOINT_WAIT_MS. Three failures are distinguished from it
//    because they must not be retried: 401/403 (a token problem), 404 on /health /
//    a non-v2 body (wrong URL, or a v1 single-slot runner), and a JSON body that
//    is not an AppView on /apps/nexus (a different service on that URL).
//
//  RUN VIA (package.json; `vite-node` is root-hoisted, house convention):
//      yarn workspace @typecrate/nexus start:modal
//      # ^ the vault's Modal box (@config/vault → Object.values(modalContainer)[0])
//      yarn workspace @typecrate/nexus deploy:endpoint https://<modal-box-url>
//      yarn workspace @typecrate/nexus deploy:endpoint https://abc123.modal.host --token s3cret
//      # ^ a Modal SANDBOX tunnel: the app is deployed as supplied; --recycle-container
//      #   is refused because the tunnel URL dies with its container (--teardown frees nexus)
//      yarn workspace @typecrate/nexus deploy:endpoint https://<sandbox-preview-url> --env NEXUS_TOKEN=xyz
//      yarn workspace @typecrate/nexus deploy:endpoint <url> --mode remote-build
//      yarn workspace @typecrate/nexus deploy:endpoint <url> --no-teardown   # existing app = error
//      yarn workspace @typecrate/nexus deploy:endpoint <url> --recycle-container  # Modal: discard EVERY app
//      yarn workspace @typecrate/nexus deploy:endpoint <url> --teardown      # DELETE /apps/nexus
//      yarn workspace @typecrate/nexus deploy:endpoint <url> --dry-run       # the plan, zero network
//    Flags after the URL are the deploy passthrough; every one of them is
//    deploy-daytona.ts's own spelling, so nothing here invents a flag name.
//
//  MACHINE-READABLE OUTPUT: after a successful run the LAST line of stdout is
//  exactly ONE single-line JSON object (nothing else is on that line; wrapped
//  here to fit):
//      {"mode":"deploy","endpoint":"https://…","app":"nexus",
//       "hubUrl":"https://…/app/nexus/","path":"/app/nexus/","status":"ready",
//       "revision":1,"internalPort":20042}
//    plus `"token":"…","tokenScope":"management"` when a preview token was used
//    (it authenticates the RUNNER origin; the admin Bearer is never printed),
//    plus `"replaced":{…}` when this run replaced/deleted a previous revision,
//    plus `"recycled":"box"` when --recycle-container replaced the Modal container.
//    `--teardown` emits
//    {"mode":"teardown","endpoint":"…","app":"nexus","path":"/app/nexus/","removed":true}.
//    A dry run emits none.
//
//  IMPORT IS A NO-OP: everything meaningful is an exported pure function over an
//  explicit input (parseArgs / classifyRunnerHealth / classifyAppProbe /
//  buildHubUrl / buildDeployArgs / buildSummary), and ALL I/O is in deps the
//  caller injects (probe / spawn / sleep / now / log / error / env). The entry
//  guard at the bottom is the only thing that touches process state, and it never
//  fires under vitest. The colocated deploy-endpoint.test.ts drives the whole flow
//  with fakes — zero network, no cargo.
// ══════════════════════════════════════════════════════════════════════════════

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
    APP_LOGS_ROUTE,
    APP_MOUNT,
    APP_NAME,
    APP_PATH,
    APP_ROUTE,
    APPS_ROUTE,
    appUrl,
    describeAppView,
    describeHealth,
    isAppReplaceable,
    isV2Runner,
    joinUrl,
    maskSecret,
    parseAppView,
    parseHealth,
    requestHeaders,
    resolveAppUrl,
} from './sandbox-contract';
import type { AppView, RunnerHealth } from './sandbox-contract';

// ══════════════════════════════════════════════════════════════════════════════
//  PINS — resolved from THIS module (never from cwd), as start-daytona.ts does
// ══════════════════════════════════════════════════════════════════════════════

/** The zero-dependency sibling this script spawns; it owns the upload/poll contract. */
const DEPLOY_SCRIPT_NAME = 'deploy-daytona.ts';
/** This file's own name, for the entry guard's fallback path below. */
const ENTRY_SCRIPT_NAME = 'deploy-endpoint.ts';
/**
 * `import.meta.url` is not always a file:// URL under this repo's vitest env, while
 * vite-node (the real runner) always gives one — so each candidate is accepted only
 * when the sibling script is ACTUALLY in it, which is what makes the cwd fallbacks
 * safe across the possible launch cwds (package root / repo root / scripts dir).
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
    return [fromImportMeta, resolve(process.cwd(), 'scripts'), process.cwd()].filter((candidate): candidate is string => Boolean(candidate));
};
const isScriptsDir = (dir: string): boolean => existsSync(join(dir, DEPLOY_SCRIPT_NAME));
const SCRIPTS_DIR = SCRIPTS_DIR_CANDIDATES().find(isScriptsDir) ?? resolve(process.cwd(), 'scripts');
/** Absolute path of the child script — spawn does NOT resolve it against cwd. */
export const DEPLOY_SCRIPT_PATH = join(SCRIPTS_DIR, DEPLOY_SCRIPT_NAME);
/** cargo runs from the package root so the repo-root .cargo/config.toml is found. */
export const PACKAGE_ROOT = resolve(SCRIPTS_DIR, '..');

/**
 * vite-node's own CLI, found by walking up from the package root (root-hoisted here;
 * a nested install is covered by the same walk), degrading to the bare name on PATH.
 * The child is a .ts file, and raw `node` is both forbidden by AGENTS.md and unable
 * to load one in this Node build — so it is started as `process.execPath <vite-node
 * cli.mjs> <script>`, the exact shape start-daytona.ts uses for the same reason.
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
export const VITE_NODE_CLI = findViteNodeCli(PACKAGE_ROOT) ?? 'vite-node';

// ══════════════════════════════════════════════════════════════════════════════
//  NUMBERS
// ══════════════════════════════════════════════════════════════════════════════

/** Every line this script writes is prefixed, so it greps apart from the child's `>>>`/`✓`. */
export const PREFIX = '[nexus:endpoint]';
/**
 * The Modal runner's own /health marker. The Daytona twin reports no `service`
 * field, so this is what separates "a Modal box whose CONTAINER we may recycle"
 * from "someone else's endpoint we must not touch" (see the reset note above).
 * It is never a reason to reset by itself: only --recycle-container acts on it.
 */
const MODAL_RUNNER_SERVICE = 'modal-container-runtime';
/** Modal's two hostname shapes: a box URL is a stable deployment, a sandbox tunnel is not. */
const MODAL_BOX_HOST = /(?:^|\.)modal\.run$/i;
const MODAL_SANDBOX_HOST = /(?:^|\.)modal\.host$/i;
/** §4.5 R-4.5: provider-only control routes live under `/_provider/`. */
const PROVIDER_RESET_ROUTE = '_provider/sandbox';
/**
 * How long to wait for an endpoint to answer AT ALL. A container that is new, recycled
 * or scaled to zero answers 502/503 (or drops the connection) until its runner binds
 * the port, so the first misses are expected, not failures. Generous enough for a cold
 * start, short enough that a dead URL is not a 15-minute mystery.
 */
export const ENDPOINT_WAIT_MS = 90_000;
/** How long to wait for DELETE /apps/nexus to be reflected in the registry. Teardown is synchronous per contract, so this is a proof fence, not a budget. */
export const SLOT_WAIT_MS = 30_000;
/**
 * How long a recycled Modal box gets to become a fresh, empty container: DELETE
 * /_provider/sandbox ends the runner, Modal then relaunches the container and the
 * replacement binds its runner. A proof fence for the swap, not a cold-start budget.
 */
export const RECYCLE_WAIT_MS = 60_000;
/**
 * How much clock skew to forgive when a replacement's own `uptimeSec` is compared with
 * the time elapsed since DELETE /_provider/sandbox. The two clocks are independent
 * (this process and Modal's container), and the decision the margin feeds is only
 * "did this process start after the reset" — a process that predates it reports an
 * uptime older than the elapsed time by its whole earlier life, which is far more
 * than this window.
 */
const FRESH_UPTIME_SLACK_MS = 15_000;
/** Poll cadence and per-request timeout — both mirror deploy-daytona.ts's own numbers. */
const POLL_MS = 2_000;
const REQUEST_TIMEOUT_MS = 15_000;
/**
 * DELETE /_provider/sandbox is heavier than DELETE /apps/nexus: the runner tears the
 * app down FIRST (stopSignal → grace → SIGKILL → log drain → rmtree) and only then
 * answers, so an ordinary request ceiling would report a transport timeout on a reset
 * that is proceeding exactly as designed.
 */
const RESET_TIMEOUT_MS = 60_000;

/** `NEXUS_ENDPOINT` is the platform-neutral name start-modal.ts forwards; kept exported for it. */
export const ENDPOINT_ENV = 'NEXUS_ENDPOINT';

// ══════════════════════════════════════════════════════════════════════════════
//  USAGE
// ══════════════════════════════════════════════════════════════════════════════

export const USAGE = `usage: yarn workspace @typecrate/nexus deploy:endpoint <url> [flags]
       (or: vite-node scripts/deploy-endpoint.ts <url> [flags])

  deploys the nexus bundle into ANY endpoint that speaks the sandbox runtime V2
  contract — a Daytona runner preview URL, a Modal box or sandbox URL, a local
  runner — as the app "${APP_NAME}", served at ${APP_PATH} on that one origin.
  An existing app is replaced APP-SCOPED (PUT /apps/${APP_NAME}); peers are untouched.

  the endpoint (required; no platform default, that is the point):
    <url>                    positional, or --endpoint <url>
                             (env fallbacks: NEXUS_ENDPOINT, NEXUS_DAYTONA_ENDPOINT)
    --token <t>              Daytona PREVIEW token, sent as x-daytona-preview-token
                             on /health, /apps, PUT/DELETE and the child's upload.
                             (env: NEXUS_ENDPOINT_TOKEN, NEXUS_DAYTONA_PREVIEW_TOKEN)
    --admin-token <t>        the runner's §12.3 ADMIN bearer, sent as
                             Authorization: Bearer alongside the preview header so
                             mutations and log reads work when SANDBOX_ADMIN_TOKEN is
                             set. Never printed. (env: NEXUS_SANDBOX_ADMIN_TOKEN,
                             then SANDBOX_ADMIN_TOKEN)

  deploy (forwarded verbatim to scripts/deploy-daytona.ts):
    --mode <m>        local (default: compile here, ship the nexus-hub ELF)
                      remote-build (ship the source tarball; cargo runs there)
    --port <n>        OPTIONAL internal loopback port; omitted = the runner
                      AUTO-ALLOCATES one (v2 default). Never public: clients use
                      ${APP_PATH}
    --env K=V         extra runtime env var; repeatable
    --no-build        ship an already-built artifact instead of running cargo
    --artifact <path> ship exactly this file (implies --no-build)
    --no-wait         return once the upload is accepted, without waiting for ready

  app policy:
    (default)         if GET /apps/nexus reports ready|failed: PUT /apps/nexus
                      (blue-green; the current revision keeps serving until the
                      new one is ready). Any other state is deleted APP-SCOPED and
                      re-created, because §6.2 forbids a PUT there.
    --no-teardown     never replace/delete: an existing app is a conflict error
                      (deploy-daytona's 409 posture), for when the app is not
                      yours to kill
    --teardown        DELETE /apps/nexus only — remove the app, ship nothing.
                      The endpoint keeps serving; no other app is touched.

  container recycle (Modal box ONLY, explicit and DESTRUCTIVE):
    --recycle-container
                      sends DELETE /_provider/sandbox, which ends the runner and
                      the container; Modal relaunches it at the same URL. This
                      removes EVERY app on the box, not just ${APP_NAME}, so it is
                      never implied by the replace posture and never used as
                      conflict resolution. Refused for non-Modal endpoints and for
                      a Modal SANDBOX tunnel (…modal.host), whose URL dies with
                      the container. Mutually exclusive with --teardown.

  behavior:
    --dry-run         print the plan; no probe, no DELETE, no child, no cargo
    -h, --help        this text

  the app state is read from GET /apps/${APP_NAME} (never the v1 /status alias) and
  the hub URL is endpoint + AppView.path (${APP_PATH}); no per-port preview link.`;

// ══════════════════════════════════════════════════════════════════════════════
//  FLAGS — pure parser: it returns errors instead of printing or exiting
// ══════════════════════════════════════════════════════════════════════════════

/** Which of the two verbs this run is. Derived in ONE place so the rule is testable. */
export type EndpointMode = 'deploy' | 'teardown';

/** Every flag value after parsing; the deploy passthrough keeps deploy-daytona.ts's names. */
export type EndpointFlags = {
    help: boolean;
    dryRun: boolean;
    teardownOnly: boolean;
    /** true = replace/delete an existing app (the default); --no-teardown flips it. */
    replace: boolean;
    /** Explicit, destructive, Modal-box-only container recycle. Never implied. */
    recycleContainer: boolean;
    endpoint: string;
    token: string;
    adminToken: string;
    mode: 'local' | 'remote-build';
    /** undefined = auto-allocate; --port 0 means the same. */
    port?: number;
    env: Record<string, string>;
    noBuild: boolean;
    artifact: string;
    wait: boolean;
};

/** Parsed flags plus the problems found — main() decides how to report them. */
export type ParsedEndpointArgs = { flags: EndpointFlags; errors: string[] };

/**
 * Strict flat-flag parser in the sibling scripts' shape: the FIRST bare token is the
 * URL, `--env` repeats and accumulates (splitting on the first `=` so values may
 * contain `=`), a `--`-leading token after a value flag is a missing value, unknown
 * flags are errors. It never throws and never prints — main() reports what it returns.
 */
export const parseArgs = (argv: readonly string[], env: Readonly<Record<string, string | undefined>> = process.env): ParsedEndpointArgs => {
    const flags: EndpointFlags = {
        help: false,
        dryRun: false,
        teardownOnly: false,
        replace: true,
        recycleContainer: false,
        // The existing daytona env names stay valid; the platform-neutral ones win.
        endpoint: env.NEXUS_ENDPOINT ?? env.NEXUS_DAYTONA_ENDPOINT ?? '',
        token: env.NEXUS_ENDPOINT_TOKEN ?? env.NEXUS_DAYTONA_PREVIEW_TOKEN ?? '',
        // `||` so an empty nexus-specific variable falls through to the generic name.
        adminToken: env.NEXUS_SANDBOX_ADMIN_TOKEN || env.SANDBOX_ADMIN_TOKEN || '',
        mode: 'local',
        // undefined = auto (§8.3); the v1 8085 default is deliberately gone.
        port: undefined,
        env: {},
        noBuild: false,
        artifact: '',
        wait: true,
    };
    const errors: string[] = [];
    /** The positional URL — first token that is not a flag or a flag's value. */
    let positional = '';
    for (let i = 0; i < argv.length; i++) {
        const flag = argv[i]!;
        if (flag === '--endpoint' || flag === '--token' || flag === '--admin-token' || flag === '--mode' || flag === '--port' || flag === '--env' || flag === '--artifact') {
            const value = argv[++i];
            if (value === undefined || value.startsWith('--')) {
                errors.push(`${flag} needs a value`);
                continue;
            }
            if (flag === '--endpoint') flags.endpoint = value;
            else if (flag === '--token') flags.token = value;
            else if (flag === '--admin-token') flags.adminToken = value;
            else if (flag === '--artifact') flags.artifact = value;
            else if (flag === '--mode') flags.mode = value as EndpointFlags['mode'];
            else if (flag === '--port') {
                const port = Number(value);
                if (!Number.isInteger(port) || port < 0 || port > 65535) errors.push(`--port ${JSON.stringify(value)} is out of range (TCP port 0-65535, 0 = auto)`);
                // 0 is the explicit spelling of auto (§8.3): it must not reach the
                // child as a literal config.port, so it lands as undefined.
                else flags.port = port === 0 ? undefined : port;
            } else {
                const eq = value.indexOf('=');
                if (eq <= 0) errors.push(`--env expects KEY=VALUE, got "${value}"`);
                else flags.env[value.slice(0, eq)] = value.slice(eq + 1);
            }
            continue;
        }
        if (flag === '--no-wait') flags.wait = false;
        else if (flag === '--wait') flags.wait = true;
        else if (flag === '--no-build') flags.noBuild = true;
        else if (flag === '--teardown') flags.teardownOnly = true;
        else if (flag === '--no-teardown') flags.replace = false;
        else if (flag === '--recycle-container') flags.recycleContainer = true;
        else if (flag === '--dry-run') flags.dryRun = true;
        else if (flag === '-h' || flag === '--help') flags.help = true;
        else if (flag.startsWith('-')) errors.push(`unknown flag "${flag}"`);
        else if (positional) errors.push(`unexpected extra argument "${flag}" — the endpoint is the only positional (got "${positional}" already)`);
        else positional = flag;
    }
    // --help wins outright: a broken command line still gets the usage screen.
    if (flags.help) return { flags, errors };
    if (!positional && !flags.endpoint) errors.push(`no endpoint — pass it as the argument (deploy:endpoint <url>) or with --endpoint / NEXUS_ENDPOINT`);
    if (flags.mode !== 'local' && flags.mode !== 'remote-build') errors.push(`--mode must be "local" or "remote-build", got "${flags.mode}"`);
    // Mirrors deploy-daytona.ts's own rule, so the contradiction dies before a build.
    if (flags.mode !== 'local' && (flags.noBuild || flags.artifact)) errors.push('--no-build and --artifact apply to --mode local only (remote-build ships a source tarball)');
    // --teardown ships no bytes, so shipping knobs would be silently ignored.
    if (flags.teardownOnly && (flags.noBuild || flags.artifact || Object.keys(flags.env).length || !flags.wait)) errors.push('--teardown ships nothing, so --env/--no-build/--artifact/--no-wait do not apply (drop them, or drop --teardown)');
    // A container recycle is a different, much more destructive verb than teardown:
    // one removes nexus, the other removes EVERY app on the box.
    if (flags.teardownOnly && flags.recycleContainer) errors.push('--teardown removes app "nexus" only; --recycle-container discards the whole Modal box — pick one');
    if (!flags.replace && flags.recycleContainer) errors.push('--no-teardown forbids touching the endpoint, and --recycle-container destroys the whole box — pick one');
    if (positional && flags.endpoint && positional !== flags.endpoint) errors.push(`two different endpoints given: "${positional}" and --endpoint "${flags.endpoint}"`);
    if (positional) flags.endpoint = positional;
    return { flags, errors };
};

/** Mode resolution — the single source of truth for "do we ship bytes or not". */
export const resolveMode = (flags: Pick<EndpointFlags, 'teardownOnly'>): EndpointMode => (flags.teardownOnly ? 'teardown' : 'deploy');

// ══════════════════════════════════════════════════════════════════════════════
//  URL + AUTH — the contract is the interface, so the same code serves every platform
// ══════════════════════════════════════════════════════════════════════════════

/**
 * Base-relative join that PRESERVES a path prefix (re-exported from the shared
 * contract module so the tests can name it from here, as they always have).
 */
export { joinUrl };

/** Empty token → NO header at all: that is what keeps public endpoints working. */
export const authHeaders = (token: string): Record<string, string> => (token ? { 'x-daytona-preview-token': token } : {});

/** The two credentials one request may carry; the admin Bearer is independent. */
export type Credentials = { previewToken?: string; adminToken?: string };

/** Thrown with the fix in the message; main() turns it into exit 1 plus that one line. */
export class EndpointDeployError extends Error {}

/** Validates the endpoint where it is first needed, so a typo is not a transport mystery. */
export const requireEndpoint = (endpoint: string): string => {
    if (!endpoint) throw new EndpointDeployError(`no endpoint — pass it as the argument, e.g. deploy:endpoint https://…`);
    let url: URL;
    try {
        url = new URL(endpoint);
    } catch {
        throw new EndpointDeployError(`endpoint is not a valid absolute URL: "${endpoint}"`);
    }
    if (url.protocol !== 'http:' && url.protocol !== 'https:') throw new EndpointDeployError(`endpoint must be http(s), got "${url.protocol}"`);
    return endpoint;
};

// ══════════════════════════════════════════════════════════════════════════════
//  PROBE — one HTTP attempt, and the PURE reading of its answer
// ══════════════════════════════════════════════════════════════════════════════

/** Normalised answer: `transportError` set only when no HTTP reply came back at all. */
export type ProbeResult = { status: number; ok: boolean; json: unknown; text: string; transportError?: string };

/** The injectable HTTP seam (the real one is at the bottom; a test scripts a list). */
export type Probe = (method: string, url: string, init: { headers: Record<string, string>; timeoutMs: number }) => Promise<ProbeResult>;

/**
 * What one probe means for a route that EXPECTS a runner answer, decided PURELY so
 * the cold-start/auth/dead table is testable without a clock or a network:
 *   'ready' — 2xx with a JSON body
 *   'retry' — no reply, or 408/429/5xx: the container is not serving yet (Modal answers
 *             502/503 until a web-function container holds the port; a Daytona proxy
 *             resets while the sandbox is still starting). Normal → wait it out.
 *   'auth'  — 401/403: something IS there, but a credential is missing or wrong.
 *             Retrying cannot fix that, so it aborts with the fix instead of burning
 *             the budget.
 *   'dead'  — any other non-2xx, or a 2xx that is not JSON: no such endpoint (a
 *             typo'd Modal label, a stopped app, an expired Daytona sandbox), or a
 *             DIFFERENT service answering on that URL. Retrying that is pure waiting.
 */
export type ProbeVerdict = 'ready' | 'retry' | 'auth' | 'dead';
export const classifyProbe = (result: ProbeResult): ProbeVerdict => {
    if (result.transportError) return 'retry';
    if (result.status === 401 || result.status === 403) return 'auth';
    if (result.status >= 500 || result.status === 408 || result.status === 429) return 'retry';
    if (!result.ok) return 'dead';
    if (typeof result.json !== 'object' || result.json === null) return 'dead';
    return 'ready';
};

/** Human label for one probe outcome, reused by both the wait narration and its errors. */
const describeProbe = (result: ProbeResult): string => result.transportError ?? `HTTP ${result.status}`;

/** The credential fix, worded once: the two tokens are different things. */
const authFix = (credentials: Credentials): string =>
    `Pass --token <the Daytona preview token> (or NEXUS_ENDPOINT_TOKEN) for the edge` +
    `${credentials.adminToken ? ', and check --admin-token' : '; if the runner has SANDBOX_ADMIN_TOKEN set, also pass --admin-token / NEXUS_SANDBOX_ADMIN_TOKEN'}` +
    `.`;

/**
 * Waits for one route to answer and returns the parsed body, aborting immediately on
 * 'auth' and 'dead'. Bounded by BOTH the clock and the attempt count — the attempt
 * count is what makes it testable with a frozen clock, and it also stops a hung proxy
 * from turning a poll loop into an unresponsive terminal.
 */
export const waitForRoute = async (input: {
    method: 'GET' | 'DELETE';
    url: string;
    credentials: Credentials;
    what: string;
    budgetMs: number;
    deps: EndpointDeps;
}): Promise<ProbeResult> => {
    const { method, url, credentials, what, budgetMs, deps } = input;
    const log = deps.log ?? ((text: string) => console.log(text));
    const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((done) => setTimeout(done, ms)));
    const now = deps.now ?? Date.now;
    const deadline = now() + budgetMs;
    const maxAttempts = Math.max(1, Math.ceil(budgetMs / POLL_MS));
    let attempts = 0;
    for (;;) {
        const result = await deps.probe(method, url, { headers: requestHeaders(credentials), timeoutMs: REQUEST_TIMEOUT_MS });
        const verdict = classifyProbe(result);
        if (verdict === 'ready') return result;
        if (verdict === 'auth')
            throw new EndpointDeployError(`${url} → ${result.status} — this endpoint guards its contract routes. ${authFix(credentials)}`);
        if (verdict === 'dead')
            throw new EndpointDeployError(
                `${url} → ${describeProbe(result)} — that is not a sandbox runtime v2 runner. Check the URL (a Daytona runner preview link, a Modal \`…--<label>.modal.run\` or \`…<id>…modal.host\` tunnel) ` +
                    `and that the container is still up: a stopped app or an expired sandbox answers exactly like this.`,
            );
        attempts += 1;
        if (attempts === 1) log(`${PREFIX} ${what} is not answering yet (${describeProbe(result)}) — waiting up to ${Math.round(budgetMs / 1000)}s while the container comes up…`);
        if (now() >= deadline || attempts >= maxAttempts)
            throw new EndpointDeployError(`${what} never answered at ${url} (last: ${describeProbe(result)}, ${attempts} tries over ${Math.round(budgetMs / 1000)}s). Follow the container's own log, then retry.`);
        await sleep(POLL_MS);
    }
};

// ══════════════════════════════════════════════════════════════════════════════
//  THE v2 GATE (§7.2, §17.1)
// ══════════════════════════════════════════════════════════════════════════════

/** The gate's verdict; a v1 single-slot runner is refused with its own message. */
export type RunnerVerdict = { ok: true; health: RunnerHealth } | { ok: false; message: string };

/**
 * Requires `apiVersion >= 2` or `capabilities` containing `apps` (§17.1). A v1
 * runner reports `daytona-runtime-v1` and satisfies neither; every route this
 * script uses (`/apps/nexus`) exists only in v2, so deploying to it would fail
 * later with a confusing 404 (or, worse, hit the v1 root alias).
 */
export const classifyRunnerHealth = (json: unknown): RunnerVerdict => {
    const health = parseHealth(json);
    if (!isV2Runner(health)) {
        return {
            ok: false,
            message:
                `this endpoint is not a sandbox runtime v2 runner — ${describeHealth(health)}. ` +
                `deploy:endpoint needs the /apps registry (apiVersion >= 2 or capabilities [apps]); a v1 single-slot runner cannot serve ${APP_PATH}. ` +
                `Update the runner, or use --teardown/deploy:daytona only against a v2 sandbox.`,
        };
    }
    return { ok: true, health };
};

// ══════════════════════════════════════════════════════════════════════════════
//  THE APP RECORD (§5.2, §7.6) — GET /apps/nexus, never the v1 /status alias
// ══════════════════════════════════════════════════════════════════════════════

/** The registry's answer for this app: present with a redacted AppView, or absent. */
export type AppState = { present: true; view: AppView } | { present: false };

/**
 * What one `GET /apps/nexus` means, decided PURELY:
 *   'present' — 200 with an AppView. 'absent' — 404 app_not_found (the ONLY not-found
 *               that means "not registered"; §7.6). 'retry' — transport/5xx/408/429.
 *   'auth'    — 401/403. 'dead' — anything else, including a 2xx that is not an AppView
 *               (a different service, or a broken runner — never treat it as absent).
 */
export type AppProbeVerdict = 'present' | 'absent' | 'retry' | 'auth' | 'dead';
export const classifyAppProbe = (result: ProbeResult): AppProbeVerdict => {
    if (result.transportError) return 'retry';
    if (result.status === 401 || result.status === 403) return 'auth';
    if (result.status === 404) return 'absent';
    if (result.status >= 500 || result.status === 408 || result.status === 429) return 'retry';
    if (!result.ok) return 'dead';
    return parseAppView(result.json) ? 'present' : 'dead';
};

/** One probe of the app record, plus its verdict. */
export const probeAppOnce = async (endpoint: string, credentials: Credentials, deps: EndpointDeps): Promise<{ verdict: AppProbeVerdict; result: ProbeResult }> => {
    const result = await deps.probe('GET', appStateUrl(endpoint), { headers: requestHeaders(credentials), timeoutMs: REQUEST_TIMEOUT_MS });
    return { verdict: classifyAppProbe(result), result };
};

/**
 * Reads the app record, waiting out a cold start. A 404 is returned IMMEDIATELY as
 * `{present:false}` — it is the ordinary "not deployed yet" answer, not a failure.
 * 'auth' and 'dead' abort with the fix; a body that is not an AppView is 'dead', so
 * a proxy error page can never be mistaken for the app being absent.
 */
export const readApp = async (input: { endpoint: string; credentials: Credentials; deps: EndpointDeps; budgetMs?: number }): Promise<AppState> => {
    const { endpoint, credentials, deps } = input;
    const budgetMs = input.budgetMs ?? ENDPOINT_WAIT_MS;
    const log = deps.log ?? ((text: string) => console.log(text));
    const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((done) => setTimeout(done, ms)));
    const now = deps.now ?? Date.now;
    const deadline = now() + budgetMs;
    const maxAttempts = Math.max(1, Math.ceil(budgetMs / POLL_MS));
    const url = appStateUrl(endpoint);
    let attempts = 0;
    for (;;) {
        const { verdict, result } = await probeAppOnce(endpoint, credentials, deps);
        if (verdict === 'present') return { present: true, view: parseAppView(result.json)! };
        if (verdict === 'absent') return { present: false };
        if (verdict === 'auth') throw new EndpointDeployError(`${url} → ${result.status} — the app record is guarded. ${authFix(credentials)}`);
        if (verdict === 'dead')
            throw new EndpointDeployError(
                `${url} → ${describeProbe(result)} — that is not an AppView from a sandbox runtime v2 runner. Check the URL and that the runner is still up.`,
            );
        attempts += 1;
        if (attempts === 1) log(`${PREFIX} GET ${APP_MOUNT} is not answering yet (${describeProbe(result)}) — waiting up to ${Math.round(budgetMs / 1000)}s while the container comes up…`);
        if (now() >= deadline || attempts >= maxAttempts)
            throw new EndpointDeployError(`GET /apps/${APP_NAME} never answered at ${url} (last: ${describeProbe(result)}, ${attempts} tries over ${Math.round(budgetMs / 1000)}s). Follow the container's own log, then retry.`);
        await sleep(POLL_MS);
    }
};

/** `GET /apps/nexus` (exported for the plan/tests, so the route is never re-spelled). */
export const appStateUrl = (endpoint: string): string => joinUrl(endpoint, APP_ROUTE);
/** `DELETE /apps/nexus` — the ONLY teardown route this script uses. */
export const appDeleteUrl = (endpoint: string): string => joinUrl(endpoint, APP_ROUTE);
/** `GET /apps/nexus/logs?lines=n` — the failure diagnosis route. */
export const appLogsUrl = (endpoint: string, lines: number): string => `${joinUrl(endpoint, APP_LOGS_ROUTE)}?lines=${lines}`;
/** `GET /health` — the v2 gate. */
export const healthUrl = (endpoint: string): string => joinUrl(endpoint, 'health');

/**
 * DELETE /apps/nexus and read its `{tornDown, removed}` answer. `removed:false` is
 * a SUCCESS (§6.4: the name was already absent); only a missing/c false `tornDown`
 * is a failure. The caller verifies the 404 afterwards — this function only proves
 * the runner accepted the verb.
 */
export const deleteApp = async (endpoint: string, credentials: Credentials, deps: EndpointDeps): Promise<{ removed: boolean }> => {
    const url = appDeleteUrl(endpoint);
    const result = await waitForRoute({ method: 'DELETE', url, credentials, what: 'DELETE /apps/nexus', budgetMs: SLOT_WAIT_MS, deps });
    const body = (result.json ?? {}) as { tornDown?: unknown; removed?: unknown };
    if (body.tornDown !== true) throw new EndpointDeployError(`${url} did not confirm the teardown: ${JSON.stringify(body).slice(0, 200) || result.text.slice(0, 200) || describeProbe(result)}`);
    return { removed: body.removed === true };
};

/**
 * Proves the app is gone: a fresh GET /apps/nexus must read 404. The contract's
 * teardown is synchronous, so this normally passes on the first poll; the check is
 * what catches a runner that answers `{tornDown:true}` and keeps the record.
 */
export const verifyAppAbsent = async (endpoint: string, credentials: Credentials, deps: EndpointDeps): Promise<void> => {
    const state = await readApp({ endpoint, credentials, deps, budgetMs: SLOT_WAIT_MS });
    if (state.present) throw new EndpointDeployError(`DELETE /apps/${APP_NAME} was accepted but GET /apps/${APP_NAME} still reports "${state.view.status}" — inspect the runner.`);
};

// ══════════════════════════════════════════════════════════════════════════════
//  MODAL CONTAINER RECYCLE — explicit only, destructive across apps
// ══════════════════════════════════════════════════════════════════════════════
//
//  Separate from deleteApp on purpose: DELETE /apps/nexus removes ONE app and the
//  container keeps serving every peer. On Modal the container is the thing that
//  would be discarded, and DELETE /_provider/sandbox is the Modal-only route that
//  ends the runner. It is NEVER ordinary conflict resolution: only
//  --recycle-container reaches it, and only after the endpoint proves it is a
//  Modal box.

/** Which Modal container shape an endpoint is. */
export type ModalContainerShape = 'box' | 'sandbox';

/**
 * Classifies an endpoint as a Modal box / sandbox, or null for anything else.
 * Two conditions, both required: the runner says it is the Modal one (`service`), and
 * the host is one of Modal's. A loopback smoke run reports the service but has no host
 * to relaunch, and a Daytona preview URL has neither — both return null, so no
 * destructive action is ever taken on a guess.
 */
export const classifyModalContainer = (endpoint: string, health: Record<string, unknown>): ModalContainerShape | null => {
    if (String(health.service ?? '') !== MODAL_RUNNER_SERVICE) return null;
    let host: string;
    try {
        host = new URL(endpoint).hostname;
    } catch {
        return null; // requireEndpoint validated it already; anything unclassifiable is left alone
    }
    if (MODAL_SANDBOX_HOST.test(host)) return 'sandbox';
    if (MODAL_BOX_HOST.test(host)) return 'box';
    return null;
};

/**
 * The DELETE that ends a Modal box's container, with three outcomes kept distinct:
 *   · the runner confirmed {tornDown:true} → proceed to the death proof;
 *   · the connection died while answering → the runner is going down anyway, and
 *     waitForModalContainerReset below is the real proof, so proceeding is safe;
 *   · any other answer (401/403 included) → fail closed. Recycling on a guess is
 *     exactly what this explicit verb exists to prevent.
 */
export const requestModalContainerReset = async (endpoint: string, credentials: Credentials, deps: EndpointDeps): Promise<void> => {
    const url = joinUrl(endpoint, PROVIDER_RESET_ROUTE);
    const result = await deps.probe('DELETE', url, { headers: requestHeaders(credentials), timeoutMs: RESET_TIMEOUT_MS });
    if (result.transportError) return;
    if (result.status === 401 || result.status === 403) throw new EndpointDeployError(`${url} → ${result.status} — the container recycle is a contract route. ${authFix(credentials)}`);
    if (result.status === 404)
        throw new EndpointDeployError(
            `${url} → 404 — this Modal runner does not implement DELETE /_provider/sandbox (the v2 route §4.5 moved it to). ` +
                `Update the Modal adapter, or deploy without --recycle-container.`,
        );
    if (result.status !== 200)
        throw new EndpointDeployError(
            `${url} → HTTP ${result.status}${result.text ? `: ${result.text.slice(0, 200)}` : ''} — without DELETE /_provider/sandbox the container cannot be recycled, and this run refuses to pretend it was.`,
        );
    const body = (result.json ?? {}) as { tornDown?: unknown };
    if (body.tornDown !== true) throw new EndpointDeployError(`${url} did not confirm the container reset: ${JSON.stringify(body).slice(0, 200) || result.text.slice(0, 200) || describeProbe(result)}`);
};

/**
 * Did a /health answer come from a process that started at or after the reset?
 *
 * This is the only per-process identity the contract exposes (`uptimeSec`), and it is
 * read on the CONTAINER's own clock — so the comparison is against the time elapsed
 * since DELETE /_provider/sandbox was sent, with a slack window for clock skew. A
 * process that predates the delete reports an uptime older than that elapsed time by
 * its whole earlier life, which is what keeps the old container's dying answers from
 * being mistaken for the replacement.
 */
const startedAfterReset = (health: unknown, resetAtMs: number, nowMs: number): boolean => {
    const uptimeSec = Number((health as Record<string, unknown> | null)?.uptimeSec);
    if (!Number.isFinite(uptimeSec)) return false;
    return uptimeSec * 1000 <= nowMs - resetAtMs + FRESH_UPTIME_SLACK_MS;
};

/**
 * Wait for the REPLACEMENT of a recycled Modal box and prove the app record is gone.
 *
 * Two facts have to agree, because "it answered 200" is not enough on this platform:
 *   · the process is NEW — its /health uptime is younger than the elapsed time since
 *     the delete (see `startedAfterReset`), or the endpoint went non-2xx in between,
 *     which proves the old one left;
 *   · nexus is ABSENT — GET /apps/nexus reads 404, so no app record survived into
 *     the new container (the whole box was discarded, so anything present is a
 *     contradiction worth failing on).
 * The loop is bounded by RECYCLE_WAIT_MS and fails closed.
 */
export const waitForModalContainerReset = async (endpoint: string, credentials: Credentials, deps: EndpointDeps, resetAtMs: number): Promise<AppState> => {
    const log = deps.log ?? ((text: string) => console.log(text));
    const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((done) => setTimeout(done, ms)));
    const now = deps.now ?? Date.now;
    const deadline = now() + RECYCLE_WAIT_MS;
    const maxAttempts = Math.max(1, Math.ceil(RECYCLE_WAIT_MS / POLL_MS));
    let oldGone = false;
    let last = 'no answer yet';
    for (let attempt = 1; ; attempt++) {
        const health = await deps.probe('GET', healthUrl(endpoint), { headers: requestHeaders(credentials), timeoutMs: REQUEST_TIMEOUT_MS });
        if (!health.ok) {
            // No 2xx: nothing is serving a runner at this URL right now, so the old
            // container is gone and whatever answers next is the replacement.
            oldGone = true;
            last = describeProbe(health);
        } else if (oldGone || startedAfterReset(health.json, resetAtMs, now())) {
            const { verdict, result } = await probeAppOnce(endpoint, credentials, deps);
            if (verdict === 'absent') return { present: false };
            if (verdict === 'present') {
                const view = parseAppView(result.json)!;
                last = `a fresh container whose app still reports "${view.status}"`;
            } else {
                last = `GET /apps/${APP_NAME} → ${describeProbe(result)}`;
            }
        } else {
            last = 'the old container is still answering';
        }
        if (attempt === 1) log(`${PREFIX} waiting for the replacement container (up to ${Math.round(RECYCLE_WAIT_MS / 1000)}s) — it must answer with a young uptime and no nexus app`);
        if (now() >= deadline || attempt >= maxAttempts)
            throw new EndpointDeployError(`${endpoint} never reported a fresh, empty container after DELETE /_provider/sandbox (last: ${last}) — nothing was deployed. Check \`modal app logs\`, then retry.`);
        await sleep(POLL_MS);
    }
};

/**
 * The explicit container recycle: kill the container, wait for the replacement Modal
 * relaunches at the same URL, and prove nexus is absent in it.
 */
export const resetModalBoxContainer = async (endpoint: string, credentials: Credentials, deps: EndpointDeps): Promise<AppState> => {
    const log = deps.log ?? ((text: string) => console.log(text));
    const resetAtMs = (deps.now ?? Date.now)();
    await requestModalContainerReset(endpoint, credentials, deps);
    const state = await waitForModalContainerReset(endpoint, credentials, deps, resetAtMs);
    log(`${PREFIX} ✓ replacement container is up — the deploy lands in THIS container`);
    return state;
};

/**
 * Where the hub actually is once it is ready, decided from the app's OWN record:
 * `endpoint + AppView.path`, with `AppView.publicUrl` as the fallback. There is no
 * per-port preview link in v2 (P-7/P-8), so a missing record means the URL is simply
 * not known — never guessed by swapping a port into the endpoint.
 */
export const buildHubUrl = (state: AppState, endpoint: string): { hubUrl: string | null; note: string } => {
    if (!state.present) return { hubUrl: null, note: `app "${APP_NAME}" is not registered` };
    const hubUrl = resolveAppUrl(endpoint, state.view);
    if (hubUrl) return { hubUrl, note: `endpoint + ${state.view.path} (the only public app URL)` };
    return { hubUrl: null, note: `the AppView reported neither path nor publicUrl (status ${state.view.status})` };
};

// ══════════════════════════════════════════════════════════════════════════════
//  CHILD INVOCATION — the upload itself belongs to deploy-daytona.ts
// ══════════════════════════════════════════════════════════════════════════════

/**
 * The exact argv handed to the child, in a fixed, documented order:
 *   --endpoint, --token?, --admin-token?, --mode, --port?, --replace?, --env…,
 *   --no-build?, --artifact?, --no-wait?
 * `--token`/`--admin-token` are omitted when empty so the child sends no header at
 * all, which is what keeps an unauthenticated endpoint working. `--port` is omitted
 * unless the operator chose one: absent is how v2 says "auto-allocate".
 */
export const buildDeployArgs = (flags: EndpointFlags): string[] => {
    const args = ['--endpoint', flags.endpoint];
    if (flags.token) args.push('--token', flags.token);
    if (flags.adminToken) args.push('--admin-token', flags.adminToken);
    args.push('--mode', flags.mode);
    if (flags.port !== undefined) args.push('--port', String(flags.port));
    // `flags.replace` here is the CHILD's verb (computed by runDeployEndpoint): true
    // means PUT /apps/nexus (blue-green), false means POST /apps (create).
    if (flags.replace) args.push('--replace');
    // Object insertion order == the order --env was typed (JS string-key guarantee).
    for (const [key, value] of Object.entries(flags.env)) args.push('--env', `${key}=${value}`);
    if (flags.noBuild) args.push('--no-build');
    if (flags.artifact) args.push('--artifact', flags.artifact);
    // The child waits by default; only the deviation has to be spelled out.
    if (!flags.wait) args.push('--no-wait');
    return args;
};

/** A fully-resolved child-process call: what to run, with what, from where. */
export type DeployInvocation = { command: string; args: string[]; cwd: string; env: Record<string, string | undefined> };

/**
 * Masks the VALUE of every `--token`/`--admin-token` pair in an argv for display.
 * A dry run prints the child command it WOULD run, and the admin bearer is a secret
 * that must never reach a terminal/log; the preview token is masked for the same
 * reason. The real invocation is untouched — only the rendered copy is redacted.
 */
export const redactSecretArgs = (args: readonly string[]): string[] => {
    const out = [...args];
    for (let i = 0; i < out.length - 1; i++) {
        if (out[i] === '--token' || out[i] === '--admin-token') out[i + 1] = '***';
    }
    return out;
};

/** buildDeployArgs plus the command, script path, cwd and env that make it runnable. */
export const buildDeployInvocation = (flags: EndpointFlags, env: Record<string, string | undefined>): DeployInvocation => ({
    command: process.execPath,
    args: [VITE_NODE_CLI, DEPLOY_SCRIPT_PATH, ...buildDeployArgs(flags)],
    cwd: PACKAGE_ROOT,
    // PASSED THROUGH UNTOUCHED — cargo needs HTTPS_PROXY etc. from the caller's shell.
    env,
});

// ══════════════════════════════════════════════════════════════════════════════
//  SUMMARY — the machine-readable last line
// ══════════════════════════════════════════════════════════════════════════════

/**
 * ONE line of JSON, keys in insertion order. v2 fields, no v1 slot language:
 * `app`, `path`, the app's own `status`/`revision`/`internalPort`, and a hubUrl
 * computed from AppView.path (or null with a hubNote when the record is missing).
 * `token` is emitted only when a preview token was used, and `tokenScope` says what
 * it authenticates: the RUNNER origin, not the app (the app's own auth is its
 * `--env` config). The admin Bearer is never emitted.
 */
export const buildSummary = (input: {
    mode: EndpointMode;
    endpoint: string;
    hubUrl?: string | null;
    status?: string;
    revision?: number | null;
    internalPort?: number | null;
    hubNote?: string;
    removed?: boolean;
    token?: string;
    replaced?: { status: string; revision: number | null };
    recycled?: ModalContainerShape;
}): string =>
    JSON.stringify({
        mode: input.mode,
        endpoint: input.endpoint,
        app: APP_NAME,
        path: APP_PATH,
        ...(input.hubUrl === undefined ? {} : { hubUrl: input.hubUrl }),
        ...(input.hubNote ? { hubNote: input.hubNote } : {}),
        ...(input.status === undefined ? {} : { status: input.status }),
        ...(input.revision === undefined ? {} : { revision: input.revision }),
        ...(input.internalPort === undefined ? {} : { internalPort: input.internalPort }),
        ...(input.removed === undefined ? {} : { removed: input.removed }),
        ...(input.token ? { token: input.token, tokenScope: 'management' } : {}),
        ...(input.replaced ? { replaced: input.replaced } : {}),
        ...(input.recycled ? { recycled: input.recycled } : {}),
    });

// ══════════════════════════════════════════════════════════════════════════════
//  ORCHESTRATION CORE — every side effect comes from the injected deps
// ══════════════════════════════════════════════════════════════════════════════

/** Injectable side effects — the seams the test suite drives with zero network. */
export type EndpointDeps = {
    /** real: global fetch, normalised into ProbeResult. */
    probe: Probe;
    /** real: child_process.spawn with stdio inherit; resolves the child's exit code. */
    spawn: (invocation: DeployInvocation) => Promise<number>;
    /** stdout sink. Default console.log. */
    log?: (line: string) => void;
    /** stderr sink (usage, failures). Default console.error. */
    error?: (line: string) => void;
    /** env read for the fallbacks and handed to the child. Default process.env. */
    env?: Record<string, string | undefined>;
    /** Poll delay. Default setTimeout. */
    sleep?: (ms: number) => Promise<void>;
    /** Clock for the wait budgets. Default Date.now. */
    now?: () => number;
};

/** Normalises flags into the two credentials every request may carry. */
export const credentialsOf = (flags: Pick<EndpointFlags, 'token' | 'adminToken'>): Credentials => ({ previewToken: flags.token, adminToken: flags.adminToken });

/**
 * Runs the whole flow and returns the process exit code (0 = app ready / plan
 * printed). Never calls process.exit, so a caller can wrap it.
 */
export const runDeployEndpoint = async (flags: EndpointFlags, deps: EndpointDeps): Promise<number> => {
    const log = deps.log ?? ((text: string) => console.log(text));
    const error = deps.error ?? ((text: string) => console.error(text));
    const env = deps.env ?? process.env;
    const line = (text: string) => log(`${PREFIX} ${text}`);
    const mode = resolveMode(flags);
    const endpoint = requireEndpoint(flags.endpoint);
    const credentials = credentialsOf(flags);
    const childEnv: Record<string, string | undefined> = {
        ...env,
        NEXUS_DAYTONA_ENDPOINT: endpoint,
        ...(flags.token ? { NEXUS_DAYTONA_PREVIEW_TOKEN: flags.token } : {}),
        ...(flags.adminToken ? { NEXUS_SANDBOX_ADMIN_TOKEN: flags.adminToken } : {}),
    };

    // ── dry run: the plan, and provably nothing else (no probe, no child) ──
    if (flags.dryRun) {
        line('DRY RUN — no probe, no DELETE, no child process, no cargo, no network');
        line(`endpoint: ${endpoint}`);
        line(`  v2 gate:   GET ${healthUrl(endpoint)} — requires apiVersion >= 2 or capabilities [apps]`);
        line(`  app:       GET ${appStateUrl(endpoint)} — 404 → POST ${joinUrl(endpoint, APPS_ROUTE)}`);
        line(`             ready|failed → PUT ${appStateUrl(endpoint)} (blue-green; no peer touched)`);
        line(`             queued|building|booting|draining → DELETE ${appDeleteUrl(endpoint)}, then POST`);
        line(`  teardown:  DELETE ${appDeleteUrl(endpoint)} — app-scoped; verifies GET → 404`);
        // Presence/length only: the plan is pasted into logs and terminals.
        line(`  auth:      x-daytona-preview-token: ${maskSecret(flags.token)}`);
        line(`             authorization: Bearer ${maskSecret(flags.adminToken)}  (never printed)`);
        if (mode === 'teardown') {
            line('mode: teardown — removes app "nexus" only; the endpoint and every peer keep serving.');
            return 0;
        }
        line(`mode: deploy (${flags.mode}) — ${flags.port === undefined ? 'internal port AUTO-ALLOCATED' : `internal port ${flags.port}`}${flags.replace ? ', and an existing app is replaced app-scoped' : ', and an existing app is an error (--no-teardown)'}`);
        line(
            flags.recycleContainer
                ? `  container: --recycle-container → DELETE ${joinUrl(endpoint, PROVIDER_RESET_ROUTE)} — DESTRUCTIVE: removes EVERY app on the Modal box, then waits for a fresh container`
                : '  container: no reset — DELETE /_provider/sandbox is never implied by the replace posture (--recycle-container opts in, Modal box only)',
        );
        line(`deploy command that WOULD run (cwd ${PACKAGE_ROOT}):`);
        // Credentials masked: the admin bearer must never be printed, and a preview
        // token in a pasted plan is the same leak.
        line(`  ${redactSecretArgs(buildDeployInvocation(flags, childEnv).args).join(' ')}`);
        line(`hub URL after success: ${appUrl(endpoint, APP_PATH)} (endpoint + AppView.path; no per-port preview link)`);
        return 0;
    }

    // ── preflight: is that URL a v2 runner? ──
    line(`probing ${endpoint} …`);
    const healthResult = await waitForRoute({ method: 'GET', url: healthUrl(endpoint), credentials, what: 'GET /health', budgetMs: ENDPOINT_WAIT_MS, deps });
    const verdict = classifyRunnerHealth(healthResult.json);
    if (!verdict.ok) throw new EndpointDeployError(verdict.message);
    line(`✓ sandbox runtime v2 — ${describeHealth(verdict.health)}`);

    // ── the app record: the registry, never the v1 /status alias ──
    let app = await readApp({ endpoint, credentials, deps });
    line(app.present ? `app ${APP_NAME}: ${describeAppView(app.view)}` : `app ${APP_NAME}: absent (POST /apps will create it)`);
    const healthJson = (healthResult.json ?? {}) as Record<string, unknown>;
    const modalContainer = classifyModalContainer(endpoint, healthJson);

    // ── teardown: remove the app only; the endpoint keeps serving every peer ──
    if (mode === 'teardown') {
        if (!app.present) {
            line(`✓ app ${APP_NAME} is already absent — nothing to remove (removed:false); the endpoint is untouched`);
            log(buildSummary({ mode, endpoint, removed: false, ...(flags.token ? { token: flags.token } : {}) }));
            return 0;
        }
        const replaced = { status: app.view.status, revision: app.view.revision };
        line(`DELETE ${appDeleteUrl(endpoint)} — removing app "${APP_NAME}" (${app.view.status}); every peer app is untouched`);
        const deleted = await deleteApp(endpoint, credentials, deps);
        await verifyAppAbsent(endpoint, credentials, deps);
        line(`✓ app ${APP_NAME} removed — GET ${appStateUrl(endpoint)} now answers 404; the endpoint keeps serving`);
        log(buildSummary({ mode, endpoint, removed: deleted.removed, replaced, ...(flags.token ? { token: flags.token } : {}) }));
        return 0;
    }

    // ── explicit container recycle (destructive across apps) ──
    let recycled: ModalContainerShape | undefined;
    if (flags.recycleContainer) {
        if (!modalContainer) {
            throw new EndpointDeployError(
                `--recycle-container only applies to a Modal box, and ${endpoint} is not one (GET /health's service marker + a *.modal.run host are both required). ` +
                    `Drop the flag: an existing app is replaced app-scoped with PUT /apps/${APP_NAME}, and peers are never touched.`,
            );
        }
        if (modalContainer === 'sandbox') {
            throw new EndpointDeployError(
                `${endpoint} is a Modal SANDBOX tunnel (…modal.host) — its URL dies with the container, so it cannot be recycled in place. ` +
                    `Deploy to the box instead (the …modal.run URL of the same app), or free app ${APP_NAME} with --teardown and deploy again.`,
            );
        }
        line(`DELETE ${joinUrl(endpoint, PROVIDER_RESET_ROUTE)} — --recycle-container: this ends the container and discards EVERY app on it, not only ${APP_NAME}`);
        const reset = await resetModalBoxContainer(endpoint, credentials, deps);
        app = reset;
        recycled = 'box';
        line(`app ${APP_NAME}: absent in the replacement container — POST /apps will create it`);
    } else if (modalContainer) {
        // Say the quiet part out loud: the replace posture must never be read as
        // permission to recycle the box.
        line('Modal box: the container is NOT recycled — replacement is app-scoped; --recycle-container opts into discarding every app');
    }

    // ── app-scoped replace policy ──
    let childFlags: EndpointFlags = flags;
    let replaced: { status: string; revision: number | null } | undefined;
    if (app.present) {
        const view = app.view;
        if (view.pending) {
            throw new EndpointDeployError(
                `a replacement of app "${APP_NAME}" is already in progress (pending revision ${view.pending.revision ?? '?'} ${view.pending.status}) — ` +
                    `wait for GET ${appStateUrl(endpoint)} to report pending:null, or --teardown to abort it`,
            );
        }
        if (!flags.replace) {
            throw new EndpointDeployError(
                `app "${APP_NAME}" already exists (${view.status}) and --no-teardown forbids replacing it — ` +
                    `drop --no-teardown to replace it app-scoped${isAppReplaceable(view) ? ' (blue-green PUT)' : ''}, or pass --teardown to remove it first`,
            );
        }
        replaced = { status: view.status, revision: view.revision };
        if (isAppReplaceable(view)) {
            // §6.3: the current revision keeps serving until the pending one is ready.
            childFlags = { ...flags, replace: true };
            line(`app ${APP_NAME} is ${view.status} — PUT ${appStateUrl(endpoint)} (blue-green; revision ${view.revision ?? '?'} keeps serving until the new one is ready)`);
        } else {
            // §6.2 step 3 forbids a PUT for queued/staging/building/booting/draining.
            // Deleting THIS APP is the app-scoped way out; the registry stays intact
            // for every peer (a container reset would not be).
            line(`app ${APP_NAME} is "${view.status}", which §6.2 does not allow a PUT to replace — deleting just this app first (DELETE ${appDeleteUrl(endpoint)})`);
            await deleteApp(endpoint, credentials, deps);
            await verifyAppAbsent(endpoint, credentials, deps);
            app = { present: false };
            childFlags = { ...flags, replace: false };
        }
    } else {
        childFlags = mode === 'deploy' ? { ...flags, replace: false } : flags;
    }

    // ── deploy: the sibling script owns cargo, the upload and the readiness poll ──
    const invocation = buildDeployInvocation(childFlags, childEnv);
    line(
        `deploying the hub (${flags.mode === 'local' ? 'cargo here, ship the ELF' : 'ship the source, cargo there'}, ` +
            `${flags.port === undefined ? 'internal port auto' : `internal port ${flags.port}`}) — the child's output follows`,
    );
    const code = await deps.spawn(invocation);
    if (code !== 0) {
        error(
            `${PREFIX} ✗ deploy failed (exit ${code}) — the child printed GET /apps/${APP_NAME}/logs?lines=40 on failure; ` +
                (recycled ? 'the container is the fresh one from --recycle-container, and its registry is empty' : 'the app record above is the state this run left'),
        );
        return code;
    }

    // ── where the app is, from its own record ──
    const ready = await readApp({
        endpoint,
        credentials,
        deps,
        // --no-wait returns before readiness; a 404 there is a real problem, not a
        // cold start, so the ordinary budget still applies (it is usually instant).
        budgetMs: ENDPOINT_WAIT_MS,
    });
    const { hubUrl, note } = buildHubUrl(ready, endpoint);
    if (ready.present) {
        line(`✓ app ${APP_NAME} — ${describeAppView(ready.view)}`);
        line(`  path:             ${ready.view.path}`);
        line(`  internal port:    ${ready.view.internalPort ?? '?'} (loopback only; never part of a URL)`);
    } else {
        line(`! app ${APP_NAME} is not registered after the child exited 0 (--no-wait?)`);
    }
    line(`  hub (clients):    ${hubUrl ?? `not reported (${note})`}`);
    line(`  management:       ${endpoint}/health · /apps · /apps/${APP_NAME} · /apps/${APP_NAME}/logs · DELETE /apps/${APP_NAME}`);
    if (hubUrl === null) line(`  note:             ${note}`);
    log(
        buildSummary({
            mode,
            endpoint,
            hubUrl,
            hubNote: hubUrl === null ? note : undefined,
            ...(ready.present
                ? { status: ready.view.status, revision: ready.view.revision, internalPort: ready.view.internalPort }
                : {}),
            ...(flags.token ? { token: flags.token } : {}),
            ...(replaced ? { replaced } : {}),
            ...(recycled ? { recycled } : {}),
        }),
    );
    return 0;
};

// ══════════════════════════════════════════════════════════════════════════════
//  REAL DEPS + main()
// ══════════════════════════════════════════════════════════════════════════════

/** Error → one line, so a failure never surfaces as a bare [object Object]. */
const describe = (error: unknown): string => (error instanceof Error ? error.message : String(error));

/**
 * The real HTTP dep: fetch normalised so a transport blip, an HTML edge error page and
 * a JSON body all arrive as one ProbeResult (classifyProbe's whole input). Reading the
 * body is inside the same window, so a proxy that sends headers then hangs surfaces as
 * a retryable error rather than as a stack.
 */
export const httpProbe: Probe = async (method, url, { headers, timeoutMs }) => {
    try {
        const response = await fetch(url, { method, headers, signal: AbortSignal.timeout(timeoutMs) });
        const text = await response.text();
        let json: unknown = null;
        try {
            json = JSON.parse(text);
        } catch {
            /* an HTML/edge error page — the caller reads status + text */
        }
        return { status: response.status, ok: response.ok, json, text };
    } catch (cause) {
        return { status: 0, ok: false, json: null, text: '', transportError: describe(cause) };
    }
};

/**
 * The slice of child_process.spawn this module needs, as an injectable seam rather than
 * a vi.mock of the builtin: in this repo's vitest setup a builtin mock rebinds the
 * binding for the TEST FILE but leaves the module under test calling the real builtin,
 * so a mocked spawn would never be seen by the code being pinned. An explicit
 * default-parameter seam is the only way to exercise the child wiring (argv, cwd,
 * stdio, env, exit-code mapping) without starting a cargo build — same reasoning as
 * start-daytona.ts.
 */
export type SpawnChild = { on: (event: 'error' | 'close', listener: (arg?: unknown) => void) => unknown };
export type SpawnImpl = (command: string, args: readonly string[], options: { cwd: string; env: Record<string, string | undefined>; stdio: 'inherit' }) => SpawnChild;

/** stdio INHERIT so the child's progress reaches the terminal untouched; its exit code is ours. */
export const spawnDeploy = (invocation: DeployInvocation, spawnImpl: SpawnImpl = spawn): Promise<number> =>
    new Promise<number>((resolveRun) => {
        const child = spawnImpl(invocation.command, invocation.args, { cwd: invocation.cwd, env: invocation.env, stdio: 'inherit' });
        child.on('error', (cause) => {
            console.error(`${PREFIX} ✗ could not start ${DEPLOY_SCRIPT_NAME}: ${describe(cause)}`);
            resolveRun(1);
        });
        // Signal-killed children report code null (a shim may report undefined);
        // anything not a number becomes 1 so "non-zero = failed" always holds.
        child.on('close', (code) => resolveRun(typeof code === 'number' ? code : 1));
    });

/** The default dep set: the real fetch and the real child process. */
export const createDefaultDeps = (): EndpointDeps => ({ probe: httpProbe, spawn: spawnDeploy });

/**
 * Parses, validates, runs, and maps the result onto an exit code. Returns it instead of
 * calling process.exit, so the flow stays testable; the entry guard below is the only
 * place that touches process state.
 */
export const main = async (argv: readonly string[] = process.argv.slice(2), deps: EndpointDeps = createDefaultDeps()): Promise<number> => {
    const log = deps.log ?? ((text: string) => console.log(text));
    const error = deps.error ?? ((text: string) => console.error(text));
    const { flags, errors } = parseArgs(argv, deps.env ?? process.env);
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
        return await runDeployEndpoint(flags, deps);
    } catch (failure) {
        // EndpointDeployError is always "here is the fix"; anything else is a bug and
        // keeps its stack, so a real defect is not dressed up as operator error.
        if (failure instanceof EndpointDeployError) {
            error(`${PREFIX} ✗ ${failure.message}`);
            return 1;
        }
        error(`${PREFIX} ✗ unexpected failure: ${failure instanceof Error ? (failure.stack ?? failure.message) : String(failure)}`);
        return 1;
    }
};

// ══════════════════════════════════════════════════════════════════════════════
//  ENTRY POINT — importing this module must never start anything
// ══════════════════════════════════════════════════════════════════════════════
//
//  The same three rules as start-daytona.ts and services/daytona/sandbox/
//  setup-sandbox.ts: `yarn deploy:endpoint` must auto-run, and `vitest` importing this
//  file must not. Rule 3 exists because a PLAIN `vite-node <file>` rewrites process.argv
//  and DROPS the file, so neither argv nor the lifecycle line can see the entry, and a
//  stack probe is evidence only inside vite-node's module runner — which is exactly
//  where that gap is.

/** Extension-bearing token that identifies THIS file as the runnable script. */
const ENTRY_SCRIPT_PATTERN = /(?:^|[/\\])deploy-endpoint\.[cm]?[jt]sx?$/;
/** vite/vite-node append module ids and query strings; strip them before matching. */
const stripModuleQuery = (token: string): string => String(token).split('?')[0]!.split('#')[0]!;
const isScriptToken = (token: unknown): boolean => ENTRY_SCRIPT_PATTERN.test(stripModuleQuery(String(token)));

/** Signals the entry detector reads — every field is process state, gathered once at import. */
export type EntryPointSignals = { argv?: readonly string[]; env?: Readonly<Record<string, string | undefined>>; modulePath?: string; stack?: string; vitest?: boolean };

/** Rule 1: argv's SCRIPT POSITIONAL is this file. */
const namesScriptPositional = (argv: readonly string[] | undefined): boolean => {
    const tokens = argv ?? [];
    if (isScriptToken(tokens[1])) return true;
    const flag = tokens.indexOf('--script');
    return flag !== -1 && isScriptToken(tokens[flag + 1]);
};

/** Rule 2: the npm/yarn lifecycle line names it (`deploy:endpoint` = "vite-node scripts/deploy-endpoint.ts"). */
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
        return join(SCRIPTS_DIR, ENTRY_SCRIPT_NAME);
    }
})();

// The stack must be captured during module evaluation; inside main() the evidence for
// rule 3 is already gone.
if (isCliEntryPoint({ argv: process.argv, env: process.env, modulePath: MODULE_FILE_PATH, stack: new Error().stack, vitest: Boolean(process.env.VITEST) })) {
    main().then(
        (code) => {
            process.exitCode = code;
        },
        (failure) => {
            console.error(`${PREFIX} ✗ unexpected failure: ${failure instanceof Error ? (failure.stack ?? String(failure)) : String(failure)}`);
            process.exitCode = 1;
        },
    );
}
