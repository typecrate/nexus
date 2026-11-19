// ══════════════════════════════════════════════════════════════════════════════
//  deploy-endpoint.ts — DEPLOY THE NEXUS HUB TO ANY RUNTIME ENDPOINT YOU HAND IT,
//  TEARING DOWN WHATEVER IS ALREADY THERE FIRST — and, on a Modal box that holds a
//  runtime, REPLACING THE CONTAINER itself so the deploy lands on a clean machine
//
//      yarn workspace @typecrate/nexus deploy:endpoint https://<your-endpoint>
// ══════════════════════════════════════════════════════════════════════════════
//
//  WHAT THIS DOES:
//
//      GET /health → GET /status → [DELETE /sandbox if a Modal box holds a runtime] →
//      (wait for it)  (what holds    (replace the CONTAINER; wait for the replacement,
//                       the slot?)     then prove its slot is empty)
//                   → [DELETE / for any other busy slot] → spawn deploy-daytona.ts
//                     (cargo build + POST the bundle + poll /status)
//
//  IT IS PLATFORM-AGNOSTIC ON PURPOSE. The only thing it depends on is the ENDPOINT
//  CONTRACT v1 (services/daytona/sandbox/runtime-server.mjs is its source of truth),
//  which every runtime this repo boots implements — a Daytona sandbox preview URL, a
//  Modal box's `…--container-sandbox.modal.run` web-function URL, a Modal sandbox's
//  tunnel URL, or the stdlib runner on loopback. So the URL is an ARGUMENT, not a
//  constant, and nothing here CREATES a platform object. The one platform rule it owns
//  is the Modal container reset below, and that too is decided from the endpoint's own
//  /health (`service`) plus its host — never from a flag the caller has to know.
//  Ask the endpoint, act on its answer, ship the bytes.
//
//  WHY IT EXISTS ALONGSIDE deploy-daytona.ts (which it drives, as a child):
//    deploy-daytona.ts already speaks POST/poll/logs — and deliberately refuses to
//    resolve a busy slot: it prints "run with --teardown first" and exits 1
//    (deploy-daytona.ts:24-27), because silently killing a hub someone else is using
//    is worse than one extra flag. That is right when the endpoint is a sandbox you
//    just created (start-daytona.ts's fresh mode: the slot is empty by construction)
//    and wrong when you point at a long-lived endpoint you own, where "yesterday's
//    hub is still holding the slot" is simply the normal case. This script is that
//    other posture: replace-on-arrival, by default, with `--no-teardown` to opt back
//    into the strict behaviour.
//
//      fresh sandbox  →  start:daytona   (creates the sandbox; slot is already empty)
//      any endpoint   →  deploy:endpoint (preflights, replaces the slot, deploys)  ← HERE
//      ship only      →  deploy:daytona  (POST + poll; a busy slot is an error)
//      stop it        →  deploy:endpoint <url> --teardown   (DELETE / — frees the slot)
//
//  TEARDOWN SEMANTICS, AND THE MODAL CONTAINER RESET:
//    DELETE / is the contract's idempotent teardown: it kills the runtime's process
//    group, removes its dir, and frees the slot — it never touches the container, so
//    the endpoint keeps serving and is ready for the next POST. On Daytona that is the
//    whole job, because the container is created and owned elsewhere.
//
//    On a Modal BOX the container is the trust boundary, so the deploy ASKS the endpoint
//    what it holds and acts on that answer (GET /health's `service` marker plus a
//    `*.modal.run` host are what identify it as a Modal box):
//      · slot EMPTY — nothing is running in the container, so it is taken as the fresh
//        machine the deploy should use. Nothing is killed; the hub is POSTed into it.
//      · slot BUSY  — DELETE /sandbox ends the runner and the container with it, Modal
//        relaunches a fresh container at the SAME stable URL (MIN_CONTAINERS = 1), and
//        the hub is POSTed into the replacement. The replacement is accepted only when
//        its /status reads empty AND its /health uptime is younger than the time since
//        the delete — the old container's dying answers cannot pass that test, and a
//        reset that cannot be proven refuses to deploy.
//
//    The reset follows the replace posture: default ON, and OFF with --no-teardown
//    (which keeps its meaning — touch nothing that is not yours). A BUSY Modal SANDBOX
//    tunnel (`*.modal.host`) is refused with the fix, because its URL dies with the
//    container and there is no stable address to relaunch into; an empty sandbox is
//    used as supplied. --teardown keeps its narrow meaning: free the slot, ship
//    nothing, reset nothing.
//
//  AUTH: one flag, both platforms. `--token` is sent as `x-daytona-preview-token` on
//    /health, /status, DELETE and POST — the header Daytona's private preview proxy
//    requires (deploy-daytona.ts:17-21) and the one the Modal runner accepts too. An
//    endpoint with no secret simply ignores it, so omitting --token keeps public
//    endpoints and the loopback stub working (no header is sent at all).
//
//  THE HUB'S OWN URL IS REPORTED, NOT ASSUMED. After the deploy, GET /status'
//    `runtime.preview.url` is printed as `hubUrl`:
//      · a Daytona runner reports the preview link of the runtime's port when its
//        driver pushed one (start-daytona.ts mints it with getPreviewLink instead);
//      · a Modal BOX reports its own web-function URL — a box forwards every
//        non-contract path to the ready runtime's port, so management URL and hub
//        URL are the SAME URL there (one port is all a box has);
//      · an endpoint that reports no preview URL yields `"hubUrl": null` plus the
//        runner's own `preview.warning`, and the fallback (the endpoint itself) is
//        printed as a note. Guessing a URL by swapping the port in would be a lie:
//        Daytona's per-port link carries its own token, which is not derivable.
//
//  WHY THE PREFLIGHT WAITS (not a Daytona/Modal special case — just cold starts):
//    a freshly created or recently recycled container answers 502/503 or drops the
//    connection until its runner binds the port, so a miss is EXPECTED and is retried
//    for ENDPOINT_WAIT_MS. Two failures are distinguished from it because they must
//    not be retried: 401/403 (a token problem) and 404 / a non-contract body (wrong
//    URL, or an app that is not deployed). Both abort immediately with the fix.
//
//  RUN VIA (package.json; `vite-node` is root-hoisted, house convention):
//      yarn workspace @typecrate/nexus start:modal
//      # ^ the vault's Modal box (@config/vault → Object.values(modalContainer)[0]); a BUSY
//      #   slot replaces the CONTAINER first (the POST lands in the replacement), and
//      #   an empty slot is used as the fresh machine
//      yarn workspace @typecrate/nexus deploy:endpoint https://<modal-box-url>
//      # ^ the same flow, for an endpoint you name yourself
//      yarn workspace @typecrate/nexus deploy:endpoint https://abc123.modal.host --token s3cret
//      # ^ a Modal SANDBOX tunnel: an empty slot is deployed into as supplied; a BUSY one
//      #   is refused, because the tunnel URL dies with its container (--teardown frees it)
//      yarn workspace @typecrate/nexus deploy:endpoint https://<sandbox-preview-url> --env NEXUS_TOKEN=xyz
//      yarn workspace @typecrate/nexus deploy:endpoint <url> --mode remote-build
//      yarn workspace @typecrate/nexus deploy:endpoint <url> --no-teardown   # busy slot = error, no reset
//      yarn workspace @typecrate/nexus deploy:endpoint <url> --teardown      # free it, deploy nothing
//      yarn workspace @typecrate/nexus deploy:endpoint <url> --dry-run       # the plan, zero network
//    Flags after the URL are the deploy passthrough; every one of them is
//    deploy-daytona.ts's own spelling, so nothing here invents a flag name.
//
//  MACHINE-READABLE OUTPUT: after a successful run the LAST line of stdout is exactly
//  ONE single-line JSON object (nothing else is on that line; wrapped here to fit):
//      {"mode":"deploy","endpoint":"https://…","hubUrl":"https://…","hubPort":8085}
//    plus `"token":"…","tokenScope":"management"` when one was used (it authenticates
//    the RUNNER's routes; whether a hub client needs a credential depends on the hub's
//    own --env, not on this), plus `"replaced":{…}` when this run tore a runtime down,
//    plus `"recycled":"box"` when this run replaced the Modal CONTAINER, plus
//    `"hubNote":"…"` when the endpoint reported no preview URL. `--teardown`
//    emits {"mode":"teardown","endpoint":"…","slot":"empty",…}. A dry run emits none.
//
//  IMPORT IS A NO-OP: everything meaningful is an exported pure function over an
//  explicit input (parseArgs / resolveMode / classifyProbe / decideTeardown /
//  buildHubUrl / buildDeployArgs / buildSummary), and ALL I/O is in deps the caller
//  injects (probe / spawn / sleep / now / log / error / env). The entry guard at the
//  bottom is the only thing that touches process state, and it never fires under
//  vitest. The colocated deploy-endpoint.test.ts drives the whole flow with fakes —
//  zero network, no cargo.
// ══════════════════════════════════════════════════════════════════════════════

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

// ══════════════════════════════════════════════════════════════════════════════
//  PINS — resolved from THIS module (never from cwd), as start-daytona.ts does
// ══════════════════════════════════════════════════════════════════════════════

/** The zero-dependency sibling this script spawns; it owns the POST/poll contract. */
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
 * The child is a .ts file, and raw `node` is both forbidden by AGENTS.md and unable to
 * load one in this Node build — so it is started as `process.execPath <vite-node cli.mjs>
 * <script>`, the exact shape start-daytona.ts uses for the same reason.
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

/** Hub port default — MUST equal DEFAULT_PORT in deploy-daytona.ts (8085). */
export const DEFAULT_HUB_PORT = 8085;
/** Every line this script writes is prefixed, so it greps apart from the child's `>>>`/`✓`. */
const PREFIX = '[nexus:endpoint]';
/** The runner's identity marker; GET /health is the one route that always carries it. */
const CONTRACT_ID = 'daytona-runtime-v1';
/**
 * The Modal runner's own /health marker. The Daytona twin reports no `service` field,
 * so this is what separates "a Modal container we may replace" from "someone else's
 * endpoint we must not touch" (see the container-reset note in the header).
 */
const MODAL_RUNNER_SERVICE = 'modal-container-runtime';
/** Modal's two hostname shapes: a box URL is a stable deployment, a sandbox tunnel is not. */
const MODAL_BOX_HOST = /(?:^|\.)modal\.run$/i;
const MODAL_SANDBOX_HOST = /(?:^|\.)modal\.host$/i;
/**
 * How long to wait for an endpoint to answer AT ALL. A container that is new, recycled
 * or scaled to zero answers 502/503 (or drops the connection) until its runner binds
 * the port, so the first misses are expected, not failures. Generous enough for a cold
 * start, short enough that a dead URL is not a 15-minute mystery.
 */
export const ENDPOINT_WAIT_MS = 90_000;
/** How long to wait for DELETE / to be reflected in the slot. Teardown is synchronous per contract, so this is a proof fence, not a budget. */
export const SLOT_WAIT_MS = 30_000;
/**
 * How long a BUSY Modal box gets to become a fresh, empty container: DELETE /sandbox
 * answers after the old runtime is torn down, Modal then relaunches the container and
 * the replacement binds its runner. A proof fence for the swap, not a cold-start budget.
 */
export const RECYCLE_WAIT_MS = 60_000;
/**
 * How much clock skew to forgive when a replacement's own `uptimeSec` is compared with
 * the time elapsed since DELETE /sandbox. The two clocks are independent (this process
 * and Modal's container), and the decision the margin feeds is only "did this process
 * start after the delete" — a process that predates it reports an uptime older than the
 * elapsed time by its whole earlier life, which is far more than this window.
 */
const FRESH_UPTIME_SLACK_MS = 15_000;
/** Poll cadence and per-request timeout — both mirror deploy-daytona.ts's own numbers. */
const POLL_MS = 2_000;
const REQUEST_TIMEOUT_MS = 15_000;
/**
 * DELETE /sandbox is heavier than DELETE /: the runner tears the runtime down FIRST
 * (stopSignal → up to 10 s grace → SIGKILL → up to 10 s more → log drain → rmtree) and
 * only then answers, so the ordinary request ceiling would report a transport timeout
 * on a reset that is proceeding exactly as designed.
 */
const RESET_TIMEOUT_MS = 60_000;

// ══════════════════════════════════════════════════════════════════════════════
//  USAGE
// ══════════════════════════════════════════════════════════════════════════════

export const USAGE = `usage: yarn workspace @typecrate/nexus deploy:endpoint <url> [flags]
       (or: vite-node scripts/deploy-endpoint.ts <url> [flags])

  deploys the nexus hub into ANY endpoint that speaks the runtime contract v1 —
  a Daytona sandbox preview URL, a Modal box or sandbox URL, a local runner —
  replacing whatever runtime already holds the slot, then prints the hub's URL.
  On a Modal BOX the container itself is REPLACED first (see below).

  the endpoint (required; no platform default, that is the point):
    <url>                    positional, or --endpoint <url>
                             (env fallbacks: NEXUS_ENDPOINT, NEXUS_DAYTONA_ENDPOINT)
    --token <t>              sent as the x-daytona-preview-token header on /health,
                             /status, DELETE and POST — Daytona's preview proxy needs
                             it, the Modal runner accepts it, a public endpoint ignores
                             it. (env: NEXUS_ENDPOINT_TOKEN, NEXUS_DAYTONA_PREVIEW_TOKEN)

  deploy (forwarded verbatim to scripts/deploy-daytona.ts):
    --mode <m>        local (default: compile here, ship the nexus-hub ELF)
                      remote-build (ship the source tarball; cargo runs there)
    --port <n>        port the hub listens on inside the container (default ${DEFAULT_HUB_PORT})
    --env K=V         extra runtime env var; repeatable
    --no-build        ship an already-built artifact instead of running cargo
    --artifact <path> ship exactly this file (implies --no-build)
    --no-wait         return once the POST is accepted, without polling /status

  slot policy:
    (default)         if GET /status shows a busy slot: DELETE /, wait for "empty",
                      then deploy — i.e. this command REPLACES the running hub
    --no-teardown     never replace: a busy slot is an error (deploy-daytona's 409
                      posture), for when the running hub is not yours to kill
    --teardown        DELETE / only — free the slot, ship nothing (no cargo, no build)

  container reset (Modal only; the default with the replace posture):
    the endpoint's OWN slot decides, and a Modal runner is recognised by GET /health's
    service marker plus a platform host:
      slot empty — nothing is running in the container, so it is taken as the fresh
                   machine; nothing is killed, and the hub is POSTed into it
      slot busy  — DELETE /sandbox ends the runner and the container with it, Modal
                   relaunches the container at the SAME URL, and the deploy waits for
                   the replacement: a NEW process (a young /health uptime, or an answer
                   gap proving the old one left) whose /status reads empty. Only then
                   is the hub POSTed into it; a reset that cannot be proven deploys
                   nothing.
    --no-teardown turns the reset off, leaving its own rule in force (a busy slot is an
    error). A BUSY Modal SANDBOX tunnel (…modal.host) is refused with the fix: its URL
    dies with the container, so it has no stable address to relaunch into (free its
    slot with --teardown, then deploy again). --teardown never resets a container.

  behavior:
    --dry-run         print the plan; no probe, no DELETE, no child, no cargo
    -h, --help        this text

  the hub URL afterwards is read from the endpoint's own GET /status → runtime.preview.url
  (a Modal box reports its own URL, because it forwards every non-contract path to the
  hub's port; a Daytona sandbox reports its port's preview link when its driver pushed
  one, and otherwise prints null — mint it with getPreviewLink(port), i.e. start:daytona).`;

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
    /** true = replace whatever holds the slot (the default); --no-teardown flips it. */
    replace: boolean;
    endpoint: string;
    token: string;
    mode: 'local' | 'remote-build';
    port: number;
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
        // The existing daytona env names stay valid; the platform-neutral ones win.
        endpoint: env.NEXUS_ENDPOINT ?? env.NEXUS_DAYTONA_ENDPOINT ?? '',
        token: env.NEXUS_ENDPOINT_TOKEN ?? env.NEXUS_DAYTONA_PREVIEW_TOKEN ?? '',
        mode: 'local',
        port: DEFAULT_HUB_PORT,
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
        if (flag === '--endpoint' || flag === '--token' || flag === '--mode' || flag === '--port' || flag === '--env' || flag === '--artifact') {
            const value = argv[++i];
            if (value === undefined || value.startsWith('--')) {
                errors.push(`${flag} needs a value`);
                continue;
            }
            if (flag === '--endpoint') flags.endpoint = value;
            else if (flag === '--token') flags.token = value;
            else if (flag === '--artifact') flags.artifact = value;
            else if (flag === '--mode') flags.mode = value as EndpointFlags['mode'];
            else if (flag === '--port') {
                const port = Number(value);
                if (!Number.isInteger(port) || port < 1 || port > 65535) errors.push(`--port ${JSON.stringify(value)} is out of range (TCP port 1-65535)`);
                else flags.port = port;
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
 * Base-relative join that PRESERVES a path prefix. Daytona's preview proxy hands out
 * URLs with path segments; a Modal box is host-rooted. `new URL('https://x/y', base)`
 * would clobber the prefix, so the suffix is appended to the existing pathname instead
 * — identical in behaviour to deploy-daytona.ts's joinUrl, because the two must POST to
 * exactly the same target.
 */
export const joinUrl = (base: string, suffix: string): string => {
    const url = new URL(base);
    url.pathname = `${url.pathname.endsWith('/') ? url.pathname : `${url.pathname}/`}${suffix}`;
    return url.toString();
};

/** Empty token → NO header at all: that is what keeps public endpoints working. */
export const authHeaders = (token: string): Record<string, string> => (token ? { 'x-daytona-preview-token': token } : {});

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
 * What one probe means, decided PURELY so the cold-start/auth/404 table is testable
 * without a clock or a network:
 *   'ready' — 2xx with a JSON body (and, for the identity check, the contract marker)
 *   'retry' — no reply, or 408/429/5xx: the container is not serving yet (Modal answers
 *             502/503 until a web-function container holds the port; a Daytona proxy
 *             resets while the sandbox is still starting). Normal → wait it out.
 *   'auth'  — 401/403: something IS there, but the token is missing or wrong. Retrying
 *             cannot fix that, so it aborts with the fix instead of burning the budget.
 *   'dead'  — any other non-2xx, or a 2xx that is not the runner: no such endpoint (a
 *             typo'd Modal label, a stopped app, an expired Daytona sandbox), or a
 *             DIFFERENT service answering on that URL. Retrying that is pure waiting.
 */
export type ProbeVerdict = 'ready' | 'retry' | 'auth' | 'dead';
export const classifyProbe = (result: ProbeResult, options: { requireContract?: boolean } = {}): ProbeVerdict => {
    if (result.transportError) return 'retry';
    if (result.status === 401 || result.status === 403) return 'auth';
    if (result.status >= 500 || result.status === 408 || result.status === 429) return 'retry';
    if (!result.ok) return 'dead';
    if (typeof result.json !== 'object' || result.json === null) return 'dead';
    if (options.requireContract && (result.json as { contract?: string }).contract !== CONTRACT_ID) return 'dead';
    return 'ready';
};

/** Human label for one probe outcome, reused by both the wait narration and its errors. */
const describeProbe = (result: ProbeResult): string => result.transportError ?? `HTTP ${result.status}`;

/**
 * Waits for one route to answer and returns the parsed body, aborting immediately on
 * 'auth' and 'dead'. Bounded by BOTH the clock and the attempt count — the attempt
 * count is what makes it testable with a frozen clock, and it also stops a hung proxy
 * from turning a poll loop into an unresponsive terminal.
 */
export const waitForRoute = async (input: {
    method: 'GET' | 'DELETE';
    url: string;
    token: string;
    what: string;
    budgetMs: number;
    /** GET /health only: prove the URL really is a runtime runner before acting on it. */
    requireContract?: boolean;
    deps: EndpointDeps;
}): Promise<ProbeResult> => {
    const { method, url, token, what, budgetMs, requireContract, deps } = input;
    const log = deps.log ?? ((text: string) => console.log(text));
    const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((done) => setTimeout(done, ms)));
    const now = deps.now ?? Date.now;
    const deadline = now() + budgetMs;
    const maxAttempts = Math.max(1, Math.ceil(budgetMs / POLL_MS));
    let attempts = 0;
    for (;;) {
        const result = await deps.probe(method, url, { headers: authHeaders(token), timeoutMs: REQUEST_TIMEOUT_MS });
        const verdict = classifyProbe(result, { requireContract });
        if (verdict === 'ready') return result;
        if (verdict === 'auth')
            throw new EndpointDeployError(
                `${url} → 401 — this endpoint guards its contract routes, and ${token ? 'this token was rejected' : 'no token was given'}. ` +
                    `Pass --token <the endpoint's secret> (or set NEXUS_ENDPOINT_TOKEN); it is sent as x-daytona-preview-token.`,
            );
        if (verdict === 'dead')
            throw new EndpointDeployError(
                `${url} → ${describeProbe(result)} — that is not a ${CONTRACT_ID} runner. Check the URL (a Daytona preview link, a Modal \`…--<label>.modal.run\` or \`…<id>…modal.host\` tunnel) ` +
                    `and that the container is still up: a stopped app or an expired sandbox answers exactly like this.`,
            );
        attempts += 1;
        if (attempts === 1) log(`${PREFIX} ${what} is not answering yet (${describeProbe(result)}) — waiting up to ${Math.round(budgetMs / 1000)}s while the container comes up…`);
        if (now() >= deadline || attempts >= maxAttempts)
            throw new EndpointDeployError(`${what} never answered at ${url} (last: ${describeProbe(result)}, ${attempts} tries over ${Math.round(budgetMs / 1000)}s). Follow the container's own log, then retry.`);
        await sleep(POLL_MS);
    }
};

/** The runner's own state, as reported by GET /status. */
export type RunnerState = { slot: string; runtime: Record<string, unknown> | null; raw: Record<string, unknown> };

/** Reads GET /status, waiting out a cold start and refusing a URL that is not a runner. */
export const readState = async (endpoint: string, token: string, deps: EndpointDeps): Promise<RunnerState> => {
    const result = await waitForRoute({ method: 'GET', url: joinUrl(endpoint, 'status'), token, what: 'GET /status', budgetMs: ENDPOINT_WAIT_MS, deps });
    const body = (result.json ?? {}) as Record<string, unknown>;
    return { slot: String(body.slot ?? 'unknown'), runtime: (body.runtime ?? null) as Record<string, unknown> | null, raw: body };
};

/**
 * THE slot policy, as a pure table so every branch is pinned:
 *   empty          → nothing to do
 *   busy/unknown   → replace (the default) or abort (--no-teardown)
 * 'unknown' counts as busy deliberately: a runner reporting a slot this script does not
 * recognise is a runner whose state cannot be trusted, and deploying blind would risk a
 * mid-flight 409 with a shipped bundle already paid for.
 */
export type TeardownDecision = { action: 'none' | 'teardown' | 'abort'; why: string };
export const decideTeardown = (slot: string, replace: boolean): TeardownDecision => {
    if (slot === 'empty') return { action: 'none', why: 'the slot is already empty' };
    if (replace) return { action: 'teardown', why: `the slot holds a ${slot} runtime, and replacing it is this command's default` };
    return { action: 'abort', why: `the slot holds a ${slot} runtime and --no-teardown forbids replacing it` };
};

/** One-line identity of the runtime being replaced, so the destructive step is never anonymous. */
export const describeRuntime = (runtime: Record<string, unknown> | null): string => {
    if (!runtime) return 'runtime';
    const preview = (runtime.preview ?? {}) as { url?: unknown };
    const parts = [`profile=${String(runtime.profile ?? '?')}`, `port=${String(runtime.port ?? '?')}`, `runtimeId=${String(runtime.runtimeId ?? runtime.id ?? '?')}`];
    if (runtime.startedAtMs) parts.push(`since=${new Date(Number(runtime.startedAtMs)).toISOString()}`);
    if (preview.url) parts.push(`serving=${String(preview.url)}`);
    return parts.join(' ');
};

/**
 * DELETE / and then PROVE the slot is empty. The contract's teardown is synchronous —
 * it answers {tornDown:true} only after killing the process group and removing the
 * runtime dir — so this normally passes on the first poll; the check is what makes the
 * child's POST a deploy rather than a 409, and it catches a runner that accepts the
 * DELETE and then never frees the slot.
 */
export const teardownSlot = async (endpoint: string, token: string, deps: EndpointDeps): Promise<void> => {
    const url = joinUrl(endpoint, '');
    const result = await waitForRoute({ method: 'DELETE', url, token, what: 'DELETE /', budgetMs: SLOT_WAIT_MS, deps });
    const body = (result.json ?? {}) as { tornDown?: unknown };
    if (body.tornDown !== true) throw new EndpointDeployError(`${url} did not confirm the teardown: ${JSON.stringify(body).slice(0, 200) || result.text.slice(0, 200) || describeProbe(result)}`);
    const state = await readState(endpoint, token, deps);
    if (state.slot !== 'empty') throw new EndpointDeployError(`DELETE / was accepted but the slot still reads "${state.slot}" — inspect GET ${joinUrl(endpoint, 'status')}`);
};

// ══════════════════════════════════════════════════════════════════════════════
//  MODAL CONTAINER RESET — a deploy on a Modal box starts from a FRESH container
// ══════════════════════════════════════════════════════════════════════════════
//
//  Separate from teardownSlot on purpose: DELETE / frees the SLOT, and the container
//  keeps serving with everything still inside it. On Modal the container is the thing
//  that must not survive a deploy — the previous bundle, its build output and anything
//  else dropped on that filesystem all live in it — and DELETE /sandbox is the
//  Modal-only contract addition that ends the runner. Modal then relaunches the
//  container at the box's STABLE URL, which is why the deploy can follow it.

/** Which replaceable Modal container an endpoint is. */
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
 *   · any other answer (401/403 included) → fail closed. Deploying on a guess is
 *     exactly what this reset exists to prevent.
 */
export const requestModalContainerReset = async (endpoint: string, token: string, deps: EndpointDeps): Promise<void> => {
    const url = joinUrl(endpoint, 'sandbox');
    const result = await deps.probe('DELETE', url, { headers: authHeaders(token), timeoutMs: RESET_TIMEOUT_MS });
    if (result.transportError) return;
    if (result.status === 401 || result.status === 403)
        throw new EndpointDeployError(
            `${url} → ${result.status} — the container reset is a contract route, and ${token ? 'this token was rejected' : 'no token was given'}. ` +
                `Pass --token <the endpoint's secret> (or set NEXUS_ENDPOINT_TOKEN); it is sent as x-daytona-preview-token.`,
        );
    if (result.status !== 200)
        throw new EndpointDeployError(
            `${url} → HTTP ${result.status}${result.text ? `: ${result.text.slice(0, 200)}` : ''} — without DELETE /sandbox the container cannot be replaced, and this deploy refuses to land in a machine that was never reset.`,
        );
    const body = (result.json ?? {}) as { tornDown?: unknown };
    if (body.tornDown !== true) throw new EndpointDeployError(`${url} did not confirm the container reset: ${JSON.stringify(body).slice(0, 200) || result.text.slice(0, 200) || describeProbe(result)}`);
};

/**
 * Did a /health answer come from a process that started at or after the reset?
 *
 * This is the only per-process identity the contract exposes (`uptimeSec`), and it is
 * read on the CONTAINER's own clock — so the comparison is against the time elapsed
 * since DELETE /sandbox was sent, with a slack window for clock skew. A process that
 * predates the delete reports an uptime older than that elapsed time by its whole
 * earlier life, which is what keeps the old container's dying answers from being
 * mistaken for the replacement (the failure mode measured on a real box: the
 * replacement comes up fast enough that every poll already sees its 200).
 */
const startedAfterReset = (health: unknown, resetAtMs: number, nowMs: number): boolean => {
    const uptimeSec = Number((health as Record<string, unknown> | null)?.uptimeSec);
    if (!Number.isFinite(uptimeSec)) return false;
    return uptimeSec * 1000 <= nowMs - resetAtMs + FRESH_UPTIME_SLACK_MS;
};

/**
 * Wait for the REPLACEMENT of a busy Modal box and hand its /status back.
 *
 * Two facts have to agree, because "it answered 200" is not enough on this platform:
 *   · the process is NEW — its /health uptime is younger than the elapsed time since the
 *     delete (see `startedAfterReset`), or the endpoint went non-2xx in between, which
 *     proves the old one left;
 *   · the container is EMPTY — /status reads `empty`, so no runtime survived into it.
 * The loop is bounded by RECYCLE_WAIT_MS and fails closed: a reset that cannot be
 * proven is reported, never papered over with a deploy into an unknown machine.
 */
export const waitForModalContainerReset = async (endpoint: string, token: string, deps: EndpointDeps, resetAtMs: number): Promise<RunnerState> => {
    const log = deps.log ?? ((text: string) => console.log(text));
    const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((done) => setTimeout(done, ms)));
    const now = deps.now ?? Date.now;
    const healthUrl = joinUrl(endpoint, 'health');
    const statusUrl = joinUrl(endpoint, 'status');
    const deadline = now() + RECYCLE_WAIT_MS;
    const maxAttempts = Math.max(1, Math.ceil(RECYCLE_WAIT_MS / POLL_MS));
    let oldGone = false;
    let last = 'no answer yet';
    for (let attempt = 1; ; attempt++) {
        const health = await deps.probe('GET', healthUrl, { headers: authHeaders(token), timeoutMs: REQUEST_TIMEOUT_MS });
        if (!health.ok) {
            // No 2xx: nothing is serving a runner at this URL right now, so the old
            // container is gone and whatever answers next is the replacement.
            oldGone = true;
            last = describeProbe(health);
        } else if (oldGone || startedAfterReset(health.json, resetAtMs, now())) {
            const status = await deps.probe('GET', statusUrl, { headers: authHeaders(token), timeoutMs: REQUEST_TIMEOUT_MS });
            if (status.ok) {
                const body = (status.json ?? {}) as Record<string, unknown>;
                const slot = String(body.slot ?? 'unknown');
                if (slot === 'empty') return { slot, runtime: (body.runtime ?? null) as Record<string, unknown> | null, raw: body };
                last = `a fresh container whose slot reads "${slot}"`;
            } else {
                last = `GET /status → ${describeProbe(status)}`;
            }
        } else {
            last = 'the old container is still answering';
        }
        if (attempt === 1) log(`${PREFIX} waiting for the replacement container (up to ${Math.round(RECYCLE_WAIT_MS / 1000)}s) — it must answer with a young uptime and an empty slot`);
        if (now() >= deadline || attempt >= maxAttempts)
            throw new EndpointDeployError(`${endpoint} never reported a fresh, empty container after DELETE /sandbox (last: ${last}) — nothing was deployed. Check \`modal app logs\`, then retry.`);
        await sleep(POLL_MS);
    }
};

/**
 * The whole Modal box reset: kill the container, wait for the replacement Modal
 * relaunches at the same URL, and hand back its (empty) /status.
 */
export const resetModalBoxContainer = async (endpoint: string, token: string, deps: EndpointDeps): Promise<RunnerState> => {
    const log = deps.log ?? ((text: string) => console.log(text));
    const resetAtMs = (deps.now ?? Date.now)();
    await requestModalContainerReset(endpoint, token, deps);
    const state = await waitForModalContainerReset(endpoint, token, deps, resetAtMs);
    log(`${PREFIX} ✓ replacement container is up — the deploy lands in THIS container`);
    return state;
};

/**
 * Where the hub actually is once it is ready, decided from the endpoint's OWN report
 * rather than from a platform guess:
 *   · preview.url present → that is the answer (a Modal box gives its own URL, because
 *     it forwards every non-contract path; a Daytona runner gives its port's link);
 *   · absent              → hubUrl stays null and the runner's own `preview.warning`
 *     (or the platform-specific remedy) becomes `note`, so a human and a machine both
 *     get the truth: the deploy succeeded, the public URL of the SERVICE is simply not
 *     something this endpoint can name.
 */
export const buildHubUrl = (state: RunnerState, endpoint: string): { hubUrl: string | null; note: string } => {
    const preview = ((state.runtime ?? {}).preview ?? {}) as { url?: unknown; warning?: unknown; exposed?: unknown };
    if (typeof preview.url === 'string' && preview.url) {
        const same = preview.url.replace(/\/+$/, '') === endpoint.replace(/\/+$/, '');
        return { hubUrl: preview.url, note: same ? 'the endpoint itself serves the hub (it forwards every non-contract path)' : 'the runtime port has its own preview URL' };
    }
    if (typeof preview.warning === 'string' && preview.warning) return { hubUrl: null, note: preview.warning };
    return { hubUrl: null, note: `the endpoint reported no preview URL for port ${String((state.runtime ?? {}).port ?? '?')}` };
};

// ══════════════════════════════════════════════════════════════════════════════
//  CHILD INVOCATION — the deploy itself belongs to deploy-daytona.ts
// ══════════════════════════════════════════════════════════════════════════════

/**
 * The exact argv handed to the child, in a fixed, documented order:
 *   --endpoint, --token?, --mode, --port, --env…, --no-build?, --artifact?, --no-wait?
 * `--token` is omitted when empty so the child sends no auth header at all, which is
 * what keeps an unauthenticated endpoint working (deploy-daytona.ts's own rule).
 */
export const buildDeployArgs = (flags: EndpointFlags): string[] => {
    const args = ['--endpoint', flags.endpoint];
    if (flags.token) args.push('--token', flags.token);
    args.push('--mode', flags.mode, '--port', String(flags.port));
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
 * ONE line of JSON, keys in insertion order. `hubUrl: null` means "this endpoint does
 * not name the service's public URL" (see buildHubUrl) and is always paired with a
 * `hubNote`, mirroring how start-daytona.ts keeps the key present rather than omitting
 * it, so a consumer must handle null instead of guessing. `token` is emitted only when
 * one was used, and `tokenScope` says what it authenticates: the RUNNER's routes, not
 * the hub's — whether a hub client needs a credential depends on the hub's own --env.
 */
export const buildSummary = (input: {
    mode: EndpointMode;
    endpoint: string;
    hubUrl?: string | null;
    hubPort?: number;
    hubNote?: string;
    slot?: string;
    token?: string;
    replaced?: { slot: string; profile?: unknown; port?: unknown };
    recycled?: ModalContainerShape;
}): string =>
    JSON.stringify({
        mode: input.mode,
        endpoint: input.endpoint,
        ...(input.hubUrl === undefined ? {} : { hubUrl: input.hubUrl }),
        ...(input.hubPort === undefined ? {} : { hubPort: input.hubPort }),
        ...(input.hubNote ? { hubNote: input.hubNote } : {}),
        ...(input.slot === undefined ? {} : { slot: input.slot }),
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
    /** Clock for the two wait budgets. Default Date.now. */
    now?: () => number;
};

/**
 * Runs the whole flow and returns the process exit code (0 = hub is serving / plan
 * printed). Never calls process.exit, so a caller can wrap it.
 */
export const runDeployEndpoint = async (flags: EndpointFlags, deps: EndpointDeps): Promise<number> => {
    const log = deps.log ?? ((text: string) => console.log(text));
    const error = deps.error ?? ((text: string) => console.error(text));
    const env = deps.env ?? process.env;
    const line = (text: string) => log(`${PREFIX} ${text}`);
    const mode = resolveMode(flags);
    const endpoint = requireEndpoint(flags.endpoint);
    const { token } = flags;
    const childEnv = { ...env, NEXUS_DAYTONA_ENDPOINT: endpoint, ...(token ? { NEXUS_DAYTONA_PREVIEW_TOKEN: token } : {}) };

    // ── dry run: the plan, and provably nothing else (no probe, no child) ──
    if (flags.dryRun) {
        line('DRY RUN — no probe, no DELETE, no child process, no cargo, no network');
        line(`endpoint: ${endpoint}`);
        line(`  preflight: GET ${joinUrl(endpoint, 'health')}  →  GET ${joinUrl(endpoint, 'status')}  →  on a BUSY slot: DELETE /sandbox on a Modal box (the container is replaced) or DELETE / anywhere else`);
        line(`  auth:      x-daytona-preview-token: ${token ? `*** (length ${token.length})` : '(not sent — public endpoint)'}`);
        if (mode === 'teardown') {
            line('mode: teardown — frees the slot and stops here. The container keeps serving; this command never calls DELETE /sandbox, so nothing is recycled.');
            return 0;
        }
        line(`mode: deploy (${flags.mode}) — hub listens on port ${flags.port}${flags.replace ? ', and a busy slot is replaced' : ', and a busy slot is an error (--no-teardown)'}`);
        line(
            flags.replace
                ? '  modal:     the SLOT decides — empty: the container is used as the fresh machine; busy: DELETE /sandbox replaces it, and the deploy waits for a new, empty container before POSTing; a busy sandbox tunnel is refused, because its URL dies with the container'
                : '  modal:     --no-teardown — no container reset: a busy slot is an error, and a Modal sandbox is accepted as supplied',
        );
        line(`deploy command that WOULD run (cwd ${PACKAGE_ROOT}):`);
        line(`  ${buildDeployInvocation(flags, childEnv).args.join(' ')}`);
        line('hub URL after success: read from GET /status → runtime.preview.url (never guessed)');
        return 0;
    }

    // ── preflight: is that URL a runner, and what holds its slot? ──
    line(`probing ${endpoint} …`);
    const health = (await waitForRoute({ method: 'GET', url: joinUrl(endpoint, 'health'), token, what: 'GET /health', budgetMs: ENDPOINT_WAIT_MS, requireContract: true, deps })).json as Record<string, unknown>;
    line(`✓ runtime runner — contract ${String(health.contract ?? CONTRACT_ID)}, runner port ${String(health.runnerPort ?? '?')}, auth ${health.authenticated ? 'token' : 'none'}, exposed ports ${JSON.stringify(health.exposedPorts ?? [])}`);

    const state = await readState(endpoint, token, deps);
    line(`slot: ${state.slot}${state.runtime ? ` (${describeRuntime(state.runtime)})` : ''}`);

    // ── Modal: the SLOT decides whether the container is used as-is or replaced ──
    // Empty = nothing is running in it, so it is taken as the fresh machine the deploy
    // wants. Busy = DELETE /sandbox ends the container, Modal relaunches it at this same
    // stable URL, and the deploy waits for a NEW, empty container before POSTing.
    const modalContainer = classifyModalContainer(endpoint, health);
    let recycled: ModalContainerShape | undefined;
    let assumedFresh = false;
    let current = state;
    if (mode === 'deploy' && flags.replace && modalContainer) {
        if (state.slot === 'empty') {
            assumedFresh = true;
            line('slot is empty — nothing is running in the container, so it is used as the fresh machine (no reset)');
        } else if (modalContainer === 'sandbox') {
            throw new EndpointDeployError(
                `${endpoint} is a Modal SANDBOX tunnel (…modal.host) holding a ${state.slot} runtime — its URL dies with the container, so it cannot be reset in place. ` +
                    `Deploy to the box instead (the …modal.run URL of the same app: Modal relaunches that container at the same address), ` +
                    `or free the slot first with --teardown, then deploy again.`,
            );
        } else {
            line(`DELETE ${joinUrl(endpoint, 'sandbox')} — the slot holds a ${state.slot} runtime, so the container is replaced${state.runtime ? ` (discarding ${describeRuntime(state.runtime)})` : ''}`);
            current = await resetModalBoxContainer(endpoint, token, deps);
            recycled = 'box';
        }
    }

    // ── the slot: --teardown's whole job, or the replace step of a deploy ──
    const decision: TeardownDecision = mode === 'teardown' ? (current.slot === 'empty' ? { action: 'none', why: 'the slot is already empty' } : { action: 'teardown', why: '--teardown frees the slot' }) : decideTeardown(current.slot, flags.replace);
    if (decision.action === 'abort') throw new EndpointDeployError(`${decision.why} — pass --teardown to free it, or drop --no-teardown to let this command replace it`);
    let replaced: { slot: string; profile?: unknown; port?: unknown } | undefined;
    if (decision.action === 'teardown') {
        line(`DELETE ${joinUrl(endpoint, '')} — ${decision.why}${current.runtime ? `, replacing ${describeRuntime(current.runtime)}` : ''}`);
        replaced = { slot: current.slot, ...(current.runtime ? { profile: current.runtime.profile, port: current.runtime.port } : {}) };
        await teardownSlot(endpoint, token, deps);
        line('✓ slot empty — the container stays up and the endpoint keeps serving');
    } else if (mode === 'deploy' && !recycled && !assumedFresh) {
        line('slot is empty — nothing to replace');
    }
    if (mode === 'teardown') {
        line(`✓ no runtime holds ${endpoint} (the endpoint itself is untouched — deploy again to put a new hub in)`);
        // LAST line, bare single-line JSON — the prefix is for humans, and a consumer
        // must be able to `tail -1 | jq` this (start-daytona.ts:831 prints it the same).
        log(buildSummary({ mode, endpoint, slot: 'empty', ...(token ? { token } : {}), ...(replaced ? { replaced } : {}) }));
        return 0;
    }

    // ── deploy: the sibling script owns cargo, the POST and the readiness poll ──
    const invocation = buildDeployInvocation(flags, childEnv);
    line(`deploying the hub (${flags.mode === 'local' ? 'cargo here, ship the ELF' : 'ship the source, cargo there'}, port ${flags.port}) — the child's output follows`);
    const code = await deps.spawn(invocation);
    if (code !== 0) {
        error(
            `${PREFIX} ✗ deploy failed (exit ${code}) — the child printed the runner's /logs on a failed slot; ` +
                (recycled ? 'the container is the fresh one from the reset above, and its slot is empty' : 'nothing here changed beyond the teardown above'),
        );
        return code;
    }

    // ── where the hub is, from the endpoint's own report (see buildHubUrl) ──
    const ready = await readState(endpoint, token, deps);
    const { hubUrl, note } = buildHubUrl(ready, endpoint);
    line(`✓ hub ready — slot ${ready.slot}`);
    line(`  hub (clients):    ${hubUrl ?? `not reported (${note})`}`);
    line(`  management:       ${endpoint}/health · /tools · /status · /logs?lines=200 · DELETE /`);
    if (hubUrl === null) line(`  note:             Daytona sandboxes mint a per-port link with getPreviewLink(${flags.port}) — start:daytona does that; a Modal box serves the hub on its own URL`);
    log(buildSummary({ mode, endpoint, hubUrl, hubPort: flags.port, hubNote: note, ...(token ? { token } : {}), ...(replaced ? { replaced } : {}), ...(recycled ? { recycled } : {}) }));
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
 * start-daytona.ts:955-985.
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
const stripModuleQuery = (token: string): string => String(token).split('?')[0].split('#')[0];
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
