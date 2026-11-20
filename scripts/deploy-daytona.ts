// Compile @typecrate/nexus and deploy it as the NAMED app `nexus` on a sandbox
// runtime v2 runner (services/daytona/sandbox-specs.md).
//
// WHAT THIS DOES:
//   cargo build → ONE single-file bundle (`nexus-hub`, a stripped static-ish ELF)
//   → POST it to `POST /apps` with `x-app-name: nexus` → the runner stages,
//   boots and probes it → we poll `GET /apps/nexus` until `ready`. The sandbox
//   owns the lifecycle; this script only ships bytes and waits.
//
// THE v2 ENDPOINT CONTRACT (normative: services/daytona/sandbox-specs.md):
//   POST   /apps              binary form: body = raw bundle bytes
//                             (application/octet-stream) + `x-app-name: nexus`,
//                             optional `x-runtime-profile`, `x-runtime-config`
//                             (base64url JSON) and `x-runtime-filename`.
//                             202 {accepted:true, app: AppView(status queued)}.
//   PUT    /apps/nexus        blue-green replace (--replace). Same headers/body;
//                             legal only while the app is ready|failed (§6.3).
//                             202 {accepted:true, app: <current>, pending: {...}}.
//   GET    /apps/nexus        the AppView (§5.2/§7.6). Poll this, NEVER /status:
//                             /status is the v1 alias for the app named `default`
//                             and reports "empty" for every named app (§7.11).
//   GET    /apps/nexus/logs   text/plain tail (§7.10) after a failure.
//   DELETE /apps/nexus        idempotent named teardown (§6.4/§7.8):
//                             200 {tornDown:true,name,removed:true|false}.
//                             `removed:false` means the name was already absent
//                             and is a SUCCESS, not a 404.
//   AUTH: a private Daytona preview URL authenticates ONLY through the request
//   header `x-daytona-preview-token` (no query param, no URL-embedded secret), so
//   --token / NEXUS_DAYTONA_PREVIEW_TOKEN is sent on every request. The runner's
//   OPTIONAL admin credential (§12.3) is a SEPARATE Bearer — --admin-token /
//   NEXUS_SANDBOX_ADMIN_TOKEN / SANDBOX_ADMIN_TOKEN — sent alongside it when the
//   runner has SANDBOX_ADMIN_TOKEN set; it authorizes mutations and log reads.
//   Without it the preview proxy still answers 401/403 before the runner sees
//   anything.
//
// THE APP IS A NAMED REGISTRY ENTRY, NOT A SLOT:
//   `POST /apps` is create-only and answers 409 app_exists when `nexus` is already
//   registered (in ANY state, including failed). This script NEVER tears anything
//   down implicitly — silently killing a running hub is worse than one extra flag.
//   The two explicit verbs are:
//     --replace    PUT /apps/nexus (blue-green; the old revision keeps serving
//                  until the new one is ready). Only legal for ready|failed.
//     --teardown   DELETE /apps/nexus (the sandbox itself keeps running).
//   A 409 app_not_replaceable (queued/building/booting/draining) or
//   replace_in_progress is explained with the exact next step, never retried
//   blind.
//
// PORT: the internal port is AUTO-ALLOCATED by the runner (§6.2 step 4) unless
//   the operator explicitly asks for one with --port. v2 removed the v1 profile
//   defaults (8085/8080), and the internal port never appears in a public URL:
//   clients use `<origin>/app/nexus/` (§4.1/§4.2, P-7/P-8). Omitting `config.port`
//   is what lets the runner pick a free loopback port for this app.
//
// TWO MODES:
//   --mode local         compile HERE, ship the binary. Zero sandbox build time.
//   --mode remote-build  ship the SOURCE as a tar.gz and let the sandbox run cargo
//                        (manifest profile). Slower, but independent of host
//                        registry access. The manifest profile does NOT get the
//                        nexus profile's runner-injected NEXUS_LISTEN (§8.6), so
//                        its runCmd supplies NEXUS_LISTEN=127.0.0.1:$PORT itself
//                        (bash -lc expands the runner-injected PORT, §9.4).
//
// GENERICITY: the bundle + profile + config are the only nexus-specific things
// here; everything else (args, upload, 409 handling, polling) is copy-paste
// generic — deliberately NOT built into a framework, since nexus is the only
// wired profile today.
//
// RUN VIA: `yarn deploy:daytona --endpoint https://…` (package.json).
// Zero npm dependencies: node:fs, node:path, node:child_process, built-in fetch,
// and the colocated sandbox-contract.ts (pure constants/helpers, no I/O).
// Scratch output (the source tarball) goes in the repo-root `Temp/` folder per
// AGENTS.md — never OS temp, never inside the nexus package (nested git repo).
import { execFileSync, spawnSync } from "node:child_process";
import type { SpawnSyncReturns } from "node:child_process";
// openSync/readSync/closeSync back the C3 format guard: the first 4 bytes of the
// bundle are probed without slurping a multi-MiB binary.
import { closeSync, existsSync, mkdirSync, openSync, readFileSync, readSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
    APP_LOGS_ROUTE,
    APP_NAME,
    APP_PATH,
    APP_ROUTE,
    APPS_ROUTE,
    V1_CONTRACT,
    describeAppView,
    describeErrorBody,
    describeHealth,
    errorCode,
    errorDetails,
    isV2Runner,
    joinUrl,
    parseAppView,
    parseHealth,
    requestHeaders,
    resolveAppUrl,
    secretPlanLine,
} from "./sandbox-contract";
import type { AppView, RunnerHealth } from "./sandbox-contract";

const here = dirname(fileURLToPath(import.meta.url));
// packages/typecrate/nexus — cargo runs from here so the repo-root
// .cargo/config.toml (target-dir = .rust/target) is discovered by walking up.
const pkgRoot = resolve(here, "..");
// packages/typecrate/nexus → packages/typecrate → packages → monorepo root:
// THREE ".." segments. The two used at services/nexus stopped one level short at
// packages/, which put the scratch tarball outside repo-root `Temp/` (breaking
// the AGENTS.md scratch rule) and made artifactCandidates() probe
// <repo>/packages/.rust/target, which never exists. Used only to locate the
// repo-root `Temp/` scratch dir and the monorepo cargo target dir; a standalone
// nexus checkout has neither, which the candidate list below accounts for.
const workspaceRoot = resolve(pkgRoot, "..", "..", "..");

export const BIN_NAME = "nexus-hub";
export const PROFILE_NEXUS = "nexus";
export const PROFILE_MANIFEST = "manifest";
// Cap waits: local is just a proxy upload + process boot; remote-build pays a
// full in-sandbox cargo build (fat LTO), which is minutes-to-decades slow.
export const WAIT_CAP_MS = { [PROFILE_NEXUS]: 6 * 60_000, [PROFILE_MANIFEST]: 40 * 60_000 };
/** The profile's readiness cap, including for a profile string widened to `string`. */
export const waitCapMs = (profile: string): number => WAIT_CAP_MS[profile as keyof typeof WAIT_CAP_MS] ?? WAIT_CAP_MS[PROFILE_NEXUS];
export const POLL_INTERVAL_MS = 2_000;
// C4: consecutive GET /apps/nexus TRANSPORT misses tolerated before giving up. A
// Daytona preview proxy blips (TLS resets, 502 during deploys) while the runtime
// keeps booting, so one reset must not abort a 40-minute build wait; 5
// consecutive misses (~10 s) is a dead endpoint and stays fatal.
export const POLL_TRANSIENT_MAX = 5;
// Scratch tarball location (repo-root Temp/, git-ignored, disposable).
const tempDir = join(workspaceRoot, "Temp", "nexus-deploy");
const srcTarball = join(tempDir, "nexus-src.tar.gz");
// Directories that must never ride along in the source tarball: VCS state,
// host-built artifacts (Windows ELF/DLL/PDB here), wasm glue, deps, scratch.
// `.rust` is the host cargo target/cache dir (4.6 GiB on this host, measured into
// an 874 MB tarball — over the runner's 512 MiB bundle cap, so remote-build would
// 413 body_too_large). `.turbo`/`var` are the local run cache and the hub's
// NEXUS_ARTIFACT_ROOT spill; all four are git-ignored and never source.
export const TAR_EXCLUDES = [".git", ".rust", ".turbo", "var", "frontend", "target", "pkg", "node_modules", "Temp"];

export const log = (message: string) => console.log(`>>> ${message}`);
export const ok = (message: string) => console.log(`✓ ${message}`);
// Same `[nexus:…]` prefix the sibling build-wasm.ts uses for failures, so
// script output is greppable across the package.
function die(message: string, hint?: string): never {
    console.error(`\n[nexus:deploy] ${message}`);
    if (hint) console.error(`[nexus:deploy] ${hint}`);
    process.exit(1);
}

export const USAGE = `usage: yarn deploy:daytona [flags]  (= vite-node scripts/deploy-daytona.ts)

  ships the nexus bundle to a sandbox runtime V2 runner and registers it as the
  app "${APP_NAME}" (POST /apps); the sandbox then serves it at ${APP_PATH}.

  --endpoint <url>   sandbox runner endpoint / runner origin (env NEXUS_DAYTONA_ENDPOINT)
  --token <t>        Daytona preview token, sent as the x-daytona-preview-token
                     header on EVERY request (env NEXUS_DAYTONA_PREVIEW_TOKEN)
  --admin-token <t>  the runner's admin bearer (env NEXUS_SANDBOX_ADMIN_TOKEN, then
                     SANDBOX_ADMIN_TOKEN); sent as Authorization: Bearer alongside
                     the preview header, and NEVER printed
  --mode <mode>      local (default: compile here, ship the nexus-hub binary)
                     remote-build (ship a source tarball, cargo runs in-sandbox)
  --port <n>         OPTIONAL internal loopback port for the app; when omitted (the
                     default) the runner AUTO-ALLOCATES one. Never public: clients
                     always use ${APP_PATH}
  --env K=V          extra env var for the runtime; repeatable
  --no-build         local mode only: skip cargo and ship an existing artifact
                     from the candidate locations (dies if none exists)
  --artifact <path>  local mode only: ship exactly this file, skip cargo
                     (implies --no-build); must be a Linux ELF binary
  --replace          PUT /apps/${APP_NAME} (blue-green replace) instead of POST /apps.
                     Legal only while the app is ready or failed; the current
                     revision keeps serving until the new one is ready.
  --teardown         DELETE /apps/${APP_NAME} instead of uploading (the SANDBOX is
                     not deleted; an already-absent app is still success)
  --no-wait          return after the 202 accepted instead of polling GET /apps/${APP_NAME}
  --dry-run          print the exact plan/request; no cargo, no network, no writes
  -h, --help         this text

examples:
  yarn deploy:daytona --endpoint https://proxy.example/abc --token <preview-token> --env NEXUS_TOKEN=xyz
  yarn deploy:daytona --mode remote-build --endpoint https://proxy.example/abc --token <preview-token>
  yarn deploy:daytona --no-build --token <preview-token>            # ship the already-built nexus-hub
  yarn deploy:daytona --artifact .rust/target/release/nexus-hub --token <preview-token>
  yarn deploy:daytona --replace --endpoint https://proxy.example/abc --token <preview-token>
  yarn deploy:daytona --teardown --endpoint https://proxy.example/abc --token <preview-token>`;

// ── args ────────────────────────────────────────────────────────────────────
/** The flag record parseArgs produces; the plan printers and builders read it through this. */
export type DeployArgs = {
    endpoint: string;
    /** '' = send no preview header (public endpoint / loopback stub). */
    token: string;
    /** '' = send no Authorization header (runner auth disabled). */
    adminToken: string;
    mode: "local" | "remote-build";
    /** undefined = let the runner auto-allocate (§6.2 step 4); --port 0 means the same. */
    port: number | undefined;
    env: Record<string, string>;
    wait: boolean;
    noBuild: boolean;
    artifact: string;
    teardown: boolean;
    replace: boolean;
    dryRun: boolean;
    help: boolean;
};

/**
 * Parses the flat flag list. `--env` is repeatable and accumulates into a plain
 * object; K=V splits on the FIRST "=" so values may contain "=" (e.g. base64 or
 * `KEY=a=b`). Unknown/missing-value flags are fatal with usage. The secret-bearing
 * flags (`--token`, `--admin-token`) fall back to env vars so a CI wrapper can
 * inject them without putting the secret in the visible command line.
 */
export function parseArgs(argv: string[]): DeployArgs {
    const out: DeployArgs = {
        endpoint: process.env.NEXUS_DAYTONA_ENDPOINT ?? "",
        // C1: preview token; empty string means "send no auth header" (public
        // endpoint / loopback stub), which is a supported configuration.
        token: process.env.NEXUS_DAYTONA_PREVIEW_TOKEN ?? "",
        // §12.3: the separate runner admin credential, optional. `||` (not `??`) so
        // an empty nexus-specific variable falls through to the generic name.
        adminToken: process.env.NEXUS_SANDBOX_ADMIN_TOKEN || process.env.SANDBOX_ADMIN_TOKEN || "",
        mode: "local",
        // v2 §8.3: absent = auto; the v1 8085 default is deliberately gone.
        port: undefined,
        env: {},
        wait: true,
        noBuild: false,
        artifact: "",
        teardown: false,
        replace: false,
        dryRun: false,
        help: false,
    };
    for (let i = 0; i < argv.length; i++) {
        const flag = argv[i]!;
        // Flags that all take a following value share this branch so the
        // "missing value" failure stays worded identically for each.
        if (flag === "--endpoint" || flag === "--mode" || flag === "--port" || flag === "--env" || flag === "--token" || flag === "--admin-token" || flag === "--artifact") {
            const value = argv[++i];
            if (value === undefined || value.startsWith("--")) die(`${flag} needs a value`, USAGE);
            if (flag === "--endpoint") out.endpoint = value;
            else if (flag === "--token") out.token = value;
            else if (flag === "--admin-token") out.adminToken = value;
            else if (flag === "--artifact") out.artifact = value;
            else if (flag === "--mode") out.mode = value as DeployArgs["mode"];
            else if (flag === "--port") {
                // §8.3: integer 0..65535; 0 is the explicit spelling of "auto".
                const port = Number(value);
                if (!Number.isInteger(port) || port < 0 || port > 65535) die(`--port must be an integer 0-65535 (0 = auto), got "${value}"`, USAGE);
                out.port = port === 0 ? undefined : port;
            } else {
                // K=V: reject an empty key; allow empty value and "=" in value.
                const eq = value.indexOf("=");
                if (eq <= 0) die(`--env expects KEY=VALUE, got "${value}"`, USAGE);
                out.env[value.slice(0, eq)] = value.slice(eq + 1);
            }
            continue;
        }
        if (flag === "--wait") out.wait = true;
        else if (flag === "--no-wait") out.wait = false;
        else if (flag === "--no-build") out.noBuild = true;
        else if (flag === "--teardown") out.teardown = true;
        else if (flag === "--replace") out.replace = true;
        else if (flag === "--dry-run") out.dryRun = true;
        else if (flag === "-h" || flag === "--help") out.help = true;
        else die(`unknown flag "${flag}"`, USAGE);
    }
    if (out.help) return out;
    if (out.mode !== "local" && out.mode !== "remote-build") {
        die(`--mode must be "local" or "remote-build", got "${out.mode}"`, USAGE);
    }
    // --no-build/--artifact pick which LOCAL bundle to ship; remote-build ships
    // a source tarball, so accepting them there would be a silent no-op and an
    // operator would believe a build (or an artifact) was skipped/used.
    if (out.mode !== "local" && (out.noBuild || out.artifact)) {
        die(`--no-build and --artifact apply to --mode local only (remote-build ships a source tarball)`, USAGE);
    }
    // --teardown and --replace are different verbs; accepting both would leave
    // the operator guessing which one won.
    if (out.teardown && out.replace) die(`--teardown and --replace are different verbs — pick one`, USAGE);
    return out;
}

/**
 * Validates the endpoint for the paths that actually need it (health/upload/
 * teardown). Preview proxies hand out absolute URLs with paths, so trailing-slash
 * joining is done by joinUrl rather than assuming the endpoint is host-rooted.
 */
export function requireEndpoint(endpoint: string): string {
    if (!endpoint) {
        die("no endpoint — pass --endpoint <url> or set NEXUS_DAYTONA_ENDPOINT", USAGE);
    }
    let url;
    try {
        url = new URL(endpoint);
    } catch {
        die(`--endpoint is not a valid absolute URL: "${endpoint}"`);
    }
    if (url.protocol !== "http:" && url.protocol !== "https:") {
        die(`--endpoint must be http(s), got "${url.protocol}"`);
    }
    return endpoint;
}

// ── small helpers ───────────────────────────────────────────────────────────
export const bytes = (n: number) => `${n.toLocaleString("en-US")} bytes (${(n / 1024 / 1024).toFixed(2)} MiB)`;
export const pretty = (value: unknown) => JSON.stringify(value, null, 2);
/** base64url (RFC 4648 §5) — the header form the contract expects. */
export const b64url = (value: unknown) => Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
/** Promise wrapper for the poll cadence (no dependency on node:timers). */
export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
/** size/existence probe used by both the plan print and the real build path. */
function probe(path: string) {
    if (!existsSync(path)) return { path, exists: false, size: 0 };
    const st = statSync(path);
    return { path, exists: st.isFile(), size: st.isFile() ? st.size : 0 };
}

/** The artifact descriptor `probe` reports (path + is-it-a-file + size). */
type Probed = ReturnType<typeof probe>;

// ── app request shape (v2) ──────────────────────────────────────────────────

/** The uploaded profile: the nexus binary, or the in-sandbox cargo build. */
export const profileFor = (args: Pick<DeployArgs, "mode">): string => (args.mode === "local" ? PROFILE_NEXUS : PROFILE_MANIFEST);

/**
 * The `x-runtime-config` payload (§8.3 allowed keys only).
 *
 * The nexus profile gets `{port?, env}`; the manifest profile gets the v1 build
 * recipe plus a runCmd that supplies `NEXUS_LISTEN`. WHY the runCmd prefix:
 * the runner injects NEXUS_LISTEN only for the `nexus` profile (§8.6), and the
 * nexus server otherwise defaults to 127.0.0.1:8085 — which is no longer the
 * allocated port now that v2 auto-allocates. `bash -lc` expands the
 * runner-injected `PORT` (§9.4) before exec, so the server binds exactly the
 * port the gateway routes to. NEXUS_LISTEN stays a RESERVED user env key for the
 * nexus profile (R-8.3); it is only ever set here inside the manifest runCmd,
 * never smuggled through `config.env`.
 *
 * `port` is included ONLY when the operator explicitly asked for one: absent
 * means auto (§8.3, v2 change). Sending `port: 8085` would make every app pin a
 * port that may already be taken by a peer or a Daytona reserved port.
 */
export function buildAppConfig(args: DeployArgs): Record<string, unknown> {
    const env = args.env;
    const port = args.port === undefined ? {} : { port: args.port };
    if (args.mode === "local") {
        // nexus profile: port + extra env. anonymous:true is a sandbox default, so
        // it is only worth overriding if a caller wants it.
        return { ...port, env };
    }
    // manifest profile: build inside the sandbox, then run the artifact it
    // produced. `runCmd` is extraction-dir relative, and `./target` is right
    // there because the tarball has no repo-root .cargo/config.toml (so no
    // .rust/target remap).
    //
    // NO `artifactPath`: §8.2 chmods it to 0755 right after STAGING, before
    // `buildCmd` runs (runtime.rs: stage_bundle at :231, run_build_step at :233,
    // the chmod at :341-366), so naming the not-yet-built target/release/nexus-hub
    // fails the app with `artifactPath "..." not found in stage dir` and the build
    // never starts. The cargo output is already executable, and `runCmd` names it.
    // (A prebuilt artifact is uploaded in --mode local, where filename is chmodded.)
    return {
        buildCmd: "cargo build --release --locked -p nexus-server",
        buildTimeoutSec: 1800,
        runCmd: `NEXUS_LISTEN=127.0.0.1:$PORT ./target/release/${BIN_NAME}`,
        env,
        ...port,
        probePath: "/v1/observe",
        probeTimeoutSec: 60,
        stopSignal: "SIGINT",
    };
}

/** Where the upload goes: create (`POST /apps`) or blue-green replace (`PUT /apps/nexus`). */
export const uploadUrl = (endpoint: string, replace: boolean): string => joinUrl(endpoint, replace ? APP_ROUTE : APPS_ROUTE);
/** Where the app state is read: `GET /apps/nexus`, never the v1 `/status` alias. */
export const appStateUrl = (endpoint: string): string => joinUrl(endpoint, APP_ROUTE);
/** Where the app is deleted: `DELETE /apps/nexus`, never the v1 root `DELETE /`. */
export const appTeardownUrl = (endpoint: string): string => joinUrl(endpoint, APP_ROUTE);
/** Where the app log tail is read after a failure. */
export const appLogsUrl = (endpoint: string, lines: number): string => `${joinUrl(endpoint, APP_LOGS_ROUTE)}?lines=${lines}`;
/** Where the runner's contract/version is read before any v2 operation. */
export const healthUrl = (endpoint: string): string => joinUrl(endpoint, "health");

/** The exact upload headers (binary form, §7.5) — also what the dry run prints. */
export function buildUploadHeaders(args: DeployArgs, config: Record<string, unknown>, filename: string): Record<string, string> {
    return {
        "content-type": "application/octet-stream",
        // §7.5: the app name is a header in binary form and the registry key.
        "x-app-name": APP_NAME,
        "x-runtime-profile": profileFor(args),
        // base64url so the header stays ASCII/JSON-safe.
        "x-runtime-config": b64url(config),
        "x-runtime-filename": filename,
        ...requestHeaders({ previewToken: args.token, adminToken: args.adminToken }),
    };
}

/** The two credentials one request carries; the poll loop and logs read pass this. */
export type Credentials = { previewToken?: string; adminToken?: string };

/** Normalises the flag record into the credential pair every request needs. */
export const credentialsOf = (args: Pick<DeployArgs, "token" | "adminToken">): Credentials => ({ previewToken: args.token, adminToken: args.adminToken });

// ── artifact location ───────────────────────────────────────────────────────
/**
 * C3 format guard. The sandbox execs the bundle verbatim, so it must be a
 * native Linux ELF. A host-built Windows PE POSTs fine (202) and then fails at
 * exec time with an opaque ENOEXEC buried in the sandbox log, so the check
 * happens here, before any bytes or sandbox time are spent.
 */
const ELF_MAGIC = Buffer.from([0x7f, 0x45, 0x4c, 0x46]); // 0x7f "E" "L" "F"
/** Reads the first 4 bytes without slurping a multi-MiB binary. */
function readMagic(path: string, n = 4): Buffer {
    const buf = Buffer.alloc(n);
    let fd;
    try {
        fd = openSync(path, "r");
        return buf.subarray(0, readSync(fd, buf, 0, n, 0));
    } catch {
        // Unreadable (permissions, race with a build): report as "no magic" so
        // the caller dies with the format message instead of a stack.
        return Buffer.alloc(0);
    } finally {
        if (fd !== undefined) closeSync(fd);
    }
}
/** Human label for whatever the first bytes actually were. */
function describeMagic(magic: Buffer) {
    if (magic.length < 4) return `short read (${magic.length} bytes)`;
    const hex = [...magic].map((b) => b.toString(16).padStart(2, "0")).join(" ");
    if (magic[0] === 0x4d && magic[1] === 0x5a) return `Windows PE/DOS ("MZ", ${hex})`;
    if (magic[0] === 0xca && magic[1] === 0xfe) return `macOS Mach-O ("CAFÉ", ${hex})`;
    return `unknown (${hex})`;
}
/** Dies unless the bundle starts with the ELF magic; returns it otherwise. */
function assertElf(found: Probed): Probed {
    const magic = readMagic(found.path);
    if (magic.equals(ELF_MAGIC)) return found;
    const label = describeMagic(magic);
    const isWindows = magic[0] === 0x4d && magic[1] === 0x5a;
    die(
        `${found.path} is not a Linux ELF binary — ${label}`,
        `the sandbox execs the bundle as-is, so a Windows build here would only fail in-sandbox:\n` +
            (isWindows
                ? `  - --mode remote-build  (recommended: the sandbox compiles a Linux ELF from source)\n`
                : `  - --mode remote-build  (the sandbox compiles a Linux ELF from source)\n`) +
            `  - --artifact <path> pointing at a Linux ${BIN_NAME} build\n` +
            `  - --no-build with a Linux artifact at ${artifactCandidates()[0]}`,
    );
}
/**
 * Two candidate locations, because cargo's target dir depends on where the
 * package is checked out:
 *   - monorepo: the repo-root .cargo/config.toml sets `target-dir = .rust/target`
 *     and cargo finds it by walking UP past the package boundary →
 *     <monorepoRoot>/.rust/target/release/nexus-hub
 *   - standalone nexus checkout: no such config → ./target/release/nexus-hub
 * The build path prefers cargo's own JSON output over guessing either one.
 */
export const artifactCandidates = () => [
    join(workspaceRoot, ".rust", "target", "release", BIN_NAME),
    join(pkgRoot, "target", "release", BIN_NAME),
];

/**
 * Presence check WITHOUT invoking cargo — the dry run must not exec anything.
 * Scans PATH for an executable entry literally named `cargo`.
 */
function cargoOnPath() {
    const sep = process.platform === "win32" ? ";" : ":";
    // On Windows both an extensionless `cargo` (git-bash shim) and the
    // PATHEXT-suffixed binaries count; on POSIX the bare name is the only form.
    const names = ["", ...(process.platform === "win32" ? (process.env.PATHEXT ?? ".EXE").split(";") : [])];
    for (const dir of (process.env.PATH ?? "").split(sep)) {
        if (!dir) continue;
        for (const ext of names) {
            try {
                if (existsSync(join(dir, `cargo${ext}`))) return true;
            } catch {
                /* a malformed PATH entry is a miss, not a crash */
            }
        }
    }
    return false;
}

/**
 * One cargo attempt with the pinned recipe; returns the raw spawnSync result.
 * `extraArgs` carries the `--offline` retry flag (see buildLocally).
 */
function runCargoBuild(extraArgs: string[] = []) {
    const args = ["build", "--release", "--locked", "-p", "nexus-server", "--message-format=json", ...extraArgs];
    log(`cargo ${args.join(" ")}  (cwd ${pkgRoot})`);
    const run = spawnSync("cargo", args, {
        cwd: pkgRoot,
        encoding: "utf8",
        // JSON lines plus compiler diagnostics can exceed the 1 MiB default.
        maxBuffer: 64 * 1024 * 1024,
    });
    // cargo itself is missing/uncallable: no retry could help, so stop here
    // with the toolchain message rather than the registry-fetch one.
    if (run.error) die(`cargo could not be started: ${run.error.message}`, "cargo is not on PATH (or the pinned toolchain is missing)");
    return run;
}

/** stderr tail used for the failure report; 12 lines keeps it readable. */
const stderrTail = (run: SpawnSyncReturns<string>) => (run.stderr ?? "").trim().split("\n").slice(-12).join("\n");

/**
 * Runs the pinned compile recipe and returns the artifact path.
 *
 * C2: the FIRST failure is retried exactly once with `--offline`, because the
 * documented host failure is the `index.crates.io` fetch (nexus README,
 * "Verification status") while the lockfile's deps are usually already vendored
 * in ~/.cargo. A second failure is fatal and lists the three escape routes —
 * nothing is ever shipped implicitly, so a stale artifact needs an explicit
 * --no-build / --artifact from the operator.
 * Returns the successful spawnSync result; resolveArtifact reads the artifact
 * path out of it (a no-op build may emit no artifact line, which is why the
 * caller still keeps the candidate list as a fallback).
 */
function buildLocally() {
    const first = runCargoBuild();
    if (first.status === 0) return first;
    const firstTail = stderrTail(first);
    log(`cargo build failed (exit ${first.status ?? "?"}) — retrying once with --offline (the crates.io index fetch is the known host failure)`);
    const retry = runCargoBuild(["--offline"]);
    if (retry.status !== 0) {
        const retryTail = stderrTail(retry);
        die(
            `cargo build failed (exit ${retry.status ?? "?"}; the --offline retry failed too)`,
            `--- first attempt ---\n${firstTail}\n--- --offline retry ---\n${retryTail}\n` +
                `options, all explicit operator actions:\n` +
                `  1. --mode remote-build      ship the source tarball; cargo runs in the sandbox (no host registry access)\n` +
                `  2. export HTTPS_PROXY=http://<proxy>:<port>  then re-run — this host needs a proxy for index.crates.io (nexus README, "Verification status")\n` +
                `  3. --no-build [--artifact <path>]  ship an existing Linux ${BIN_NAME} ELF (e.g. ${artifactCandidates()[0]})`,
        );
    }
    ok("cargo build succeeded on the --offline retry");
    return retry;
}

/**
 * Pulls the compiler-artifact executable path out of cargo's NDJSON stream
 * (`--message-format=json`). This is the only reliable locator: the target dir
 * depends on whether a repo-root .cargo/config.toml is discovered (see
 * artifactCandidates above), so guessing is the fallback, not the primary.
 * Returns null for a no-op build that emitted no artifact line.
 */
function artifactFromCargoJson(run: SpawnSyncReturns<string>) {
    let executable = null;
    for (const line of (run.stdout ?? "").split("\n")) {
        if (!line.startsWith("{")) continue;
        let msg;
        try {
            msg = JSON.parse(line);
        } catch {
            continue;
        }
        // Later matches win: the final artifact line is the finished binary.
        if (msg.reason === "compiler-artifact" && msg.target?.name === BIN_NAME && msg.executable) executable = msg.executable;
    }
    if (executable) ok(`cargo reported the artifact: ${executable}`);
    return executable;
}

/**
 * Picks the bundle to ship in local mode (C2/C3), in operator-intent order:
 *   --artifact <path> → that exact file; cargo never runs, so an explicitly
 *                       named artifact can never be silently shadowed by a
 *                       build that writes somewhere else (implies --no-build)
 *   --no-build        → cargo never runs; first non-empty candidate wins
 *   otherwise         → compile, trust cargo's reported path, then candidates
 * Every returned path has passed the ELF format guard (C3).
 */
function resolveArtifact(args: DeployArgs): Probed {
    if (args.artifact) {
        const path = resolve(args.artifact);
        const found = probe(path);
        if (!found.exists || found.size === 0) {
            die(
                `--artifact "${args.artifact}" is not a non-empty file`,
                `resolved to ${path} (${found.exists ? "not a regular file" : "absent"}) — cargo is skipped by --artifact, so there is nothing to ship`,
            );
        }
        ok(`shipping --artifact ${path} — ${bytes(found.size)} (cargo skipped)`);
        return assertElf(found);
    }
    // cargo's reported path first, then the two guessed locations as fallbacks.
    let fromCargo: string | null = null;
    if (args.noBuild) {
        log("--no-build: cargo skipped; looking for an existing artifact instead");
    } else {
        fromCargo = artifactFromCargoJson(buildLocally());
    }
    const candidates = [fromCargo, ...artifactCandidates()].filter((candidate): candidate is string => Boolean(candidate));
    for (const c of candidates) {
        const found = probe(c);
        if (found.exists && found.size > 0) {
            ok(`${BIN_NAME} ready — ${bytes(found.size)}`);
            return assertElf(found);
        }
    }
    die(
        args.noBuild
            ? `--no-build: no existing non-empty ${BIN_NAME} found to ship`
            : `build reported success but no non-empty ${BIN_NAME} was found`,
        `${candidates.map((c) => `  - ${c}`).join("\n")}\n` +
            (args.noBuild ? `drop --no-build to compile it, or point --artifact at an existing Linux ${BIN_NAME}` : ""),
    );
}

/**
 * Packs the package source for the manifest profile using system tar.
 * Excludes keep host-built and VCS bytes out of the bundle; the sandbox
 * unpacks to a fresh dir, so its cargo writes to ./target (no repo-root
 * .cargo/config.toml exists inside the tarball).
 */
function buildSourceTarball(): Probed {
    mkdirSync(tempDir, { recursive: true });
    const args = ["-czf", srcTarball];
    for (const pattern of TAR_EXCLUDES) args.push(`--exclude=${pattern}`);
    args.push("-C", pkgRoot, ".");
    log(`tar ${TAR_EXCLUDES.map((e) => `--exclude=${e}`).join(" ")} ${pkgRoot} → ${srcTarball}`);
    try {
        execFileSync("tar", args, { encoding: "utf8", maxBuffer: 16 * 1024 * 1024 });
    } catch (error) {
        // tar exit 1 means "some files changed/vanished while reading"; the
        // archive is still usable. Exit ≥2 is fatal. Surface stderr either way.
        const tar = error as { status?: number; stderr?: string; message?: string };
        const stderr = (tar.stderr ?? "").trim();
        if (tar.status === undefined || tar.status >= 2) {
            die(`tar failed (exit ${tar.status ?? tar.message})`, stderr);
        }
        console.warn(`[nexus:deploy] tar warning: ${stderr.split("\n")[0] ?? ""}`);
    }
    const found = probe(srcTarball);
    if (!found.exists || found.size === 0) die(`tar produced no archive at ${srcTarball}`);
    ok(`source bundle ready — ${bytes(found.size)}`);
    return found;
}

// ── HTTP ────────────────────────────────────────────────────────────────────
/**
 * fetch + JSON parse with a body dump on non-2xx: a preview proxy returning
 * HTML (502/404) is otherwise an opaque "Unexpected token <" JSON error.
 *
 * `options.soft` (poll loop only) turns a TRANSPORT failure into a
 * `{ networkError }` result instead of exiting, so the caller can tolerate a
 * proxy blip. It deliberately does NOT cover HTTP statuses — those are the
 * runner answering, which is a real answer.
 */
/** fetch init plus this script's two own knobs (`timeoutMs`, and C4 `soft`). */
export type RequestOptions = RequestInit & { timeoutMs?: number; soft?: boolean };
/** Normalised fetch answer: status, parsed JSON when the body was JSON, and
 *  `networkError` only when `soft` turned a transport failure into a value. */
export type RequestResult = { status: number; json: any; text: string; ok: boolean; networkError?: string };
/** The injectable HTTP seam: the real fetch here, a scripted list in a test. */
export type RequestFn = (method: string, url: string, options?: RequestOptions) => Promise<RequestResult>;
export async function request(method: string, url: string, options: RequestOptions = {}): Promise<RequestResult> {
    // `timeoutMs` and `soft` are ours, never part of fetch's init.
    const { timeoutMs, soft, ...init } = options;
    let response: Response;
    try {
        // `method` must be in the init: fetch defaults to GET, and a GET with a
        // body throws "Request with GET/HEAD method cannot have body".
        response = await fetch(url, { ...init, method, signal: AbortSignal.timeout(timeoutMs ?? 15_000) });
    } catch (error) {
        const message = (error as { message?: string })?.message ?? String(error);
        if (soft) return { status: 0, json: null, text: "", ok: false, networkError: message };
        // An upload that timed out here may already have been ACCEPTED, so callers
        // must not retry it — the registry would answer 409 app_exists to the retry.
        die(`${method} ${url} failed: ${message}`, "is the endpoint reachable and the Daytona preview proxy up?");
    }
    // Reading the body is inside the same timeout window, so a proxy that
    // headers-then-hangs surfaces as a clear message rather than a stack.
    let text: string;
    try {
        text = await response.text();
    } catch (error) {
        const message = `response body read failed: ${(error as { message?: string }).message ?? error}`;
        if (soft) return { status: 0, json: null, text: "", ok: false, networkError: message };
        die(`${method} ${url}: ${message}`);
    }
    let json = null;
    try {
        json = JSON.parse(text);
    } catch {
        /* non-JSON (proxy error page) — handled below */
    }
    if (!response.ok) {
        return { status: response.status, json, text, ok: false };
    }
    return { status: response.status, json, text, ok: true };
}

/** The v2 gate's verdict — a union so a test can pin the refusal without exiting. */
export type V2Gate = { ok: true; health: RunnerHealth } | { ok: false; message: string; hint: string };

/**
 * Refuses to run any v2 verb against a runner that is not v2 (§17.1: a v2 client
 * MUST require apiVersion >= 2 or the `apps` capability). This is a DIFFERENT
 * error from deploy-daytona's old v1 single-slot contract, and it is what stops
 * a confusing `404 not_found` on `POST /apps` from being reported as a bundle
 * problem. No auth is required on /health (§7.2), but the preview header is sent
 * anyway: the Daytona edge gates every route, /health included.
 */
export async function readV2Gate(endpoint: string, credentials: Credentials, requestImpl: RequestFn = request): Promise<V2Gate> {
    const res = await requestImpl("GET", healthUrl(endpoint), { headers: requestHeaders(credentials), timeoutMs: 15_000 });
    if (!res.ok) {
        return { ok: false, message: `GET /health returned ${describeErrorBody(res)}`, hint: "is this the runner origin, and is the Daytona preview proxy up?" };
    }
    const health = parseHealth(res.json);
    if (!isV2Runner(health)) {
        return {
            ok: false,
            message: `this endpoint is not a sandbox runtime v2 runner — ${describeHealth(health)}`,
            hint:
                `deploy:daytona registers the app "${APP_NAME}" at ${APP_PATH}, which needs /apps. ` +
                `A ${V1_CONTRACT} single-slot runner cannot host it; update the sandbox runner (services/daytona/sandbox/runtime-server).`,
        };
    }
    return { ok: true, health };
}

/** The gate as main uses it: a refusal is a clean exit 1 with the fix on stderr. */
async function requireV2Runner(endpoint: string, credentials: Credentials, requestImpl: RequestFn): Promise<void> {
    const gate = await readV2Gate(endpoint, credentials, requestImpl);
    if (!gate.ok) die(gate.message, gate.hint);
}

// ── upload outcome + failure explanations (pure) ────────────────────────────

/** What a create/replace was accepted as: the target revision the poll waits for. */
export type UploadOutcome =
    | { accepted: true; view: AppView | null; operation: "create" | "replace"; targetRevision: number | null }
    | { accepted: false; reason: string };

/**
 * Reads a 202 body (§7.5 create / §7.7 replace). A create's target revision is
 * the accepted app's revision; a replace's is the PENDING revision, because the
 * current app deliberately keeps its old revision while the pending one boots.
 * Anything other than 202 is a failure: the contract only vouches for a bundle
 * once it has been stored and queued.
 */
export function readUploadOutcome(res: RequestResult, operation: "create" | "replace"): UploadOutcome {
    if (res.status !== 202) return { accepted: false, reason: explainUploadFailure(res, operation) };
    if (!res.ok) return { accepted: false, reason: explainUploadFailure(res, operation) };
    const body = (res.json ?? {}) as { accepted?: unknown; app?: unknown; pending?: unknown };
    if (body.accepted !== true) {
        return { accepted: false, reason: `HTTP ${res.status} did not confirm the app was accepted: ${pretty(res.json ?? res.text.slice(0, 300))}` };
    }
    const view = parseAppView(body.app);
    if (operation === "replace") {
        // §7.7: a replace 202 carries `pending`, and `pending.revision` is the revision
        // to wait for. Without it the poll cannot tell the old current revision from the
        // replacement, and the old "ok:true with targetRevision null" made polling report
        // the still-serving current app as success. Treat it as a malformed acceptance:
        // the replacement may be running, but this client cannot verify it.
        const pending = (body.pending ?? null) as { revision?: unknown } | null;
        const pendingRevision = pending && Number.isInteger(Number(pending.revision)) ? Number(pending.revision) : null;
        if (pendingRevision === null) {
            return {
                accepted: false,
                // The raw response was already printed by main(); keep the reason
                // stable and body-independent (a `{}`, a null and a string revision
                // are the same defect to the operator).
                reason: `HTTP 202 accepted the replacement but reported no numeric pending.revision — cannot identify the revision to wait for, and the still-current revision is never success`,
            };
        }
        return { accepted: true, view, operation, targetRevision: pendingRevision };
    }
    return { accepted: true, view, operation, targetRevision: view?.revision ?? null };
}

/**
 * The 409 table of §19 in operator words. Every branch says what to DO next:
 *   · app_exists          → --replace or --teardown
 *   · app_not_replaceable → --replace only accepts ready|failed; --teardown first
 *   · replace_in_progress → wait for the pending revision to settle
 *   · reserved/in-use port→ drop --port (auto) or pick another
 *   · unauthorized        → the admin Bearer, which is a different token from
 *                           the Daytona preview token
 * Everything else keeps the runner's own code + details.
 */
export function explainUploadFailure(res: RequestResult, operation: "create" | "replace"): string {
    const code = errorCode(res.json);
    const details = errorDetails(res.json);
    switch (code) {
        case "app_exists":
            return `app "${APP_NAME}" already exists — re-run with --replace (blue-green PUT /apps/${APP_NAME}) or --teardown, then deploy again`;
        case "app_not_replaceable":
            return `app "${APP_NAME}" is "${String(details.status ?? "not ready or failed")}" — --replace only accepts a ready or failed app; run --teardown first, then deploy again`;
        case "replace_in_progress":
            return `a replacement of app "${APP_NAME}" is already in progress — poll GET /apps/${APP_NAME} until pending is null, then retry`;
        case "replace_failed":
            return `the blue-green replacement failed — the current revision is still serving; read GET /apps/${APP_NAME} → lastReplaceError, then fix and retry`;
        case "reserved_port":
        case "port_in_use":
        case "port_exhausted":
            return `${describeErrorBody(res)} — either drop --port (the runner auto-allocates, the v2 default) or pick a free non-reserved port`;
        case "runtime_limit":
            return `${describeErrorBody(res)} — the sandbox is at its app cap; delete an app first (DELETE /apps/<name>)`;
        case "staging_busy":
        case "build_queue_full":
            return `${describeErrorBody(res)} — the runner is busy; retry shortly`;
        case "unauthorized":
            return `${describeErrorBody(res)} — this runner has SANDBOX_ADMIN_TOKEN set: pass --admin-token (or set NEXUS_SANDBOX_ADMIN_TOKEN). It is not the Daytona preview token`;
        case "app_not_found":
            return operation === "replace"
                ? `app "${APP_NAME}" does not exist — --replace needs an existing app; drop --replace to create it`
                : `${describeErrorBody(res)} — the registry does not know the app after it was accepted`;
        default:
            return describeErrorBody(res);
    }
}

/**
 * Reads a DELETE answer (§6.4/§7.8). Success is `tornDown:true` in EITHER
 * `removed` state: `removed:false` means the name was already absent, which is
 * the idempotent behaviour the contract promises — not a 404 and not an error.
 */
export function readTeardownOutcome(res: RequestResult): { ok: true; removed: boolean } | { ok: false; reason: string } {
    if (!res.ok) return { ok: false, reason: describeErrorBody(res) };
    const body = (res.json ?? {}) as { tornDown?: unknown; removed?: unknown };
    if (body.tornDown !== true) {
        return { ok: false, reason: `DELETE /apps/${APP_NAME} did not confirm the teardown: ${pretty(res.json ?? res.text.slice(0, 300))}` };
    }
    return { ok: true, removed: body.removed === true };
}

// ── poll state machine ──────────────────────────────────────────────────────

/** What the poll is waiting for: a create's revision, or a replace's pending one. */
export type PollTarget = { operation: "create" | "replace"; revision: number | null };

/** The poll's verdict: a ready AppView, or a failure carrying the last view/reason. */
export type PollOutcome = { ok: true; view: AppView } | { ok: false; view?: AppView; reason: string };

/**
 * Polls `GET /apps/nexus` every 2 s until the target revision is ready or the
 * profile-specific cap. Prints only state transitions (plus a heartbeat every 10
 * polls) so a 40-minute in-sandbox cargo build does not drown the terminal.
 *
 * THE REPLACE TRAP: during a blue-green PUT the CURRENT app keeps reporting
 * `status:"ready"` the whole time, so a naive "ready means done" test would pass
 * before the replacement even booted. The target revision is the PENDING one
 * from the 202 body; success needs that revision to be current. Failure is the
 * pending disappearing while the current revision is still the old one — that
 * is the only way §6.3 removes a failed pending.
 *
 * C1: every poll carries the preview token (and the admin Bearer when given) —
 * the proxy guards /apps exactly like the upload.
 * C4: TRANSPORT failures are tolerated up to POLL_TRANSIENT_MAX consecutive
 * misses; an HTTP error from the runner is a real answer and returns a reason.
 */
export async function pollAppUntilReady(input: {
    endpoint: string;
    credentials: Credentials;
    target: PollTarget;
    capMs: number;
    requestImpl?: RequestFn;
    sleepImpl?: (ms: number) => Promise<void>;
    logImpl?: (line: string) => void;
    errorImpl?: (line: string) => void;
}): Promise<PollOutcome> {
    const requestImpl = input.requestImpl ?? request;
    const sleepImpl = input.sleepImpl ?? sleep;
    const logImpl = input.logImpl ?? log;
    const errorImpl = input.errorImpl ?? ((line: string) => console.error(line));
    const deadline = Date.now() + input.capMs;
    let lastKey = "";
    let tick = 0;
    let misses = 0;
    const targetRevision = input.target.revision;
    while (Date.now() < deadline) {
        const res = await requestImpl("GET", appStateUrl(input.endpoint), {
            headers: requestHeaders(input.credentials),
            soft: true,
            timeoutMs: 15_000,
        });
        if (res.networkError) {
            // `>` not `>=`: 5 misses are tolerated, the 6th in a row ends it.
            if (++misses > POLL_TRANSIENT_MAX) {
                return { ok: false, reason: `GET /apps/${APP_NAME} failed ${misses} times in a row — giving up: ${res.networkError}` };
            }
            logImpl(`GET /apps/${APP_NAME} fetch failed (${res.networkError}) — transient miss ${misses}/${POLL_TRANSIENT_MAX}, the app may still be booting`);
            await sleepImpl(POLL_INTERVAL_MS);
            continue;
        }
        // A successful round trip (any status) proves the endpoint is alive.
        misses = 0;
        // The app disappeared while we polled: another actor deleted it. Retrying
        // cannot bring it back, and a silent "ready" would be a lie.
        if (res.status === 404) {
            return { ok: false, reason: `GET /apps/${APP_NAME} returned 404 — the app disappeared while polling (deleted elsewhere?)` };
        }
        if (!res.ok) return { ok: false, reason: `GET /apps/${APP_NAME} returned ${describeErrorBody(res)}` };
        const view = parseAppView(res.json);
        if (!view) return { ok: false, reason: `GET /apps/${APP_NAME} returned a body that is not an AppView: ${(res.text ?? "").slice(0, 300)}` };
        const pendingKey = view.pending ? `${view.pending.revision ?? "?"}:${view.pending.status}` : "-";
        const key = `${view.revision ?? "?"}:${view.status}:${pendingKey}`;
        if (key !== lastKey) {
            logImpl(`app ${APP_NAME}: ${describeAppView(view)}`);
            lastKey = key;
        }
        // §7.7: PUT is legal on a FAILED app, so while a pending replacement exists
        // the CURRENT record is old state and may legitimately be failed (or ready).
        // Nothing about it ends the poll; only the disappearance of `pending` does.
        if (view.pending === null) {
            if (input.target.operation === "replace") {
                if (targetRevision === null) {
                    // readUploadOutcome rejects such a 202, so reaching this means a
                    // direct caller gave the poll a replace target it cannot verify.
                    // Never fall back to "current is ready" — that is the old
                    // success-on-old-revision bug.
                    return { ok: false, view, reason: `a replace poll needs the pending revision, but the target was null — refusing to treat the current revision as the replacement` };
                }
                if ((view.revision ?? 0) < targetRevision) {
                    // The pending revision vanished without becoming current → failed.
                    // The old record is returned untouched (its revision/status/error
                    // stay visible to the caller); only the "still serving" half is
                    // conditional, because a previously FAILED old app is registered
                    // (§9.1) but not serving.
                    const previous =
                        view.status === "ready" && view.ready
                            ? "the previous revision is still serving"
                            : `the previous revision (status ${view.status}) remains registered and is NOT serving`;
                    return { ok: false, view, reason: `the replacement revision ${targetRevision} failed — ${view.lastReplaceError ?? "no lastReplaceError reported"}; ${previous}` };
                }
                if (view.status === "ready" && view.ready) {
                    // The admitted revision is current AND serving. §5.2: `ready`
                    // requires status==="ready" AND the last probe succeeded.
                    return { ok: true, view };
                }
                if (view.status === "failed") {
                    // The new revision became current and then failed: this is the new
                    // app's failure, not a failed replacement of the old one.
                    return { ok: false, view, reason: `the replacement revision ${view.revision ?? "?"} became current and failed: ${view.error ?? "no error code reported"}` };
                }
            } else {
                // create: the accepted revision must be current and serving.
                const revisionReached = targetRevision === null || (view.revision ?? 0) >= targetRevision;
                if (view.status === "ready" && view.ready && revisionReached) {
                    return { ok: true, view };
                }
                if (view.status === "failed") {
                    return { ok: false, view, reason: `app "${APP_NAME}" failed: ${view.error ?? "no error code reported"}` };
                }
            }
        }
        if (++tick % 10 === 0) logImpl(`still ${view.status}${view.pending ? ` (pending ${view.pending.status})` : ""}… (${Math.round((deadline - Date.now()) / 1000)} s of budget left)`);
        await sleepImpl(POLL_INTERVAL_MS);
    }
    return { ok: false, reason: `timed out after ${Math.round(input.capMs / 60_000)} min waiting for revision ${targetRevision ?? "?"} to be ready (last state: ${lastKey || "never seen"})` };
}

/**
 * Best-effort log tail after a failure: the app's own runtime log is the only
 * way to see WHY (cargo error, missing env, bind failure). The preview token is
 * required here too — /apps/<name>/logs is behind the same proxy, and the admin
 * Bearer when the runner has auth enabled (§7.10/§12.3).
 */
export async function fetchAppLogs(endpoint: string, credentials: Credentials, lines = 40, requestImpl: RequestFn = request): Promise<string | null> {
    const res = await requestImpl("GET", appLogsUrl(endpoint, lines), { headers: requestHeaders(credentials), timeoutMs: 15_000 });
    return res.ok ? res.text : null;
}

// ── dry run ─────────────────────────────────────────────────────────────────
/**
 * Prints the full plan and the exact request that WOULD be sent, with no cargo
 * exec, no network, and no file writes. Used to sanity-check an endpoint/config
 * before spending sandbox time, and to show where the artifact would be.
 */
export function printDryRunPlan(args: DeployArgs, config: Record<string, unknown>): void {
    const isNexus = args.mode === "local";
    const profile = profileFor(args);
    log(`dry run — mode ${args.mode}, profile ${profile}, app ${APP_NAME}, no cargo / no network / no writes`);
    if (isNexus) {
        // C2: the plan mirrors resolveArtifact's precedence exactly, so a dry
        // run tells the truth about which bundle a real run would ship.
        if (args.artifact) log(`cargo: skipped (--artifact ships an explicit file)`);
        else if (args.noBuild) log("cargo: skipped (--no-build); an existing artifact from the candidates below must exist");
        else if (!cargoOnPath()) log("cargo is NOT on PATH here — local mode would fail; --mode remote-build avoids it");
        const paths = args.artifact ? [resolve(args.artifact)] : artifactCandidates();
        log(args.artifact ? "artifact (explicit --artifact, used verbatim):" : "artifact candidates (first non-empty one wins):");
        for (const c of paths) {
            const found = probe(c);
            if (!found.exists || found.size === 0) {
                console.log(`    · ${found.path}  (absent${args.artifact ? " — a real run dies here" : ""})`);
                continue;
            }
            // C3 preview: the ELF magic is read (read-only, no writes/exec) so a
            // Windows build is caught in the plan, not after a 3 MiB upload.
            const magic = readMagic(found.path);
            const format = magic.equals(ELF_MAGIC) ? "ELF ✓" : `NOT ELF — ${describeMagic(magic)}, a real run refuses it`;
            console.log(`    ✓ ${found.path}  ${bytes(found.size)}  [${format}]`);
        }
        if (!args.artifact && !args.noBuild) {
            log("(a real run compiles with `cargo build --release --locked -p nexus-server --message-format=json`, retries once with --offline on failure, and trusts cargo's reported path)");
        }
    } else {
        const found = probe(srcTarball);
        log(`source bundle: ${srcTarball} ${found.exists ? `✓ ${bytes(found.size)}` : "(absent — a real run creates it via system tar)"}`);
        log(`tar excludes: ${TAR_EXCLUDES.join(", ")}`);
        log("remote-build runCmd: NEXUS_LISTEN=127.0.0.1:$PORT ./target/release/nexus-hub (the runner injects PORT; the manifest profile has no derived NEXUS_LISTEN)");
    }
    // Validating the URL here (not just before the upload) is the point of a dry
    // run: a malformed endpoint should fail in the plan, not after the build.
    if (args.endpoint) requireEndpoint(args.endpoint);
    const method = args.replace ? "PUT" : "POST";
    const target = args.endpoint ? uploadUrl(args.endpoint, args.replace) : `<endpoint>/${args.replace ? APP_ROUTE : APPS_ROUTE}`;
    const stateTarget = args.endpoint ? appStateUrl(args.endpoint) : `<endpoint>/${APP_ROUTE}`;
    log(`endpoint: ${args.endpoint || "(none — --endpoint not set)"}`);
    if (args.endpoint) log(`v2 preflight: GET ${healthUrl(args.endpoint)} — refuses a non-v2 runner before any upload`);
    const filename = isNexus ? BIN_NAME : "nexus-src.tar.gz";
    log(
        `request that would be sent:\n  ${method} ${target}\n  content-type: application/octet-stream\n  x-app-name: ${APP_NAME}\n  x-runtime-profile: ${profile}\n` +
            `  x-runtime-filename: ${filename}\n  x-runtime-config: ${b64url(config).slice(0, 24)}… (base64url, masked; decoded below)\n` +
            // Secrets are shown masked — the plan is pasted into logs/terminals.
            `${secretPlanLine("x-daytona-preview-token", args.token)}${args.token ? "" : "  (no --token / NEXUS_DAYTONA_PREVIEW_TOKEN)"}\n` +
            `${secretPlanLine("authorization", args.adminToken)}${args.adminToken ? "  ← Bearer <admin token>" : "  (no --admin-token / NEXUS_SANDBOX_ADMIN_TOKEN)"}`,
    );
    console.log(`  config (decoded):\n${pretty(config).split("\n").map((l) => `    ${l}`).join("\n")}`);
    console.log(`  config.port: ${args.port === undefined ? "ABSENT — the runner auto-allocates a loopback port (v2 default)" : `${args.port} (explicit; must be free and non-reserved)`}`);
    const bodyNote = isNexus
        ? args.artifact
            ? `${args.artifact} as it sits on disk (cargo skipped)`
            : args.noBuild
              ? `the existing ${BIN_NAME} ELF from the candidates above (cargo skipped)`
              : `the ${BIN_NAME} ELF once compiled here (a few MiB)`
        : "nexus-src.tar.gz source tree (a few hundred KiB)";
    console.log(`  body: raw application/octet-stream — ${bodyNote}`);
    log(
        `after 202: ${args.wait ? `poll GET ${stateTarget} every ${POLL_INTERVAL_MS / 1000}s (auth on every poll; up to ${POLL_TRANSIENT_MAX} consecutive fetch blips tolerated), cap ${waitCapMs(profile) / 60_000} min` : "no wait (--no-wait)"}; ` +
            `409 app_exists → "--replace or --teardown", exit 1`,
    );
    if (args.wait) log(`on success: path ${APP_PATH}, hub URL ${args.endpoint ? resolveAppUrl(args.endpoint, { path: APP_PATH }) : `<endpoint>${APP_PATH}`} (no per-port preview link)`);
    ok("dry run complete (exit 0)");
}

/**
 * C5: `--dry-run --teardown` must not touch the network, so the DELETE is
 * printed instead of sent (a real teardown against an unreachable endpoint is
 * still a valid plan check). The endpoint is only validated, never contacted,
 * and may be omitted entirely in a dry run.
 */
export function printTeardownPlan(args: DeployArgs): void {
    log(`dry run — teardown of app "${APP_NAME}", no cargo / no network / no writes`);
    if (args.endpoint) requireEndpoint(args.endpoint);
    const target = args.endpoint ? appTeardownUrl(args.endpoint) : `<endpoint>/${APP_ROUTE}`;
    log(`request that would be sent:\n  DELETE ${target}\n  (idempotent: an already-absent app still answers { tornDown: true, removed: false })`);
    console.log(secretPlanLine("x-daytona-preview-token", args.token));
    console.log(`${secretPlanLine("authorization", args.adminToken)}${args.adminToken ? "  ← Bearer <admin token>" : ""}`);
    ok("dry run complete (exit 0)");
}

// ── main ────────────────────────────────────────────────────────────────────
export async function main(argv: string[] = process.argv.slice(2), requestImpl: RequestFn = request): Promise<number> {
    const args = parseArgs(argv);
    if (args.help) {
        console.log(USAGE);
        return 0;
    }
    const credentials = credentialsOf(args);

    // Teardown is its own verb: DELETE /apps/nexus is idempotent, so it needs
    // neither a mode nor an artifact.
    if (args.teardown) {
        // C5: a dry run prints the DELETE rather than sending it — no network,
        // no endpoint requirement, exit 0 even against an unreachable host.
        if (args.dryRun) {
            printTeardownPlan(args);
            return 0;
        }
        const endpoint = requireEndpoint(args.endpoint);
        await requireV2Runner(endpoint, credentials, requestImpl);
        const url = appTeardownUrl(endpoint);
        log(`DELETE ${url} — teardown of app "${APP_NAME}" (the SANDBOX is not deleted)`);
        const res = await requestImpl("DELETE", url, { headers: requestHeaders(credentials), timeoutMs: 60_000 });
        const outcome = readTeardownOutcome(res);
        if (!outcome.ok) die(`teardown failed: ${outcome.reason}`);
        console.log(pretty(res.json ?? { status: res.status, text: res.text.slice(0, 400) }));
        if (outcome.removed) ok(`app "${APP_NAME}" removed — GET /apps/${APP_NAME} now answers 404, the sandbox keeps running`);
        else ok(`no app named "${APP_NAME}" — nothing to remove (removed:false); the sandbox keeps running`);
        return 0;
    }

    const profile = profileFor(args);
    const config = buildAppConfig(args);

    if (args.dryRun) {
        printDryRunPlan(args, config);
        return 0;
    }

    const endpoint = requireEndpoint(args.endpoint);
    // v2 gate: refuse a v1 single-slot runner BEFORE spending build time or
    // uploading bytes, with the exact reason and the fix.
    await requireV2Runner(endpoint, credentials, requestImpl);

    let bundle: Probed;
    if (args.mode === "local") {
        // C2/C3: honours --no-build/--artifact and rejects a non-ELF bundle
        // before a single byte is uploaded.
        bundle = resolveArtifact(args);
    } else {
        bundle = buildSourceTarball();
    }

    const operation = args.replace ? "replace" : "create";
    const filename = args.mode === "local" ? BIN_NAME : "nexus-src.tar.gz";
    const target = uploadUrl(endpoint, args.replace);
    log(`${args.replace ? "PUT" : "POST"} ${target} — app ${APP_NAME}, profile ${profile}, ${bytes(bundle.size)}`);
    // Read the bytes here (not at build time) so the body matches what is on
    // disk right before sending; the size printed above is the same stat.
    const payload = readFileSync(bundle.path);
    const res = await requestImpl(args.replace ? "PUT" : "POST", target, {
        headers: buildUploadHeaders(args, config, filename),
        body: payload as unknown as BodyInit,
        // A 3+ MiB binary through a preview proxy is slower than a GET.
        timeoutMs: 120_000,
    });
    // Pretty-print whatever the runner said (202 app/pending info, a 409 code,
    // or the raw proxy error page when the body is not JSON).
    console.log(pretty(res.json ?? { status: res.status, text: (res.text ?? "").slice(0, 400) }));
    const outcome = readUploadOutcome(res, operation);
    if (!outcome.accepted) die(`${operation === "replace" ? "PUT" : "POST"} failed: ${outcome.reason}`);
    ok(`accepted (HTTP 202) — the sandbox is staging/building/booting app "${APP_NAME}"`);
    if (outcome.view) log(`accepted revision ${outcome.view.revision ?? "?"} (${outcome.view.status}), internal port ${outcome.view.internalPort ?? "auto/not yet allocated"}`);
    if (outcome.operation === "replace" && outcome.targetRevision !== null) {
        log(`blue-green: the current revision keeps serving at ${APP_PATH} until revision ${outcome.targetRevision} is ready`);
    }

    if (!args.wait) {
        log(`not waiting (--no-wait); poll GET ${appStateUrl(endpoint)} when you want the state`);
        return 0;
    }
    const polled = await pollAppUntilReady({
        endpoint,
        credentials,
        target: { operation: outcome.operation, revision: outcome.targetRevision },
        capMs: waitCapMs(profile),
        requestImpl,
    });
    if (!polled.ok) {
        // Surface the record even on failure: revision, status, internal port, path
        // and the runner's short error code are what an operator correlates with
        // the log tail (and `path` proves the route that WAS being served).
        if (polled.view) {
            console.error(`✗ app ${APP_NAME}: ${describeAppView(polled.view)}`);
            console.error(`  path: ${polled.view.path}${polled.view.publicUrl ? `  publicUrl: ${polled.view.publicUrl}` : ""}`);
        }
        console.error(`✗ ${polled.reason}`);
        const logs = await fetchAppLogs(endpoint, credentials, 40, requestImpl);
        if (logs !== null) console.error(`--- last log lines (GET /apps/${APP_NAME}/logs?lines=40) ---\n${logs}`);
        return 1;
    }
    const view = polled.view;
    const hubUrl = resolveAppUrl(endpoint, view);
    ok(`app "${APP_NAME}" is ready — revision ${view.revision ?? "?"}, status ${view.status}, internal port ${view.internalPort ?? "?"}`);
    log(`  path:    ${view.path}`);
    log(`  hub URL: ${hubUrl ?? "(not determinable)"}  ← clients use this base; no per-port preview link`);
    console.log(pretty({ app: view, hubUrl }));
    return 0;
}

// ══════════════════════════════════════════════════════════════════════════════
//  ENTRY POINT — importing this module must never start anything
// ══════════════════════════════════════════════════════════════════════════════
//
//  Same three rules as start-daytona.ts / deploy-endpoint.ts: `yarn
//  deploy:daytona` must auto-run, and `vitest` importing this file must not.
//  Rule 3 exists because a PLAIN `vite-node <file>` rewrites process.argv and
//  DROPS the file (measured in setup-sandbox.ts), so neither argv nor the
//  lifecycle line can see the entry; a stack probe is evidence only inside
//  vite-node's module runner, which is exactly where that gap is.

/** Extension-bearing token that identifies THIS file as the runnable script. */
const ENTRY_SCRIPT_PATTERN = /(?:^|[/\\])deploy-daytona\.[cm]?[jt]sx?$/;
/** vite/vite-node append module ids and query strings; strip them before matching. */
const stripModuleQuery = (token: string): string => String(token).split("?")[0]!.split("#")[0]!;
const isScriptToken = (token: unknown): boolean => ENTRY_SCRIPT_PATTERN.test(stripModuleQuery(String(token)));

/** Signals the entry detector reads — every field is process state, gathered once at import. */
export type EntryPointSignals = { argv?: readonly string[]; env?: Readonly<Record<string, string | undefined>>; modulePath?: string; stack?: string; vitest?: boolean };

/** Rule 1: argv's SCRIPT POSITIONAL is this file. */
const namesScriptPositional = (argv: readonly string[] | undefined): boolean => {
    const tokens = argv ?? [];
    if (isScriptToken(tokens[1])) return true;
    const flag = tokens.indexOf("--script");
    return flag !== -1 && isScriptToken(tokens[flag + 1]);
};

/** Rule 2: the npm/yarn lifecycle line names it (`deploy:daytona` = "vite-node scripts/deploy-daytona.ts"). */
const namesLifecycleEntryPoint = (env: Readonly<Record<string, string | undefined>> | undefined): boolean =>
    String(env?.npm_lifecycle_script ?? "")
        .split(/\s+/)
        .some(isScriptToken);

/** Rule 3: vite-node evaluated THIS file as its entry — only runner frames sit above it. */
const isRunnerEntryPoint = (stack: string | undefined, modulePath: string | undefined): boolean => {
    if (!stack || !modulePath) return false;
    const frames = stack.split("\n").map((frame) => frame.trim().replace("file://", ""));
    if (!frames.some((frame) => frame.includes("ViteNodeRunner"))) return false;
    const own = frames.reduce((found, frame, index) => (frame.includes(modulePath) ? index : found), -1);
    if (own < 0) return false;
    const callers = frames.slice(own + 1);
    const executeFile = callers.findIndex((frame) => frame.includes("ViteNodeRunner.executeFile"));
    const upTo = executeFile === -1 ? callers.length : executeFile;
    return callers.slice(0, upTo).every((frame) => frame.includes("ViteNodeRunner"));
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
        return join(here, "deploy-daytona.ts");
    }
})();

// The stack must be captured during module evaluation; inside main() the
// evidence for rule 3 is already gone.
if (isCliEntryPoint({ argv: process.argv, env: process.env, modulePath: MODULE_FILE_PATH, stack: new Error().stack, vitest: Boolean(process.env.VITEST) })) {
    main().then(
        (code) => {
            process.exitCode = code;
        },
        (failure) => {
            console.error(`[nexus:deploy] ✗ unexpected failure: ${failure instanceof Error ? (failure.stack ?? failure.message) : String(failure)}`);
            process.exitCode = 1;
        },
    );
}
