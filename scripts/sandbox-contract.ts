// ─────────────────────────────────────────────────────────────────────────────
//  sandbox-contract.ts — the ONE place the Nexus scripts name the sandbox
//  runtime v2 contract (services/daytona/sandbox-specs.md).
//
//  WHY THIS MODULE EXISTS: three scripts (deploy-daytona, start-daytona,
//  deploy-endpoint) all speak the same v2 surface — `/apps`, `/apps/nexus`,
//  `/apps/nexus/logs`, `/app/nexus/` — and all send the same two credentials
//  (the Daytona preview token and the optional runner admin bearer). Keeping the
//  app name, path, URL joining, auth headers, health recognition and AppView
//  parsing here makes a route rename a one-line change instead of three.
//
//  NO SIDE EFFECTS, NO I/O: importable from a vitest spec and from the scripts'
//  entry guards alike. Spec references are to services/daytona/sandbox-specs.md.
// ─────────────────────────────────────────────────────────────────────────────

// ── identity ─────────────────────────────────────────────────────────────────
/** The registered app name (§4.7). Lowercase, fixed: the path and every route derive from it. */
export const APP_NAME = 'nexus';
/** The canonical public app root (§4.2/§5.1). Clients MUST use the trailing-slash form. */
export const APP_PATH = '/app/nexus/';
/** The mount without the trailing slash (what `Location`/Set-Cookie paths use, §5.1). */
export const APP_MOUNT = '/app/nexus';
/** The registry record route (§7.6/§7.7/§7.8): GET/PUT/DELETE. */
export const APP_ROUTE = `apps/${APP_NAME}`;
/** The app log tail route (§7.10), query added by the caller. */
export const APP_LOGS_ROUTE = `apps/${APP_NAME}/logs`;
/** The control-plane routes the scripts use (POST create is `/apps`, §7.5). */
export const APPS_ROUTE = 'apps';

// ── contract markers ─────────────────────────────────────────────────────────
/** The v2 wire marker (§17.1). A v1 client's `daytona-runtime-v1` is NOT this. */
export const V2_CONTRACT = 'sandbox-runtime-v2';
/** The v1 marker the runner must never report again (§17.1) — named so errors can say it. */
export const V1_CONTRACT = 'daytona-runtime-v1';
/** The capability that proves a v2 runner even if `apiVersion` is missing (§17.1). */
export const V2_CAPABILITY = 'apps';

// ── URL composition (Appendix B) ─────────────────────────────────────────────

/**
 * Base-relative join that PRESERVES a proxy's path prefix: `new URL('/apps', base)`
 * would clobber it, so the suffix is appended to the existing pathname instead.
 * deploy-daytona.ts and deploy-endpoint.ts must POST to the exact same target, so
 * this is the only join either of them is allowed to use.
 */
export const joinUrl = (base: string, suffix: string): string => {
    const url = new URL(base);
    url.pathname = `${url.pathname.endsWith('/') ? url.pathname : `${url.pathname}/`}${suffix}`;
    return url.toString();
};

/**
 * The deterministic public app URL: the management endpoint the client already
 * knows plus `AppView.path` (Appendix B: "clients should prefer `path` and join
 * it to their known endpoint"). A preview proxy's path prefix (if any) is kept by
 * joinUrl, which is exactly what a pass-through adapter forwards.
 */
export const appUrl = (endpoint: string, path: string = APP_PATH): string => joinUrl(endpoint, path.replace(/^\/+/, ''));

/**
 * The hub base URL a client uses once the nexus app is ready. There is NO per-port
 * preview link in v2 (§16.1, P-7): the single runner origin serves every app under
 * `/app/<name>/`, so this is endpoint + `/app/nexus/` in fresh AND reuse mode.
 */
export const hubUrlFor = (endpoint: string): string => appUrl(endpoint, APP_PATH);

/**
 * The hub URL reported for one `AppView`: prefer the server's `path` joined to the
 * known endpoint, fall back to the server's `publicUrl` when `path` is missing
 * (older/hand-written bodies), and null when neither exists. Never guesses a
 * per-port link (P-7/P-8), so a consumer can treat null as "not determined".
 */
export const resolveAppUrl = (endpoint: string, view: { path?: string | null; publicUrl?: string | null } | null | undefined): string | null => {
    if (view && typeof view.path === 'string' && view.path) return appUrl(endpoint, view.path);
    if (view && typeof view.publicUrl === 'string' && view.publicUrl) return view.publicUrl;
    return null;
};

// ── auth (§12.3, §16.1) ──────────────────────────────────────────────────────

/** Empty token → NO header at all, which keeps public endpoints and loopback stubs working. */
export const authHeaders = (previewToken: string): Record<string, string> => (previewToken ? { 'x-daytona-preview-token': previewToken } : {});

/**
 * The runner's optional admin credential (§12.3): Bearer wins over a session
 * cookie and is the only form a script can send. It is a SECOND, independent
 * credential from the preview token — the preview token gets the request through
 * the Daytona edge, the Bearer authorizes the mutation/log read inside the runner.
 */
export const adminHeaders = (adminToken: string): Record<string, string> => (adminToken ? { authorization: `Bearer ${adminToken}` } : {});

/** Both credentials on one request: preview header first, then the runner Bearer. */
export const requestHeaders = (credentials: { previewToken?: string; adminToken?: string }): Record<string, string> => ({
    ...authHeaders(credentials.previewToken ?? ''),
    ...adminHeaders(credentials.adminToken ?? ''),
});

/** Presence + length for a plan/usage line — the value itself is NEVER printed. */
export const maskSecret = (secret: string): string => (secret ? `*** (length ${secret.length})` : '(not sent)');

/** The exact plan line for one credential, so a dry run can prove what would be sent. */
export const secretPlanLine = (header: string, secret: string): string => `  ${header}: ${maskSecret(secret)}`;

// ── v2 health recognition (§7.2, §17.1) ──────────────────────────────────────

/** Only the health fields the scripts read; everything else is ignored. */
export type RunnerHealth = {
    contract: string;
    apiVersion: number | null;
    capabilities: string[];
    runnerPort: number | null;
    publicOrigin: string | null;
    authRequired: boolean;
    service: string;
};

/** Reads `GET /health` without trusting its shape: missing fields become neutral values. */
export const parseHealth = (json: unknown): RunnerHealth => {
    const raw = (json ?? {}) as Record<string, unknown>;
    const apiVersion = Number(raw.apiVersion);
    return {
        contract: typeof raw.contract === 'string' ? raw.contract : '',
        apiVersion: Number.isFinite(apiVersion) ? apiVersion : null,
        capabilities: Array.isArray(raw.capabilities) ? raw.capabilities.filter((entry): entry is string => typeof entry === 'string') : [],
        runnerPort: Number.isFinite(Number(raw.runnerPort)) ? Number(raw.runnerPort) : null,
        publicOrigin: typeof raw.publicOrigin === 'string' ? raw.publicOrigin : null,
        // v2 reports `authRequired`; the v1 marker was `authenticated`, and both are
        // read so a migrated runner still tells the operator a credential exists.
        authRequired: raw.authRequired === true || raw.authenticated === true,
        service: typeof raw.service === 'string' ? raw.service : '',
    };
};

/**
 * THE v2 test (§17.1): a v2 client MUST require `apiVersion >= 2` OR
 * `capabilities` containing `apps`. Both are accepted because the runner may
 * report either first (the spec permits a v2 runner to omit one in a migration),
 * and a v1 runner satisfies neither. `authRequired` is read from the v1
 * `authenticated` marker too, so a v2 runner that keeps the legacy field still
 * tells the operator that a credential exists.
 */
export const isV2Runner = (health: RunnerHealth): boolean => (health.apiVersion !== null && health.apiVersion >= 2) || health.capabilities.includes(V2_CAPABILITY);

/** One line describing what answered, for both the success line and the v1 refusal. */
export const describeHealth = (health: RunnerHealth): string => {
    const contract = health.contract || '(no contract marker)';
    const api = health.apiVersion === null ? 'apiVersion ?' : `apiVersion ${health.apiVersion}`;
    const capabilities = health.capabilities.length ? health.capabilities.join(', ') : '(none)';
    return `${contract}, ${api}, capabilities [${capabilities}], runner port ${health.runnerPort ?? '?'}, public origin ${health.publicOrigin ?? 'not reported'}, auth ${health.authRequired ? 'admin token' : 'none'}`;
};

// ── AppView (§5.2) ───────────────────────────────────────────────────────────

/** §9.1. `draining` is transient; `failed` is terminal until PUT/DELETE. */
export const APP_STATUSES = ['staging', 'queued', 'building', 'booting', 'ready', 'draining', 'failed'] as const;
export type AppStatus = (typeof APP_STATUSES)[number];
/** The v2 states a PUT may replace (§6.2 step 3); anything else is `app_not_replaceable`. */
export const REPLACEABLE_STATUSES: readonly AppStatus[] = ['ready', 'failed'];

/** One pending revision during a blue-green replace (§5.2). */
export type PendingView = {
    revision: number | null;
    status: string;
    internalPort: number | null;
    queue: { position: number; ahead: number } | null;
    error: string | null;
};

/**
 * The REDACTED slice of `AppView` the scripts print and decide on. §5.3 forbids
 * env values and command text in any control response, and this parser is the
 * second line of defence: only these fields are ever surfaced, so a future
 * un-redacted field can never leak through a pretty-print of the raw body.
 */
export type AppView = {
    name: string;
    revision: number | null;
    path: string;
    publicUrl: string | null;
    internalPort: number | null;
    profile: string;
    filename: string;
    bundleBytes: number | null;
    status: string;
    ready: boolean;
    pid: number | null;
    error: string | null;
    exit: { code: number | null; signal: string | null } | null;
    lastReplaceError: string | null;
    queue: { position: number; ahead: number } | null;
    pending: PendingView | null;
};

/**
 * Number-or-null for the integer fields. `null`/`undefined`/'' stay null — note
 * `Number(null) === 0`, which would otherwise turn "not allocated yet" into a port
 * 0 fact. NaN and non-numeric strings also become null.
 */
const intOrNull = (value: unknown): number | null => {
    if (value === null || value === undefined || value === '') return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
};

/** `{position, ahead}` only while a build-lane wait is in progress (§9.3). */
const parseQueue = (value: unknown): { position: number; ahead: number } | null => {
    const raw = (value ?? null) as Record<string, unknown> | null;
    if (!raw || !Number.isFinite(Number(raw.position))) return null;
    const position = Number(raw.position);
    return { position, ahead: Number.isFinite(Number(raw.ahead)) ? Number(raw.ahead) : Math.max(0, position - 1) };
};

/** Parses a `pending` revision, or null when the body has none (§5.2). */
const parsePending = (value: unknown): PendingView | null => {
    const raw = (value ?? null) as Record<string, unknown> | null;
    if (!raw || typeof raw !== 'object') return null;
    return {
        revision: intOrNull(raw.revision),
        status: typeof raw.status === 'string' ? raw.status : 'unknown',
        internalPort: intOrNull(raw.internalPort),
        queue: parseQueue(raw.queue),
        error: typeof raw.error === 'string' ? raw.error : null,
    };
};

/** Parses `{code, signal}` for a dead process (§5.2); null before/without an exit. */
const parseExit = (value: unknown): { code: number | null; signal: string | null } | null => {
    const raw = (value ?? null) as Record<string, unknown> | null;
    if (!raw || typeof raw !== 'object') return null;
    return { code: intOrNull(raw.code), signal: typeof raw.signal === 'string' ? raw.signal : null };
};

/**
 * `AppView` from a control body, or null when the body is not one. Null is also
 * returned for a `{error}` body, so a caller can tell "no app" from "an app".
 */
export const parseAppView = (json: unknown): AppView | null => {
    const raw = (json ?? null) as Record<string, unknown> | null;
    if (!raw || typeof raw !== 'object' || typeof raw.name !== 'string' || !raw.name) return null;
    return {
        name: raw.name,
        revision: intOrNull(raw.revision),
        path: typeof raw.path === 'string' ? raw.path : APP_PATH,
        publicUrl: typeof raw.publicUrl === 'string' ? raw.publicUrl : null,
        internalPort: intOrNull(raw.internalPort),
        profile: typeof raw.profile === 'string' ? raw.profile : '',
        filename: typeof raw.filename === 'string' ? raw.filename : '',
        bundleBytes: intOrNull(raw.bundleBytes),
        status: typeof raw.status === 'string' ? raw.status : 'unknown',
        ready: raw.ready === true,
        pid: intOrNull(raw.pid),
        error: typeof raw.error === 'string' ? raw.error : null,
        exit: parseExit(raw.exit),
        lastReplaceError: typeof raw.lastReplaceError === 'string' ? raw.lastReplaceError : null,
        queue: parseQueue(raw.queue),
        pending: parsePending(raw.pending),
    };
};

/** One line for a human log: the fields that matter, and nothing redacted. */
export const describeAppView = (view: AppView): string => {
    const parts = [`revision ${view.revision ?? '?'}`, `status ${view.status}`, `internal port ${view.internalPort ?? '?'}`];
    if (view.ready) parts.push('ready');
    if (view.queue) parts.push(`queue ${view.queue.position} (ahead ${view.queue.ahead})`);
    if (view.pending) parts.push(`pending revision ${view.pending.revision ?? '?'} ${view.pending.status}`);
    if (view.error) parts.push(`error ${view.error}`);
    if (view.lastReplaceError) parts.push(`last replace error ${view.lastReplaceError}`);
    return parts.join(', ');
};

/** §9.1 terminality: only `failed` is terminal; `ready` can still drain. */
export const isAppFailed = (view: AppView): boolean => view.status === 'failed';

/** §6.2 step 3: a PUT is legal only for ready/failed, and only with no pending replace. */
export const isAppReplaceable = (view: AppView): boolean =>
    (REPLACEABLE_STATUSES as readonly string[]).includes(view.status) && view.pending === null;

// ── error bodies (§19) ───────────────────────────────────────────────────────

/** The `error` code from a control error body, or '' when the body is not one. */
export const errorCode = (json: unknown): string => {
    const raw = (json ?? {}) as Record<string, unknown>;
    return typeof raw.error === 'string' ? raw.error : '';
};

/** The error `details` object (§7.1), or an empty object. */
export const errorDetails = (json: unknown): Record<string, unknown> => {
    const raw = (json ?? {}) as Record<string, unknown>;
    const details = raw.details;
    return details && typeof details === 'object' ? (details as Record<string, unknown>) : {};
};

/** A compact human description of a non-2xx body: code + details, else the raw text. */
export const describeErrorBody = (res: { status: number; json: unknown; text: string }): string => {
    const code = errorCode(res.json);
    if (code) {
        const details = errorDetails(res.json);
        const detail = Object.keys(details).length ? ` ${JSON.stringify(details)}` : '';
        return `HTTP ${res.status} ${code}${detail}`;
    }
    const text = (res.text ?? '').trim();
    return text ? `HTTP ${res.status}: ${text.slice(0, 300)}` : `HTTP ${res.status}`;
};
