// ══════════════════════════════════════════════════════════════════════════════
//  start-modal.ts — `start:modal`: deploy the hub to the Modal box @config/vault
//  names, never to a URL typed into this package.
//
//  The box URL is configuration, not package identity: it lives in the vault
//  (`Object.values(modalContainer)[0]`, config/vault/src/container/modal-container.ts)
//  and is read from there at run time. Everything else is the generic verb —
//  scripts/deploy-endpoint.ts — with every flag forwarded verbatim:
//
//      yarn workspace @typecrate/nexus start:modal                 # deploy
//      yarn workspace @typecrate/nexus start:modal --dry-run       # the plan
//      yarn workspace @typecrate/nexus start:modal --teardown      # free the slot
//
//  A URL passed positionally is still allowed (the deploy verb supports any
//  endpoint); it simply has to agree with the vault value, exactly as passing
//  two different --endpoint values already has to.
// ══════════════════════════════════════════════════════════════════════════════

import { modalContainer } from '@config/vault';

import { main } from './deploy-endpoint';

// `main` returns the exit code instead of exiting, which is what keeps this
// wrapper importable and its run observable (deploy-endpoint.ts:987-1018).
const exitCode = await main([Object.values(modalContainer)[0], ...process.argv.slice(2)]);
process.exitCode = exitCode;
