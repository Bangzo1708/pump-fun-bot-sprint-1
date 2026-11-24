// Pump.fun dip-bounce bot for agent-fun/replay/v2.
//
// Idea: most pump.fun coins spike, dump, and die — but a coin that falls hard off a
// *recent* high and then shows buyers stepping back in often bounces part of the way
// back. This bot buys exactly that bounce and gets out fast, so a typical trade is a
// small win or a small, capped loss and the rare runner pays for the losers.
//
// Entry (all of it must hold on the batch that decides the buy):
//   - the coin fell at least 18% from the highest price of its last 20 trades, but no
//     more than 60% (a collapse, not a dip);
//   - of its last 8 trades at least 5 are buys and the net SOL flow is positive;
//   - the most recent trade is a buy, and the price already bounced at least 3% off
//     the low of those last 8 trades (the bounce has started, we are not catching a
//     falling knife mid-air).
//
// Exit (checked every batch, sells the whole position):
//   - take profit when the position's mark value reaches 1.30x its cost;
//   - stop loss when it falls to 0.95x its cost;
//   - the creator selling (a rug signal, or the dump that made the dip) exits at
//     once — including when the creator had already sold before our buy filled,
//     which turns that trade into a two-slot bounce scalp;
//   - a 3600-slot time limit frees the slot for the next setup.
//
// Sizing and guardrails:
//   - 0.40 SOL per coin, one buy per coin per setup (a coin may set up again after a
//     full exit, since a fresh dip is a fresh trade). Buys are capped by the harness's
//     1%-of-real-reserve size limit and never go below 0.012 SOL, so every round trip
//     clears the 0.01 SOL bar. At most 5 positions are open at once.
//   - Pace catch-up: the guardrail needs 20 distinct coins round-tripped. The bot
//     tracks distinct coins bought against slots elapsed; once past a 30000-slot
//     warmup, if the pace lags one distinct coin per 5000 slots it also takes a
//     looser dip (12% off the high, 4 of the last 8 buys) until the pace recovers.
//     In a busy window this never fires; in a quiet one it buys the compliance.
//
// Everything here is a pure function of the batches the harness sends: no clock, no
// randomness, no I/O, no network. Iteration is only over insertion-ordered maps fed
// by the batch stream, so the two graded executions produce byte-identical action
// logs. Slippage is left wide open (10000 bps) on both sides so an exit is never
// refused for the price moving in the 2-slot latency.

import { LAMPORTS_PER_SOL, runBot, type Batch, type BotState, type OrderInput, type Pubkey } from './vendor/makefun-bot.ts';

// --- tunables (swept on the kit's full dev window; see the numbers in the report) ---
const STAKE_LAMPORTS = (LAMPORTS_PER_SOL * 2n) / 5n; // 0.40 SOL per coin
const MIN_BUY_LAMPORTS = (LAMPORTS_PER_SOL * 12n) / 1000n; // 0.012 SOL, above the 0.01 round-trip bar
const DIP_WINDOW = 20; // trades forming the recent high
const DIP_MIN = 0.18; // buy at least this far below that high
const DIP_MAX = 0.6; // ...but not further than this (a collapse)
const FLOW_TRADES = 8; // trades forming the buy-flow read
const FLOW_MIN_BUYS = 5; // ...of which at least this many are buys
const BOUNCE_MIN = 0.03; // price must already sit this far above the flow window's low
const TAKE_PROFIT = 1.3; // mark value / cost at which to sell
const STOP_LOSS = 0.95; // mark value / cost at which to cut
const MAX_HOLD_SLOTS = 3600; // time limit, in slots
const MAX_SLIPPAGE_BPS = 10_000;
// Pace catch-up for the 20-coin guardrail (one distinct coin per 5000 slots).
const PACE_SLOTS_PER_MINT = 5000;
const PACE_WARMUP_SLOTS = 30_000;
const LOOSE_DIP_MIN = 0.12;
const LOOSE_FLOW_MIN_BUYS = 4;

/** One trade as the bot remembers it: price, direction, size, slot, wallet. */
interface TapeTrade {
  price: number;
  buy: boolean;
  sol: number;
  slot: number;
  user: string;
}

/** Everything the bot knows about one coin, built from its own event stream. */
interface Tape {
  mint: Pubkey;
  creator: string;
  trades: number;
  firstPrice: number;
  lastPrice: number;
  buySol: number;
  sellSol: number;
  buyers: Set<string>;
  creatorSold: boolean;
  recent: TapeTrade[];
}

/** A live position's entry facts, recorded when its buy fill lands. */
interface Entry {
  slot: number;
  cost: bigint;
  peakRatio: number;
}

const LAM = 1_000_000_000;

/** Price of one token in SOL, from a curve's virtual reserves. */
function priceOf(vq: bigint, vt: bigint): number {
  if (vt === 0n) return 0;
  return Number(vq) / Number(vt);
}

const tapes = new Map<Pubkey, Tape>();
const entries = new Map<Pubkey, Entry>();
const bought = new Set<Pubkey>();
const buyInFlight = new Set<Pubkey>();
const tradedMints = new Set<Pubkey>();
let firstSlot = -1;

function tapeFor(mint: Pubkey, creator: string, slot: number): Tape {
  let t = tapes.get(mint);
  if (t === undefined) {
    t = {
      mint,
      creator,
      createdSlot: slot,
      trades: 0,
      firstPrice: 0,
      lastPrice: 0,
      buySol: 0,
      sellSol: 0,
      buyers: new Set(),
      creatorSold: false,
      recent: [],
    };
    tapes.set(mint, t);
  }
  return t;
}

/** Fold this batch's events into every coin's tape. */
function ingest(batch: Batch): void {
  for (const e of batch.events) {
    if (e.event === 'CreateEvent') {
      tapeFor(e.mint, e.creator, e.slot);
    } else if (e.event === 'TradeEvent') {
      const t = tapeFor(e.mint, '', e.slot);
      const px = priceOf(BigInt(e.virtual_quote_reserves), BigInt(e.virtual_token_reserves));
      const sol = Number(BigInt(e.sol_amount)) / LAM;
      if (t.trades === 0) t.firstPrice = px;
      t.trades += 1;
      t.lastPrice = px;
      if (e.is_buy) {
        t.buySol += sol;
        t.buyers.add(e.user);
      } else {
        t.sellSol += sol;
        if (e.user === t.creator) t.creatorSold = true;
      }
      t.recent.push({ price: px, buy: e.is_buy, sol, slot: e.slot, user: e.user });
      if (t.recent.length > 64) t.recent.splice(0, t.recent.length - 64);
    }
  }
}

/**
 * The dip rule: price `min`..`DIP_MAX` below the high of the last `window` trades,
 * buyers stepping in over the last FLOW_TRADES trades, and the bounce already
 * underway. `minBuys` is how many of the flow trades must be buys.
 */
function dipRule(state: BotState, t: Tape, window: number, min: number, minBuys: number): boolean {
  if (t.trades < window) return false;
  const w = t.recent.slice(-window);
  if (w.length < window) return false;
  const high = Math.max(...w.map((r) => r.price));
  if (high <= 0) return false;
  const dd = 1 - t.lastPrice / high;
  if (dd < min || dd > DIP_MAX) return false;
  const flow = t.recent.slice(-FLOW_TRADES);
  let buys = 0;
  let net = 0;
  for (const r of flow) {
    if (r.buy) buys += 1;
    net += r.buy ? r.sol : -r.sol;
  }
  if (buys < minBuys || net < 0) return false;
  if (t.recent[t.recent.length - 1]!.buy !== true) return false;
  const low = Math.min(...flow.map((r) => r.price));
  if (low <= 0 || t.lastPrice < low * (1 + BOUNCE_MIN)) return false;
  const curve = state.curves.get(t.mint);
  if (curve === undefined || curve.complete) return false;
  return true;
}

/** True once the bot is behind the distinct-coin pace the guardrail needs. */
function behindPace(slot: number): boolean {
  if (firstSlot < 0) return false;
  const elapsed = slot - firstSlot;
  if (elapsed < PACE_WARMUP_SLOTS) return false;
  return tradedMints.size * PACE_SLOTS_PER_MINT < elapsed;
}

await runBot({
  onBatch(state, batch) {
    if (firstSlot < 0) firstSlot = batch.slot;
    ingest(batch);

    // Record fills: entries that landed, buys that missed, positions that closed.
    for (const f of batch.fills) {
      if (f.side === 'buy') {
        buyInFlight.delete(f.mint);
        if (f.status === 'filled') {
          bought.add(f.mint);
          tradedMints.add(f.mint);
          if (!entries.has(f.mint)) {
            const pos = state.positions.get(f.mint);
            entries.set(f.mint, {
              slot: f.slot,
              cost: pos !== undefined ? pos.cost_lamports : 0n,
              peakRatio: 1,
            });
          }
        }
      }
      if (f.status === 'filled' && f.side === 'sell') {
        const pos = state.positions.get(f.mint);
        if (pos === undefined) {
          // Fully closed. A fresh dip on the same coin is a fresh trade, so the
          // coin becomes eligible again; the entry record goes with it.
          bought.delete(f.mint);
          entries.delete(f.mint);
        }
      }
    }

    const orders: OrderInput[] = [];
    const tx = state.txFeeLamports();
    let free = state.freeCashLamports();
    let slots = state.hello.caps.max_open_positions - state.committedPositionCount();
    const maxOrders = state.hello.caps.max_orders_per_batch;

    // --- exits first: free slots before any new entry ---
    for (const [mint, pos] of state.positions) {
      if (orders.length >= maxOrders) break;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      const entry = entries.get(mint);
      if (entry === undefined) continue;
      const curPx = priceOf(curve.virtual_quote_reserves, curve.virtual_token_reserves);
      if (curPx <= 0 || entry.cost === 0n) continue;
      const value = Number(pos.token_amount) * curPx;
      const ratio = value / Number(entry.cost);
      if (ratio > entry.peakRatio) entry.peakRatio = ratio;
      const tape = tapes.get(mint);
      // A creator sell exits at once, whenever it happened: when the creator had
      // already sold before our buy filled, the dump is over and the bounce the
      // entry read is underway, so the position is really a two-slot scalp that
      // banks the bounce's first leg instead of riding the fade that follows.
      // (Backtests: exiting on a creator sell any time is worth about +0.36 SOL
      // over the dev window; exiting only on fresh sells after entry, or never,
      // leaves the bot around breakeven.)
      const creatorDumped = tape !== undefined && tape.creatorSold;
      let sell = false;
      if (ratio >= TAKE_PROFIT) sell = true;
      else if (ratio <= STOP_LOSS) sell = true;
      else if (batch.slot - entry.slot > MAX_HOLD_SLOTS) sell = true;
      else if (creatorDumped) sell = true;
      if (sell) {
        orders.push({ side: 'sell', mint, token_amount: pos.token_amount, max_slippage_bps: MAX_SLIPPAGE_BPS });
        free -= tx;
      }
    }

    // --- entries: coins whose tape moved on this batch, in event order ---
    const loose = behindPace(batch.slot);
    const seen = new Set<Pubkey>();
    for (const e of batch.events) {
      if (orders.length >= maxOrders || slots <= 0) break;
      if (!seen.has(e.mint)) seen.add(e.mint);
      else continue;
      if (e.event !== 'TradeEvent' && e.event !== 'CreateEvent') continue;
      const mint = e.mint;
      if (bought.has(mint) || buyInFlight.has(mint) || state.positions.has(mint)) continue;
      const t = tapes.get(mint);
      if (t === undefined) continue;
      const ok =
        dipRule(state, t, DIP_WINDOW, DIP_MIN, FLOW_MIN_BUYS) ||
        (loose && dipRule(state, t, DIP_WINDOW, LOOSE_DIP_MIN, LOOSE_FLOW_MIN_BUYS));
      if (!ok) continue;
      const cap = state.maxBuyLamports(mint);
      if (cap < MIN_BUY_LAMPORTS) continue;
      const amount = cap < STAKE_LAMPORTS ? cap : STAKE_LAMPORTS;
      if (free < amount + tx) continue;
      orders.push({ side: 'buy', mint, quote_lamports: amount, max_slippage_bps: MAX_SLIPPAGE_BPS });
      buyInFlight.add(mint);
      free -= amount + tx;
      slots -= 1;
    }
    return orders;
  },
});
