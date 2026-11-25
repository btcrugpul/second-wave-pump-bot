// Second Wave — pullback-reversal paper-trading bot for agent-fun/replay/v2.
//
// Protocol shape (all handled inline below, no imports beyond Node builtins):
//   harness -> bot : hello {protocol, seed, universe, capital, caps, latency_slots, fees}
//                    batch {seq, slot, events, marks, fills, account}   (one per slot step)
//                    done  {seq, reason, account}
//   bot -> harness : exactly one reply per batch: {"type":"reply","seq":<batch seq>,"orders":[...]}
// Orders decided on the batch for slot s fill at slot s + latency_slots, so every
// decision below is made from the reserves and account carried by that batch alone.
// u64 amounts travel as decimal strings; BigInt is used wherever lamports are summed.
// Determinism: no clock, no Math.random, no environment reads — the same hello seed
// and the same batch stream always produce the same orders, byte for byte.
//
// Strategy sketch ("Second Wave"):
//   1. First wave — watch each mint for a sharp impulse off a local anchor low.
//   2. Pullback   — after the impulse peak, wait for a real retrace that does not kill
//                   the curve (reserves and trade flow still alive).
//   3. Second wave — buy only when price turns back up out of the pullback (reclaims
//                   the pullback's short average / breaks its local high).
//   Exits are mechanical: take-profit, stop-loss, and a max-hold slot exit, so every
//   round trip closes. Sizes are small and fixed so breadth (many distinct mints)
//   comes before size, and churn stays low because round-trip fees are ~2.5%.

import { createInterface } from 'node:readline';

// ---------------------------------------------------------------------------
// Tunables (plain numbers; revised in a later pass after local grading).
// ---------------------------------------------------------------------------
const IMPULSE_GAIN = 0.40; // first wave: +40% off the anchor low arms a mint
const IMPULSE_WINDOW_SLOTS = 600; // impulse must complete within this many slots
const ANCHOR_WINDOW_SLOTS = 900; // anchor low is forgotten after this idle span
const RETRACE_MIN = 0.15; // pullback: at least 15% down from the impulse peak
const RETRACE_MAX = 0.55; // deeper than 55% off the peak: treat the coin as dead
const RECLAIM_GAIN = 0.05; // second wave: +5% off the pullback low = turn back up
const MIN_TRADES_TO_ARM = 4; // a mint needs real trade flow before it can arm
const MIN_REAL_QUOTE = 2_000_000_000n; // 2 SOL of real reserves: curve is alive
const BASE_SIZE = 50_000_000n; // 0.05 SOL target position size
const MIN_SIZE = 12_000_000n; // never buy under 0.012 SOL (round trips count >= 0.01)
const TAKE_PROFIT = 0.10; // scalp the second wave: exit at +10% over entry
const STOP_LOSS = 0.07; // exit at -7% under entry
const MAX_HOLD_SLOTS = 700; // the bounce is short: time exit keeps it that way
const BUY_SLIPPAGE_BPS = 300;
const SELL_SLIPPAGE_BPS = 500;
const EQUITY_DD_HALT = 0.35; // stop opening new positions past 35% equity drawdown

// ---------------------------------------------------------------------------
// Per-mint path state. Price is virtual_quote_reserves / virtual_token_reserves
// as a JS number (deterministic: same inputs, same IEEE-754 result).
// ---------------------------------------------------------------------------
interface MintState {
  price: number;
  prevPrice: number;
  realQuote: bigint;
  complete: boolean;
  trades: number;
  lastSlot: number;
  // first-wave tracking
  anchor: number;
  anchorSlot: number;
  peak: number;
  peakSlot: number;
  phase: number; // 0 = watching for impulse, 1 = in pullback, 2 = bought once, cooling
  pullLow: number;
  pullLowSlot: number;
  pullStartSlot: number;
  pullPrices: number[]; // recent prices inside the pullback (short average + local high)
  coolUntilSlot: number;
}

interface HeldInfo {
  sinceSlot: number;
  sellPending: boolean;
  peakPnl: number; // best unrealised pnl fraction seen on this holding
}

const mints = new Map<string, MintState>();
const held = new Map<string, HeldInfo>();
const pendingBuys = new Set<string>();
const pendingSells = new Set<string>();

let capsMaxOpen = 5;
let capsMaxOrderBps = 100;
let capsMaxOrders = 16;
let orderCounter = 0;
let equityPeak = 0;
let halted = false;

function nextClientId(): string {
  orderCounter += 1;
  return 'sw' + orderCounter.toString(36) + '-' + orderCounter;
}

function priceOf(vq: string, vt: string): number {
  const q = Number(BigInt(vq));
  const t = Number(BigInt(vt));
  if (t <= 0) return 0;
  return q / t;
}

function stateFor(mint: string, slot: number): MintState {
  let s = mints.get(mint);
  if (s === undefined) {
    s = {
      price: 0,
      prevPrice: 0,
      realQuote: 0n,
      complete: false,
      trades: 0,
      lastSlot: slot,
      anchor: 0,
      anchorSlot: slot,
      peak: 0,
      peakSlot: slot,
      phase: 0,
      pullLow: 0,
      pullLowSlot: slot,
      pullStartSlot: slot,
      pullPrices: [],
      coolUntilSlot: 0,
    };
    mints.set(mint, s);
  }
  return s;
}

// Fold one batch's events and marks into the per-mint path state, in wire order.
function absorb(batch: any): void {
  const slot: number = batch.slot;
  for (const e of batch.events ?? []) {
    const s = stateFor(e.mint, slot);
    s.lastSlot = slot;
    if (e.event === 'CreateEvent') {
      const p = priceOf(e.virtual_quote_reserves, e.virtual_token_reserves);
      if (p > 0) {
        s.price = p;
        if (s.anchor === 0) {
          s.anchor = p;
          s.anchorSlot = slot;
        }
      }
      s.realQuote = 0n;
    } else if (e.event === 'TradeEvent') {
      const p = priceOf(e.virtual_quote_reserves, e.virtual_token_reserves);
      s.trades += 1;
      s.realQuote = BigInt(e.real_quote_reserves);
      if (p > 0) {
        s.prevPrice = s.price;
        s.price = p;
      }
    } else if (e.event === 'CompleteEvent') {
      s.complete = true;
    }
  }
  for (const m of batch.marks ?? []) {
    const s = mints.get(m.mint);
    if (s === undefined) continue;
    const p = priceOf(m.virtual_quote_reserves, m.virtual_token_reserves);
    if (p > 0) {
      s.prevPrice = s.price;
      s.price = p;
    }
    s.realQuote = BigInt(m.real_quote_reserves);
  }
  for (const f of batch.fills ?? []) {
    if (f.status === 'filled' || f.status === 'rejected' || f.status === 'failed') {
      pendingBuys.delete(f.mint);
      pendingSells.delete(f.mint);
      const h = held.get(f.mint);
      if (h !== undefined) h.sellPending = false;
    }
  }
}

// Advance the first-wave / pullback state machine for one mint at this slot.
function advanceWave(s: MintState, slot: number): void {
  if (s.price <= 0 || s.complete) return;
  if (s.phase === 0) {
    // Watching: keep the anchor at the lowest price inside the anchor window.
    if (slot - s.anchorSlot > ANCHOR_WINDOW_SLOTS || s.anchor === 0) {
      s.anchor = s.price;
      s.anchorSlot = slot;
    } else if (s.price < s.anchor) {
      s.anchor = s.price;
      s.anchorSlot = slot;
    }
    if (
      s.trades >= MIN_TRADES_TO_ARM &&
      s.anchor > 0 &&
      slot - s.anchorSlot <= IMPULSE_WINDOW_SLOTS &&
      s.price >= s.anchor * (1 + IMPULSE_GAIN)
    ) {
      // First wave confirmed: remember the peak and start the pullback watch.
      s.phase = 1;
      s.peak = s.price;
      s.peakSlot = slot;
      s.pullLow = s.price;
      s.pullLowSlot = slot;
      s.pullStartSlot = slot;
      s.pullPrices = [s.price];
    }
  } else if (s.phase === 2) {
    // Cooling after a buy: once the cool-down slot passes, watch afresh.
    if (slot > s.coolUntilSlot) {
      s.phase = 0;
      s.anchor = s.price;
      s.anchorSlot = slot;
    }
  } else if (s.phase === 1) {
    if (s.price > s.peak) {
      // Still making the first wave: lift the peak, restart the pullback.
      s.peak = s.price;
      s.peakSlot = slot;
      s.pullLow = s.price;
      s.pullLowSlot = slot;
      s.pullStartSlot = slot;
      s.pullPrices = [s.price];
      return;
    }
    if (s.price < s.pullLow) {
      s.pullLow = s.price;
      s.pullLowSlot = slot;
    }
    s.pullPrices.push(s.price);
    if (s.pullPrices.length > 24) s.pullPrices.shift();
    if (slot - s.pullStartSlot > IMPULSE_WINDOW_SLOTS * 2) {
      // Pullback never resolved: disarm and re-anchor at the current price.
      s.phase = 0;
      s.anchor = s.price;
      s.anchorSlot = slot;
      s.pullPrices = [];
    }
  }
}

// Equity estimate for the drawdown halt: cash plus open positions marked at the
// latest known price. Deterministic because it only uses batch-carried data.
function equityOf(account: any): number {
  let eq = Number(BigInt(account.cash_lamports));
  for (const p of account.positions ?? []) {
    const s = mints.get(p.mint);
    if (s !== undefined && s.price > 0) eq += s.price * Number(BigInt(p.token_amount));
  }
  return eq;
}

// ---------------------------------------------------------------------------
// Main loop: read NDJSON lines, answer each batch with exactly one reply.
// ---------------------------------------------------------------------------
const rl = createInterface({ input: process.stdin, crlfDelay: Infinity });

for await (const line of rl) {
  if (line.trim() === '') continue;
  const msg = JSON.parse(line);
  if (msg.type === 'hello') {
    capsMaxOpen = msg.caps.max_open_positions;
    capsMaxOrderBps = msg.caps.max_order_bps_of_real_reserve;
    capsMaxOrders = msg.caps.max_orders_per_batch;
    equityPeak = Number(BigInt(msg.capital.lamports));
    continue;
  }
  if (msg.type === 'done') {
    break;
  }
  if (msg.type !== 'batch') continue;

  const batch = msg;
  const slot: number = batch.slot;
  absorb(batch);

  const account = batch.account;
  const positions: any[] = account.positions ?? [];
  const heldMints = new Set<string>();
  for (const p of positions) {
    if (BigInt(p.token_amount) > 0n) heldMints.add(p.mint);
  }
  // Track when each holding first appeared (for the max-hold exit).
  for (const mint of heldMints) {
    if (!held.has(mint)) held.set(mint, { sinceSlot: slot, sellPending: false, peakPnl: 0 });
  }
  for (const mint of [...held.keys()]) {
    if (!heldMints.has(mint)) held.delete(mint);
  }

  // Drawdown halt: only blocks new buys, never exits.
  const eq = equityOf(account);
  if (eq > equityPeak) equityPeak = eq;
  if (equityPeak > 0 && eq < equityPeak * (1 - EQUITY_DD_HALT)) halted = true;

  const orders: any[] = [];

  // --- Exits first: they free position slots for this same batch's buys. ---
  for (const p of positions) {
    const tokens = BigInt(p.token_amount);
    if (tokens <= 0n) continue;
    const s = mints.get(p.mint);
    const h = held.get(p.mint);
    if (s === undefined || h === undefined) continue;
    if (h.sellPending || pendingSells.has(p.mint)) continue;
    const cost = Number(BigInt(p.cost_lamports));
    const entryPrice = cost / Number(tokens);
    let exit = false;
    if (s.complete) exit = true;
    else if (s.price > 0 && entryPrice > 0) {
      const pnl = s.price / entryPrice - 1;
      if (pnl > h.peakPnl) h.peakPnl = pnl;
      if (pnl >= TAKE_PROFIT) exit = true;
      else if (pnl <= -STOP_LOSS) exit = true;
    }
    if (!exit && slot - h.sinceSlot >= MAX_HOLD_SLOTS) exit = true;
    if (exit && orders.length < capsMaxOrders) {
      orders.push({
        side: 'sell',
        client_id: nextClientId(),
        mint: p.mint,
        token_amount: tokens.toString(),
        max_slippage_bps: SELL_SLIPPAGE_BPS,
      });
      h.sellPending = true;
      pendingSells.add(p.mint);
    }
  }

  // --- Second Wave entries. ---
  // Advance every mint's wave state machine on this slot's prices first.
  for (const s of mints.values()) advanceWave(s, slot);

  const committed = new Set<string>(heldMints);
  for (const mint of pendingBuys) committed.add(mint);

  if (!halted) {
    // Collect this batch's entry candidates, then rank them deterministically:
    // deepest healthy retrace first, mint string as the stable tiebreak.
    const candidates: { mint: string; s: MintState; score: number }[] = [];
    for (const [mint, s] of mints) {
      if (s.phase !== 1 || s.complete) continue;
      if (heldMints.has(mint) || pendingBuys.has(mint) || pendingSells.has(mint)) continue;
      if (s.price <= 0 || s.peak <= 0 || s.pullLow <= 0) continue;
      if (s.realQuote < MIN_REAL_QUOTE) continue;
      // Pullback depth: a real retrace off the peak, but the coin is not dead —
      // price must still sit clearly above the anchor the impulse started from.
      const offPeak = 1 - s.price / s.peak;
      if (offPeak < RETRACE_MIN || offPeak > RETRACE_MAX) continue;
      if (s.price < s.anchor * 1.08) continue;
      // Second wave turn: price has lifted off the pullback low and reclaimed
      // the pullback's own short average (the mean of its recent pullback prices).
      if (s.price < s.pullLow * (1 + RECLAIM_GAIN)) continue;
      // The low must be a few slots old — a low printed this slot is a knife
      // still falling, not a stabilised pullback — and the turn must have
      // recovered a real share of the retrace, on a rising latest trade.
      if (slot - s.pullLowSlot < 5) continue;
      if (s.price < s.pullLow + 0.30 * (s.peak - s.pullLow)) continue;
      if (s.prevPrice > 0 && s.price < s.prevPrice) continue;
      let sum = 0;
      const n = Math.min(s.pullPrices.length, 8);
      for (let i = 0; i < n; i++) sum += s.pullPrices[s.pullPrices.length - 1 - i];
      const shortAvg = n > 0 ? sum / n : s.price;
      if (s.price < shortAvg) continue;
      candidates.push({ mint, s, score: offPeak });
    }
    candidates.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return a.mint < b.mint ? -1 : a.mint > b.mint ? 1 : 0;
    });

    let cash = BigInt(account.cash_lamports);
    for (const c of candidates) {
      if (orders.length >= capsMaxOrders) break;
      if (committed.size >= capsMaxOpen) break;
      // Size cap: this batch's buys on one mint share caps.max_order_bps of its
      // real reserves — one order per mint here, so size straight against it.
      const capByReserve = (c.s.realQuote * BigInt(capsMaxOrderBps)) / 10000n;
      let size = BASE_SIZE < capByReserve ? BASE_SIZE : capByReserve;
      if (size < MIN_SIZE) continue;
      if (cash < size + 200_000n) continue; // keep room for the order's tx fees
      orders.push({
        side: 'buy',
        client_id: nextClientId(),
        mint: c.mint,
        quote_lamports: size.toString(),
        max_slippage_bps: BUY_SLIPPAGE_BPS,
      });
      cash -= size;
      committed.add(c.mint);
      pendingBuys.add(c.mint);
      // One shot per impulse: cool the mint down so it cannot re-arm instantly.
      c.s.phase = 2;
      c.s.coolUntilSlot = slot + MAX_HOLD_SLOTS + 2000;
      c.s.pullPrices = [];
    }
  }

  process.stdout.write(JSON.stringify({ type: 'reply', seq: batch.seq, orders }) + '\n');
}
