# Second Wave — pump.fun paper-trading bot

An original, self-contained paper-trading bot built for the
[Pump.fun Paper Sprint: Any Bot Can Win](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
bounty on [make.fun](https://make.fun). It trades only on replayed market
data — paper trading, never real funds.

[![make.fun bounty](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/btcrugpul/second-wave-pump-bot/rank.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/btcrugpul/second-wave-pump-bot/reward.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

## The strategy: ride the second wave, not the first

Most pump.fun coins that pump hard do it in two pushes: an impulsive first
wave, a pullback while early buyers take profit, and then — if the coin is
still alive — a second wave. Second Wave ignores the first wave entirely:

1. **Arm** — a coin's price jumps sharply off its anchor low (a strong
   impulse on real volume). The bot only watches from here.
2. **Pullback** — price retraces part of the impulse while the bonding
   curve stays alive (real reserves still healthy). Dead coins are dropped.
3. **Entry** — the bot buys only when the pullback low has aged a few
   slots, price has recovered a solid share of the retrace, reclaimed its
   short pullback average, and the latest trade is rising again.
4. **Exits** — a fixed take-profit, a tighter stop-loss, and a max-hold
   exit close every position, so round trips always complete. A deep
   equity drawdown halts new buys for the rest of the run.

Positions are small and capped at 1% of a coin's real reserves, at most
five are open at once, and every order is sized to keep drawdown shallow
while still completing round trips of at least 0.01 SOL across many coins.

## How it runs

`main.ts` (Node 22, no dependencies) speaks the `agent-fun/replay/v2`
protocol directly: newline-delimited JSON on stdin/stdout. The harness
sends a `hello` handshake, then one `batch` of market events per step;
the bot answers each batch with exactly one `reply` carrying its orders
for that slot. The bot is fully deterministic — no clock, no randomness,
no network — so two executions of the same window produce byte-identical
action logs.
