# Spiko Dune Dashboard

## Overview
Analytics dashboard for Spiko's on-chain tokenized fund products across multiple blockchains. Contains 35 queries organized into 7 categories.

## API Key
Stored in `.env` (git-ignored). Use the Dune MCP tools (`mcp__dune__getDuneQuery`, `mcp__dune__updateDuneQuery`, `mcp__dune__executeQueryById`, `mcp__dune__getExecutionResults`) for all query operations. **Be conservative with executions — limited API credits.**

## Query Categories

### KPI Queries (5)
| ID | Purpose |
|----|---------|
| 6737375 | Total TVL |
| 6737416 | TVL Growth (30d) |
| 6737456 | Total Holders |
| 6737468 | Holder Growth (30d) |
| 6706994 | Active Wallets (30d) |

### TVL by User (8)
| ID | Purpose |
|----|---------|
| 6706751 | USTBL TVL by user |
| 6706737 | EUTBL TVL by user |
| 6706770 | UKTBL TVL by user |
| 6706785 | SPKCC TVL by user |
| 6706775 | eurSPKCC TVL by user |
| 6941111 | SAFO-USD TVL by user |
| 6941193 | SAFO-EUR TVL by user |
| 6941234 | SAFO-GBP/CHF TVL by user |

### Avg/Median TVL (5)
| ID | Purpose |
|----|---------|
| 6743176 | USTBL avg/median |
| 6743237 | EUTBL avg/median |
| 6743243 | UKTBL avg/median |
| 6743246 | SPKCC avg/median |
| 6743252 | eurSPKCC avg/median |

### Holding Period (5)
| ID | Purpose |
|----|---------|
| 6707437 | USTBL holding period |
| 6707444 | EUTBL holding period |
| 6707461 | UKTBL holding period |
| 6707481 | SPKCC holding period |
| 6707499 | eurSPKCC holding period |

### SAFO Dashboard (3)
| ID | Purpose |
|----|---------|
| 6941175 | SAFO TVL timeseries |
| 6941212 | SAFO holder distribution |
| 6941280 | SAFO KPIs |

### Competitive Analysis (4)
| ID | Purpose |
|----|---------|
| 6847260 | TVL Competitors timeseries |
| 6803919 | Supply Competitors timeseries |
| 6847440 | Mint/Burn Competitors |
| 6747818 | Growth past 3 months |

### Advanced Analytics (5)
| ID | Purpose |
|----|---------|
| 6846216 | Cohort analysis |
| 6845718 | Retention |
| 6845793 | TVL concentration |
| 6846428 | Whale tracking |
| 6845179 | TVL by product timeseries |

## Spiko Products

All Spiko tokens use **5 decimals**.

| Product | Chains |
|---------|--------|
| USTBL | Ethereum, Polygon, Arbitrum, Base, Etherlink, Starknet, Stellar |
| EUTBL | Ethereum, Polygon, Arbitrum, Base, Etherlink, Starknet, Stellar |
| UKTBL | Ethereum, Polygon, Arbitrum |
| SPKCC | Ethereum, Polygon |
| eurSPKCC | Ethereum, Polygon |
| SAFO-USD | Arbitrum |
| SAFO-EUR | Arbitrum |
| SAFO-GBP | Arbitrum |
| SAFO-CHF | Arbitrum |

## Competitors

| Ticker | Decimals | Notes |
|--------|----------|-------|
| BUIDL | 6 | BlackRock — beware omnibus contract `0x6a9da2d710bb9b700acde7cb81f10f1ff8c89041` inflating mint/burn |
| BENJI | 18 | Franklin Templeton |
| OUSG | 18 | Ondo Finance |
| USYC | 6 | Hashnote |
| USTB | 6 | Superstate — 6 decimals on both Ethereum and Plume |
| USCC | 6 | Superstate |
| WTGXX | 18 | WisdomTree |
| JTRSY | 6 | Anemoy |
| FDIT | 18 | FundBridge |

## NAV Oracle Architecture (Arbitrum)

### Spiko Old Format (USTBL, EUTBL, UKTBL, SPKCC, eurSPKCC, SAFO-*)
- **Topic0**: `0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6`
- **Price location**: Data word 2 (bytes 33-64), i.e. `bytea2numeric(substr(data, 33, 32))`
- **Precision**: 6 decimals (divide by 1e6)

### Chainlink Format
- **Topic0**: `0xc797025feeeaf2cd924c99e9205acb8ec04d5cad21c41ce637a38fb6dee6016a`
- **Price location**: Data word 1
- **Precision**: 6 decimals

### Redstone Format (FX oracles)
- **Topic0**: `0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f`
- **Price location**: Topic1
- **Precision**: 8 decimals

## Oracle Contract Addresses (Arbitrum)

### SAFO NAV Oracles (deployed 2026-03-31)
| Product | Address |
|---------|---------|
| SAFO-USD | `0x372e37cA79747A2d1671EDBC5f1e2853B96BA351` |
| SAFO-EUR | `0x385D443ffA5b6Fb462b988D023a5DC3b37Ef1644` |
| SAFO-GBP | `0x835B48E97CBF727e23E7AA3bD40248818d20A2b0` |
| SAFO-CHF | `0xD1F12049cC311DfB177f168046Ed8e2bd341a7AF` |

### FX Oracles (Redstone, Arbitrum)
| Pair | Address |
|------|---------|
| EUR/USD | `0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871` |
| GBP/USD | `0x78f28D363533695458696b42577D2e1728cEa3D1` |

### CHF/USD
No on-chain oracle available. **Hardcoded to 1.13** across all queries.

## Supply Tracking Pattern
Mint/burn events are tracked via ERC20 Transfer events:
- **Mint**: `from = 0x0000000000000000000000000000000000000000`
- **Burn**: `to = 0x0000000000000000000000000000000000000000`

## Incremental Query Pattern
Many queries use Dune's materialized view pattern:
```sql
WITH previous AS (
    SELECT * FROM TABLE(previous.query.result(...))
)
```
This allows queries to build on their own prior results for efficiency.

## Known Issues
1. **BUIDL omnibus rebalancing**: Contract `0x6a9da2d710bb9b700acde7cb81f10f1ff8c89041` creates large internal transfers that inflate mint/burn metrics in query 6847440
2. **Growth 3m (6747818)**: Solana BENJI/OUSG may use wrong decimal divisor (1e6 vs 1e8); Starknet topic selector may not match actual Transfer events
3. **CHF/USD hardcoded**: All queries use 1.13 — update if an oracle becomes available
4. **Competitor queries (6847260, 6803919)**: Output raw token supply without NAV price conversion for non-Spiko tickers (by design — avoids oracle parsing bugs)
