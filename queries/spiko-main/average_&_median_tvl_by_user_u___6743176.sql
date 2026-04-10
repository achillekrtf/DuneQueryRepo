-- part of a query repo
-- query name: Average & Median TVL by User USTBL
-- query link: https://dune.com/queries/6743176


-- ============================================================
-- Average + Median TVL per Holder — USTBL
-- All chains: EVM (eth/polygon/arbitrum/base/etherlink) + Starknet + Stellar
-- NAV: Ethereum mainnet — old oracle < 2024-12-03, Chainlink >= 2024-12-03
-- ============================================================
WITH
-- NAV price — Ethereum mainnet (6 decimals, USD)
nav_raw AS (
    -- Old oracle (< 2024-12-03)
    SELECT date_trunc('day', block_time) AS day,
        bytearray_to_uint256(bytearray_substring(data, 1+32, 32)) AS price_raw
    FROM ethereum.logs
    WHERE block_time < TIMESTAMP '2024-12-03'
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0x021289588cd81dC1AC87ea91e91607eEF68303F5
    UNION ALL
    -- Chainlink oracle (>= 2024-12-03)
    SELECT date_trunc('day', block_time) AS day,
        bytearray_to_uint256(bytearray_substring(data, 1, 32)) AS price_raw
    FROM ethereum.logs
    WHERE block_time >= TIMESTAMP '2024-12-03'
      AND topic0 = 0xc797025feeeaf2cd924c99e9205acb8ec04d5cad21c41ce637a38fb6dee6016a
      AND contract_address = 0xC1C24f0f2103F5899b7AB415A1930E519B7D3423
),
nav_latest AS (
    SELECT MAX_BY(price_raw, day) * 1e-6 AS p
    FROM nav_raw
),
-- EVM: net balance per address
evm_net AS (
    SELECT address, SUM(delta) AS net_balance
    FROM (
        SELECT CAST("to" AS VARCHAR) AS address,  CAST(value AS DOUBLE) / 1e5 AS delta
        FROM evms.erc20_transfers
        WHERE evt_block_time >= TIMESTAMP '2024-04-30'
          AND "to" != 0x0000000000000000000000000000000000000000
          AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
          AND contract_address IN (
              0xe4880249745eAc5F1eD9d8F7DF844792D560e750,
              0x021289588cd81dC1AC87ea91e91607eEF68303F5
          )
        UNION ALL
        SELECT CAST("from" AS VARCHAR), -CAST(value AS DOUBLE) / 1e5
        FROM evms.erc20_transfers
        WHERE evt_block_time >= TIMESTAMP '2024-04-30'
          AND "from" != 0x0000000000000000000000000000000000000000
          AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
          AND contract_address IN (
              0xe4880249745eAc5F1eD9d8F7DF844792D560e750,
              0x021289588cd81dC1AC87ea91e91607eEF68303F5
          )
    ) t
    GROUP BY 1
    HAVING SUM(delta) > 0.001
),
-- Starknet: net balance per address
starknet_net AS (
    SELECT
        CASE
            WHEN keys[2] = 0x0000000000000000000000000000000000000000000000000000000000000000
                THEN CAST(keys[3] AS VARCHAR)
            ELSE CAST(keys[2] AS VARCHAR)
        END AS address,
        SUM(CASE
            WHEN keys[2] = 0x0000000000000000000000000000000000000000000000000000000000000000
                THEN  CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5
            WHEN keys[3] = 0x0000000000000000000000000000000000000000000000000000000000000000
                THEN -CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5
            ELSE 0
        END) AS net_balance
    FROM starknet.events
    WHERE block_date >= DATE '2024-04-30'
      AND cardinality(keys) >= 3 AND cardinality(data) >= 1
      AND keys[1] = 0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9
      AND from_address = 0x020ff2f6021ada9edbceaf31b96f9f67b746662a6e6b2bc9d30c0d3e290a71f6
    GROUP BY 1
    HAVING SUM(CASE
        WHEN keys[2] = 0x0000000000000000000000000000000000000000000000000000000000000000
            THEN  CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5
        WHEN keys[3] = 0x0000000000000000000000000000000000000000000000000000000000000000
            THEN -CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5
        ELSE 0
    END) > 0.001
),
-- Stellar: latest balance per address
stellar_net AS (
    SELECT
        json_extract_scalar(key_decoded, '$.vec[1].address') AS address,
        MAX_BY(TRY_CAST(json_extract_scalar(val_decoded, '$.i128') AS DOUBLE) / 1e5, closed_at) AS net_balance
    FROM stellar.contract_data
    WHERE closed_at_date >= DATE '2024-04-30'
      AND contract_id = 'CARUUX2FZNPH6DGJOEUFSIUQWYHNL5AVDV7PMVSHWL7OBYIBFC76F4TO'
      AND deleted = false
      AND contract_key_type = 'ScValTypeScvVec'
      AND json_extract_scalar(key_decoded, '$.vec[0].symbol') = 'Balance'
    GROUP BY 1
    HAVING MAX_BY(TRY_CAST(json_extract_scalar(val_decoded, '$.i128') AS DOUBLE) / 1e5, closed_at) > 0.001
),
all_holders AS (
    SELECT net_balance FROM evm_net
    UNION ALL
    SELECT net_balance FROM starknet_net
    UNION ALL
    SELECT net_balance FROM stellar_net
)

SELECT
    current_date                                                      AS "As of Date",
    COUNT(*)                                                          AS "Holders",
    ROUND(SUM(net_balance * p), 0)                                    AS "Total USTBL TVL (USD)",
    ROUND(AVG(net_balance * p), 2)                                    AS "Avg TVL per Holder (USD)",
    ROUND(approx_percentile(net_balance * p, 0.5), 2)                 AS "Median TVL per Holder (USD)"
FROM all_holders
CROSS JOIN nav_latest