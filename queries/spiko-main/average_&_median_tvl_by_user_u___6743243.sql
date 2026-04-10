-- part of a query repo
-- query name: Average & Median TVL by User UKTBL
-- query link: https://dune.com/queries/6743243


-- ============================================================
-- Average + Median TVL per Holder — UKTBL
-- All chains: EVM + Starknet + Stellar
-- ============================================================
WITH
-- GBP/USD FX (Chainlink Arbitrum)
fx_raw AS (
    SELECT date_trunc('day', block_time) AS day,
        bytearray_to_int256(topic1) * 1e-8 AS rate_gbp_usd
    FROM arbitrum.logs
    WHERE block_date >= DATE '2024-11-01'
      AND topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f
      AND contract_address = 0x78f28D363533695458696b42577D2e1728cEa3D1
),
fx_latest AS (
    SELECT COALESCE(MAX_BY(rate_gbp_usd, day), 1.27) AS fx
    FROM fx_raw
),
-- NAV price — Arbitrum oracle (6 decimals, GBP)
nav_raw AS (
    SELECT date_trunc('day', block_time) AS day,
        bytearray_to_uint256(bytearray_substring(data, 1+32, 32)) AS price_raw
    FROM arbitrum.logs
    WHERE block_date >= DATE '2024-11-01'
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59
),
nav_latest AS (
    SELECT COALESCE(MAX_BY(price_raw, day), 1000000) * 1e-6 AS p
    FROM nav_raw
),
evm_net AS (
    SELECT address, SUM(delta) AS net_balance
    FROM (
        SELECT CAST("to" AS VARCHAR) AS address,  CAST(value AS DOUBLE) / 1e5 AS delta
        FROM evms.erc20_transfers
        WHERE evt_block_time >= TIMESTAMP '2024-04-30'
          AND "to" != 0x0000000000000000000000000000000000000000
          AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
          AND contract_address IN (
              0xf695Df6c0f3bB45918A7A82e83348FC59517734E,
              0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D,
              0x903d5990119bC799423e9C25c56518Ba7DD19474,
              0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068
          )
        UNION ALL
        SELECT CAST("from" AS VARCHAR), -CAST(value AS DOUBLE) / 1e5
        FROM evms.erc20_transfers
        WHERE evt_block_time >= TIMESTAMP '2024-04-30'
          AND "from" != 0x0000000000000000000000000000000000000000
          AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
          AND contract_address IN (
              0xf695Df6c0f3bB45918A7A82e83348FC59517734E,
              0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D,
              0x903d5990119bC799423e9C25c56518Ba7DD19474,
              0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068
          )
    ) t
    GROUP BY 1
    HAVING SUM(delta) > 0.001
),
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
      AND from_address = 0x0153d6e0462080bb2842109e9b64f589ef5aa06bb32b26bbdb894aca92674395
    GROUP BY 1
    HAVING SUM(CASE
        WHEN keys[2] = 0x0000000000000000000000000000000000000000000000000000000000000000
            THEN  CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5
        WHEN keys[3] = 0x0000000000000000000000000000000000000000000000000000000000000000
            THEN -CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5
        ELSE 0
    END) > 0.001
),
stellar_net AS (
    SELECT
        json_extract_scalar(key_decoded, '$.vec[1].address') AS address,
        MAX_BY(TRY_CAST(json_extract_scalar(val_decoded, '$.i128') AS DOUBLE) / 1e5, closed_at) AS net_balance
    FROM stellar.contract_data
    WHERE closed_at_date >= DATE '2024-04-30'
      AND contract_id = 'CDT3KU6TQZNOHKNOHNAFFDQZDURVC3MSTL4ML7TUTZGNOPBZCLABP4FR'
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
    current_date                                                           AS "As of Date",
    COUNT(*)                                                               AS "Holders",
    ROUND(SUM(net_balance * p * fx), 0)                                    AS "Total UKTBL TVL (USD)",
    ROUND(AVG(net_balance * p * fx), 2)                                    AS "Avg TVL per Holder (USD)",
    ROUND(approx_percentile(net_balance * p * fx, 0.5), 2)                 AS "Median TVL per Holder (USD)"
FROM all_holders
CROSS JOIN nav_latest
CROSS JOIN fx_latest