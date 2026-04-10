-- part of a query repo
-- query name: Average & Median TLV by User eurSPKCC
-- query link: https://dune.com/queries/6743252


-- ============================================================
-- Average + Median TVL per Holder — eurSPKCC
-- All chains: EVM + Starknet + Stellar
-- NAV: Redstone (Ethereum) primary, Arbitrum fallback — EUR → USD via Chainlink
-- ============================================================
WITH
-- EUR/USD FX (Chainlink Arbitrum)
fx_raw AS (
    SELECT date_trunc('day', block_time) AS day,
        bytearray_to_int256(topic1) * 1e-8 AS rate_eur_usd
    FROM arbitrum.logs
    WHERE block_date >= DATE '2024-11-01'
      AND topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f
      AND contract_address = 0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871
),
fx_latest AS (
    SELECT COALESCE(MAX_BY(rate_eur_usd, day), 1.08) AS fx
    FROM fx_raw
),
-- NAV price — Redstone (8 dec, EUR) primary, Arbitrum (6 dec, EUR) fallback
redstone_raw AS (
    SELECT date_trunc('day', block_time) AS day,
        bytearray_to_int256(topic1) * 1e-8 AS p
    FROM ethereum.logs
    WHERE block_date >= DATE '2024-11-01'
      AND topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f
      AND contract_address = 0x4B2C406f0Dbf7624a32971277DA7B4C43A7A942b
),
arb_raw AS (
    SELECT date_trunc('day', block_time) AS day,
        bytearray_to_uint256(bytearray_substring(data, 1+32, 32)) * 1e-6 AS p
    FROM arbitrum.logs
    WHERE block_date >= DATE '2024-11-01'
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0x7A16Df1C2Cd8B9EEb9ED9942c82C2e7c90Bb93Db
),
nav_latest AS (
    SELECT COALESCE(
        (SELECT MAX_BY(p, day) FROM redstone_raw),
        (SELECT MAX_BY(p, day) FROM arb_raw),
        1.0
    ) AS p
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
              0x3868D4e336d14D38031cf680329d31e4712e11cC,
              0x99F70A0e1786402a6796c6B0AA997ef340a5c6da,
              0x0e389C83Bc1d16d86412476F6103027555C03265,
              0x4f33aCf823E6eEb697180d553cE0c710124C8D59
          )
        UNION ALL
        SELECT CAST("from" AS VARCHAR), -CAST(value AS DOUBLE) / 1e5
        FROM evms.erc20_transfers
        WHERE evt_block_time >= TIMESTAMP '2024-04-30'
          AND "from" != 0x0000000000000000000000000000000000000000
          AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
          AND contract_address IN (
              0x3868D4e336d14D38031cf680329d31e4712e11cC,
              0x99F70A0e1786402a6796c6B0AA997ef340a5c6da,
              0x0e389C83Bc1d16d86412476F6103027555C03265,
              0x4f33aCf823E6eEb697180d553cE0c710124C8D59
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
      AND from_address = 0x06472cabc51a3805975b9c60c7dec63897c9a287f2db173a1d6c589d18dd1e07
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
      AND contract_id = 'CDWOB6T7SVSMMQN5V3P2OPTBAXOP7DAZHGVW3PYTZIKHVFKN6TBSXR6A'
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
    ROUND(SUM(net_balance * p * fx), 0)                                    AS "Total eurSPKCC TVL (USD)",
    ROUND(AVG(net_balance * p * fx), 2)                                    AS "Avg TVL per Holder (USD)",
    ROUND(approx_percentile(net_balance * p * fx, 0.5), 2)                 AS "Median TVL per Holder (USD)"
FROM all_holders
CROSS JOIN nav_latest
CROSS JOIN fx_latest