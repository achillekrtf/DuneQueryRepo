-- part of a query repo
-- query name: USTBL TVL by User
-- query link: https://dune.com/queries/6706751


-- ============================================================
-- USTBL — TVL by User | All Chains
-- Output: Address, Balance (tokens) | Pie chart ready
-- ============================================================

WITH
evm_transfers AS (
    SELECT "from", "to", value / 1e5 AS amount
    FROM evms.erc20_transfers
    WHERE evt_block_time >= TIMESTAMP '2024-04-30'
      AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
      AND contract_address IN (
          0xe4880249745eAc5F1eD9d8F7DF844792D560e750,  -- Eth/Poly/Base/Etherlink
          0x021289588cd81dC1AC87ea91e91607eEF68303F5   -- Arbitrum
      )
),
evm_balances AS (
    SELECT CAST(wallet AS VARCHAR) AS address, SUM(net) AS balance
    FROM (
        SELECT "to" AS wallet, amount AS net FROM evm_transfers
        WHERE "to" != 0x0000000000000000000000000000000000000000
        UNION ALL
        SELECT "from" AS wallet, -amount AS net FROM evm_transfers
        WHERE "from" != 0x0000000000000000000000000000000000000000
    ) t
    GROUP BY 1
    HAVING SUM(net) > 0.01
),

starknet_events AS (
    SELECT keys[2] AS from_addr, keys[3] AS to_addr,
        CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5 AS amount
    FROM starknet.events
    WHERE block_date >= DATE '2024-04-30'
      AND cardinality(keys) >= 3 AND cardinality(data) >= 1
      AND keys[1] = 0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9
      AND from_address = 0x020ff2f6021ada9edbceaf31b96f9f67b746662a6e6b2bc9d30c0d3e290a71f6
),
starknet_balances AS (
    SELECT CAST(wallet AS VARCHAR) AS address, SUM(net) AS balance
    FROM (
        SELECT to_addr AS wallet, amount AS net FROM starknet_events
        WHERE to_addr != 0x0000000000000000000000000000000000000000000000000000000000000000
        UNION ALL
        SELECT from_addr AS wallet, -amount AS net FROM starknet_events
        WHERE from_addr != 0x0000000000000000000000000000000000000000000000000000000000000000
    ) t
    GROUP BY 1
    HAVING SUM(net) > 0.01
),

stellar_balances AS (
    SELECT address, balance
    FROM (
        SELECT
            json_extract_scalar(key_decoded, '$.vec[1].address') AS address,
            TRY_CAST(json_extract_scalar(val_decoded, '$.i128') AS DOUBLE) / 1e5 AS balance,
            ROW_NUMBER() OVER (
                PARTITION BY json_extract_scalar(key_decoded, '$.vec[1].address')
                ORDER BY closed_at DESC
            ) AS rn
        FROM stellar.contract_data
        WHERE contract_id = 'CARUUX2FZNPH6DGJOEUFSIUQWYHNL5AVDV7PMVSHWL7OBYIBFC76F4TO'
          AND deleted = false
          AND contract_key_type = 'ScValTypeScvVec'
          AND json_extract_scalar(key_decoded, '$.vec[0].symbol') = 'Balance'
    ) t
    WHERE rn = 1 AND balance > 0.01
),

all_balances AS (
    SELECT address, balance FROM evm_balances
    UNION ALL
    SELECT address, balance FROM starknet_balances
    UNION ALL
    SELECT address, balance FROM stellar_balances
)

SELECT
    address        AS "Address",
    SUM(balance)   AS "Balance (tokens)"
FROM all_balances
GROUP BY 1
ORDER BY 2 DESC