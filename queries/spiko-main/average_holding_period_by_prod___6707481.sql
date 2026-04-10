-- part of a query repo
-- query name: Average Holding Period by Product SPKCC
-- query link: https://dune.com/queries/6707481


-- Average Holding Period — SPKCC — EVM + Starknet + Stellar
-- Exclude wallets with entry_day < 60 days ago
WITH
-- ====== EVM TRANSFERS ======
evm_transfers AS (
    SELECT
        DATE_TRUNC('day', evt_block_time) AS day,
        CAST("to" AS VARCHAR) AS wallet,
        CAST(value AS DOUBLE) / 1e5 AS amount
    FROM evms.erc20_transfers
    WHERE evt_block_time >= TIMESTAMP '2024-04-30'
      AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
      AND (
          (contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59 AND blockchain IN ('ethereum','etherlink'))
          OR (contract_address = 0x903d5990119bC799423e9C25c56518Ba7DD19474 AND blockchain = 'polygon')
          OR (contract_address = 0x99F70A0e1786402a6796c6B0AA997ef340a5c6da AND blockchain = 'arbitrum')
          OR (contract_address = 0xf695Df6c0f3bB45918A7A82e83348FC59517734E AND blockchain = 'base')
      )
      AND "to" != 0x0000000000000000000000000000000000000000
    UNION ALL
    SELECT
        DATE_TRUNC('day', evt_block_time) AS day,
        CAST("from" AS VARCHAR) AS wallet,
        -CAST(value AS DOUBLE) / 1e5 AS amount
    FROM evms.erc20_transfers
    WHERE evt_block_time >= TIMESTAMP '2024-04-30'
      AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
      AND (
          (contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59 AND blockchain IN ('ethereum','etherlink'))
          OR (contract_address = 0x903d5990119bC799423e9C25c56518Ba7DD19474 AND blockchain = 'polygon')
          OR (contract_address = 0x99F70A0e1786402a6796c6B0AA997ef340a5c6da AND blockchain = 'arbitrum')
          OR (contract_address = 0xf695Df6c0f3bB45918A7A82e83348FC59517734E AND blockchain = 'base')
      )
      AND "from" != 0x0000000000000000000000000000000000000000
),
-- ====== STARKNET TRANSFERS ======
starknet_transfers AS (
    SELECT
        DATE_TRUNC('day', block_date) AS day,
        CAST(keys[3] AS VARCHAR) AS wallet,
        CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5 AS amount
    FROM starknet.events
    WHERE block_date >= DATE '2024-04-30'
      AND from_address = 0x04bade88e79a6120f893d64e51006ac6853eceeefa1a50868d19601b1f0a567d
      AND keys[1] = 0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9
      AND cardinality(keys) >= 3 AND cardinality(data) >= 1
      AND keys[3] != 0x0000000000000000000000000000000000000000000000000000000000000000
    UNION ALL
    SELECT
        DATE_TRUNC('day', block_date) AS day,
        CAST(keys[2] AS VARCHAR) AS wallet,
        -CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5 AS amount
    FROM starknet.events
    WHERE block_date >= DATE '2024-04-30'
      AND from_address = 0x04bade88e79a6120f893d64e51006ac6853eceeefa1a50868d19601b1f0a567d
      AND keys[1] = 0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9
      AND cardinality(keys) >= 3 AND cardinality(data) >= 1
      AND keys[2] != 0x0000000000000000000000000000000000000000000000000000000000000000
),
-- ====== STELLAR BALANCES ======
stellar_raw AS (
    SELECT closed_at, DATE_TRUNC('day', closed_at) AS day,
        json_extract_scalar(key_decoded, '$.vec[1].address') AS wallet,
        TRY_CAST(json_extract_scalar(val_decoded, '$.i128') AS DOUBLE) / 1e5 AS balance
    FROM stellar.contract_data
    WHERE closed_at_date >= DATE '2024-04-30'
      AND contract_id = 'CDS2GCAQTNQINSCJUJIVBJXILKBWP5PU7LOBGHMP3X47QCQBFKPMTCNT'
      AND deleted = false
      AND contract_key_type = 'ScValTypeScvVec'
      AND json_extract_scalar(key_decoded, '$.vec[0].symbol') = 'Balance'
),
stellar_daily AS (
    SELECT day, wallet, balance
    FROM (
        SELECT day, wallet, balance,
            ROW_NUMBER() OVER (PARTITION BY wallet, day ORDER BY closed_at DESC) AS rn
        FROM stellar_raw
    ) t WHERE rn = 1
),
-- ====== COMBINE ALL CHAINS ======
all_transfers AS (
    SELECT * FROM evm_transfers
    UNION ALL
    SELECT * FROM starknet_transfers
),
daily_wallet_flows AS (
    SELECT day, wallet, SUM(amount) AS net_flow
    FROM all_transfers
    GROUP BY 1, 2
),
transfer_balances AS (
    SELECT day, wallet,
        SUM(net_flow) OVER (PARTITION BY wallet ORDER BY day) AS balance
    FROM daily_wallet_flows
),
wallet_balances AS (
    SELECT day, wallet, balance FROM transfer_balances
    UNION ALL
    SELECT day, wallet, balance FROM stellar_daily
),
first_positive AS (
    SELECT wallet, MIN(day) AS entry_day
    FROM wallet_balances
    WHERE balance > 0
    GROUP BY 1
),
with_prev AS (
    SELECT *,
        LAG(balance) OVER (PARTITION BY wallet ORDER BY day) AS prev_balance
    FROM wallet_balances
),
first_exit AS (
    SELECT wallet, MIN(day) AS exit_day
    FROM with_prev
    WHERE balance <= 0 AND prev_balance > 0
    GROUP BY 1
),
holding_periods AS (
    SELECT
        fp.wallet, fp.entry_day, fe.exit_day,
        CASE
            WHEN fe.exit_day IS NOT NULL
            THEN DATE_DIFF('day', fp.entry_day, fe.exit_day)
            ELSE DATE_DIFF('day', fp.entry_day, current_date)
        END AS holding_days,
        CASE WHEN fe.exit_day IS NULL THEN 'active' ELSE 'exited' END AS status
    FROM first_positive fp
    LEFT JOIN first_exit fe ON fp.wallet = fe.wallet
    WHERE fp.entry_day < current_date - INTERVAL '60' DAY  -- exclude wallets created < 60d ago
)
SELECT
    'SPKCC' AS product,
    COUNT(*) AS total_wallets,
    COUNT(*) FILTER (WHERE status = 'active') AS active_wallets,
    COUNT(*) FILTER (WHERE status = 'exited') AS exited_wallets,
    ROUND(AVG(holding_days), 1) AS avg_holding_days,
    APPROX_PERCENTILE(holding_days, 0.5) AS median_holding_days,
    MIN(holding_days) AS min_holding_days,
    MAX(holding_days) AS max_holding_days,
    ROUND(AVG(holding_days) FILTER (WHERE status = 'exited'), 1) AS avg_days_exited,
    ROUND(AVG(holding_days) FILTER (WHERE status = 'active'), 1) AS avg_days_active
FROM holding_periods