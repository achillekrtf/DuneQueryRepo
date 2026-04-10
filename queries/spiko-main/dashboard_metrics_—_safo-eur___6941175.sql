-- part of a query repo
-- query name: Dashboard Metrics — SAFO-EUR
-- query link: https://dune.com/queries/6941175


WITH
fx_latest AS (
    SELECT CAST(bytearray_to_int256(topic1) AS DOUBLE) * 1e-8 AS fx_rate
    FROM arbitrum.logs
    WHERE topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f
      AND contract_address = 0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871
    ORDER BY block_time DESC LIMIT 1
),
nav_latest AS (
    SELECT COALESCE(
        MAX(bytearray_to_uint256(bytearray_substring(data, 1+32, 32))) * 1e-6,
        1.0
    ) AS nav
    FROM arbitrum.logs
    WHERE block_date >= current_date - INTERVAL '5' DAY
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0x385D443ffA5b6Fb462b988D023a5DC3b37Ef1644
),
evm_transfers AS (
    SELECT CAST("to" AS VARCHAR) AS address, CAST(value AS DOUBLE)/1e5 AS amount, DATE(evt_block_time) AS tx_date
    FROM evms.erc20_transfers
    WHERE evt_block_time >= TIMESTAMP '2024-04-30'
      AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
      AND "to" != 0x0000000000000000000000000000000000000000
      AND contract_address IN (0x0990b149e915cb08e2143a5c6f669c907eddc8b0,0x272ea767712cc4839f4a27ee35eb73116158c8a2,0x1412632f2b89e87bfa20c1318a43ced25f1d7b76,0xd879846cbe20751bde8a9342a3cca00a3e56ca47,0x35dfec1813c43d82e6b87c682f560bbb8ea0c121)
    UNION ALL
    SELECT CAST("from" AS VARCHAR), -CAST(value AS DOUBLE)/1e5, DATE(evt_block_time)
    FROM evms.erc20_transfers
    WHERE evt_block_time >= TIMESTAMP '2024-04-30'
      AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
      AND "from" != 0x0000000000000000000000000000000000000000
      AND contract_address IN (0x0990b149e915cb08e2143a5c6f669c907eddc8b0,0x272ea767712cc4839f4a27ee35eb73116158c8a2,0x1412632f2b89e87bfa20c1318a43ced25f1d7b76,0xd879846cbe20751bde8a9342a3cca00a3e56ca47,0x35dfec1813c43d82e6b87c682f560bbb8ea0c121)
),
starknet_transfers AS (
    SELECT CASE WHEN keys[2]=0x0000000000000000000000000000000000000000000000000000000000000000 THEN CAST(keys[3] AS VARCHAR) ELSE CAST(keys[2] AS VARCHAR) END AS address,
           CASE WHEN keys[2]=0x0000000000000000000000000000000000000000000000000000000000000000 THEN CAST(bytearray_to_uint256(data[1]) AS DOUBLE)/1e5
                WHEN keys[3]=0x0000000000000000000000000000000000000000000000000000000000000000 THEN -CAST(bytearray_to_uint256(data[1]) AS DOUBLE)/1e5 ELSE 0 END AS amount,
           DATE(block_time) AS tx_date
    FROM starknet.events
    WHERE block_date >= DATE '2024-04-30' AND cardinality(keys)>=3 AND cardinality(data)>=1
      AND keys[1]=0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9
      AND from_address=0x0128f41ef8017ab56140ffad6439305a3196ed862841ba61ff4d78e380c346a6
),
all_transfers AS (
    SELECT address, amount, tx_date FROM evm_transfers UNION ALL
    SELECT address, amount, tx_date FROM starknet_transfers
),
wallet_agg AS (
    SELECT address, SUM(amount) AS net_balance, MIN(tx_date) AS entry_day, MAX(tx_date) AS last_day
    FROM all_transfers GROUP BY 1 HAVING SUM(amount) > 0.001
),
stellar_agg AS (
    SELECT json_extract_scalar(key_decoded,'$.vec[1].address') AS address,
           MAX_BY(TRY_CAST(json_extract_scalar(val_decoded,'$.i128') AS DOUBLE)/1e5, closed_at) AS net_balance
    FROM stellar.contract_data
    WHERE closed_at_date >= DATE '2024-04-30'
      AND contract_id='CBOOCGZSVRSZFRE4U2NWR2B4RXYVJWRCBTGOUD2JPI2TDJPWMTJX7FZP'
      AND deleted=false AND contract_key_type='ScValTypeScvVec'
      AND json_extract_scalar(key_decoded,'$.vec[0].symbol')='Balance'
    GROUP BY 1 HAVING MAX_BY(TRY_CAST(json_extract_scalar(val_decoded,'$.i128') AS DOUBLE)/1e5, closed_at) > 0.001
),
all_holders AS (
    SELECT address, net_balance,
           DATE_DIFF('day', entry_day, current_date) AS holding_days,
           CASE WHEN last_day >= current_date - INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_active
    FROM wallet_agg
    UNION ALL
    SELECT address, net_balance, NULL AS holding_days, NULL AS is_active
    FROM stellar_agg
)
SELECT
    COUNT(*)                                                          AS total_wallets,
    COUNT(CASE WHEN is_active=1 THEN 1 END)                           AS active_wallets,
    ROUND(AVG(holding_days), 1)                                       AS avg_holding_days,
    ROUND(approx_percentile(CAST(holding_days AS DOUBLE), 0.5), 0)    AS median_holding_days,
    ROUND(AVG(net_balance * nav * fx_rate), 2)                              AS "Avg TVL per User (USD)",
    ROUND(approx_percentile(net_balance * nav * fx_rate, 0.5), 2)           AS "Median TVL per User (USD)"
FROM all_holders CROSS JOIN fx_latest CROSS JOIN nav_latest;