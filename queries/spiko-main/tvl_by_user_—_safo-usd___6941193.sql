-- part of a query repo
-- query name: TVL by User — SAFO-USD
-- query link: https://dune.com/queries/6941193


WITH nav_latest AS (
    SELECT COALESCE(
        MAX(bytearray_to_uint256(bytearray_substring(data, 1+32, 32))) * 1e-6,
        1.0
    ) AS nav
    FROM arbitrum.logs
    WHERE block_date >= current_date - INTERVAL '5' DAY
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0x372e37cA79747A2d1671EDBC5f1e2853B96BA351
),
fx_latest AS ( SELECT 1.0 AS fx_rate ),
evm_net AS (
    SELECT address, SUM(delta) AS net_balance
    FROM (
        SELECT CAST("to" AS VARCHAR) AS address, CAST(value AS DOUBLE)/1e5 AS delta
        FROM evms.erc20_transfers
        WHERE evt_block_time >= TIMESTAMP '2024-04-30'
          AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
          AND "to" != 0x0000000000000000000000000000000000000000
          AND contract_address IN (0xcbade7d9bdee88411cb6cbcbb29952b742036992,0x6f64f47f95cf656f21b40e14798f6b49f80b3dc5,0x0c709396739b9cfb72bcea6ac691ce0ddf66479c,0x0bb754d8940e283d9ff6855ab5dafbc14165c059,0x5677a4dc7484762ffccee13cba20b5c979def446)
        UNION ALL
        SELECT CAST("from" AS VARCHAR), -CAST(value AS DOUBLE)/1e5
        FROM evms.erc20_transfers
        WHERE evt_block_time >= TIMESTAMP '2024-04-30'
          AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
          AND "from" != 0x0000000000000000000000000000000000000000
          AND contract_address IN (0xcbade7d9bdee88411cb6cbcbb29952b742036992,0x6f64f47f95cf656f21b40e14798f6b49f80b3dc5,0x0c709396739b9cfb72bcea6ac691ce0ddf66479c,0x0bb754d8940e283d9ff6855ab5dafbc14165c059,0x5677a4dc7484762ffccee13cba20b5c979def446)
    ) t GROUP BY 1 HAVING SUM(delta) > 0.001
),
starknet_net AS (
    SELECT CASE WHEN keys[2]=0x0000000000000000000000000000000000000000000000000000000000000000 THEN CAST(keys[3] AS VARCHAR) ELSE CAST(keys[2] AS VARCHAR) END AS address,
           SUM(CASE WHEN keys[2]=0x0000000000000000000000000000000000000000000000000000000000000000 THEN CAST(bytearray_to_uint256(data[1]) AS DOUBLE)/1e5
                    WHEN keys[3]=0x0000000000000000000000000000000000000000000000000000000000000000 THEN -CAST(bytearray_to_uint256(data[1]) AS DOUBLE)/1e5 ELSE 0 END) AS net_balance
    FROM starknet.events
    WHERE block_date >= DATE '2024-04-30' AND cardinality(keys)>=3 AND cardinality(data)>=1
      AND keys[1]=0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9
      AND from_address=0x035bdc17f7a7d09c45d31ab476a576d4f7aad916676b2948fe172c3bcb33725a
    GROUP BY 1 HAVING SUM(CASE WHEN keys[2]=0x0000000000000000000000000000000000000000000000000000000000000000 THEN CAST(bytearray_to_uint256(data[1]) AS DOUBLE)/1e5 WHEN keys[3]=0x0000000000000000000000000000000000000000000000000000000000000000 THEN -CAST(bytearray_to_uint256(data[1]) AS DOUBLE)/1e5 ELSE 0 END) > 0.001
),
stellar_net AS (
    SELECT json_extract_scalar(key_decoded,'$.vec[1].address') AS address,
           MAX_BY(TRY_CAST(json_extract_scalar(val_decoded,'$.i128') AS DOUBLE)/1e5, closed_at) AS net_balance
    FROM stellar.contract_data
    WHERE closed_at_date >= DATE '2024-04-30'
      AND contract_id='CDGSC6BA4TCAOVSFQCUEHDMOIIHYYVNYBT6YEARS4MX3ITAHUINVGQHX'
      AND deleted=false AND contract_key_type='ScValTypeScvVec'
      AND json_extract_scalar(key_decoded,'$.vec[0].symbol')='Balance'
    GROUP BY 1 HAVING MAX_BY(TRY_CAST(json_extract_scalar(val_decoded,'$.i128') AS DOUBLE)/1e5, closed_at) > 0.001
),
all_holders AS (
    SELECT address, net_balance FROM evm_net UNION ALL
    SELECT address, net_balance FROM starknet_net UNION ALL
    SELECT address, net_balance FROM stellar_net
)
SELECT address, SUM(net_balance) AS "Balance",
       ROUND(SUM(net_balance) * nav * fx_rate, 2) AS "TVL (USD)"
FROM all_holders CROSS JOIN nav_latest CROSS JOIN fx_latest
GROUP BY address, nav, fx_rate
ORDER BY 3 DESC;