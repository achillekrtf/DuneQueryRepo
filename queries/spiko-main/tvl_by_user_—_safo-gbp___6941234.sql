-- part of a query repo
-- query name: TVL by User — SAFO-GBP
-- query link: https://dune.com/queries/6941234


WITH nav_latest AS (
    SELECT COALESCE(
        MAX(bytearray_to_uint256(bytearray_substring(data, 1+32, 32))) * 1e-6,
        1.0
    ) AS nav
    FROM arbitrum.logs
    WHERE block_date >= current_date - INTERVAL '5' DAY
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0x835B48E97CBF727e23E7AA3bD40248818d20A2b0
),
fx_latest AS (
    SELECT CAST(bytearray_to_int256(topic1) AS DOUBLE) * 1e-8 AS fx_rate
    FROM arbitrum.logs
    WHERE topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f
      AND contract_address = 0x78f28D363533695458696b42577D2e1728cEa3D1
    ORDER BY block_time DESC LIMIT 1
),
evm_net AS (
    SELECT address, SUM(delta) AS net_balance
    FROM (
        SELECT CAST("to" AS VARCHAR) AS address, CAST(value AS DOUBLE)/1e5 AS delta
        FROM evms.erc20_transfers
        WHERE evt_block_time >= TIMESTAMP '2024-04-30'
          AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
          AND "to" != 0x0000000000000000000000000000000000000000
          AND contract_address IN (0xc273986a91e4bfc543610a5cb5860b7cfefb6cc0,0x4fe515c67eeeadb3282780325f09bb7c244fe774,0xbe023308ac2ef7e1c3799f4e6a3003ee6d342635,0x2f6c0e5e06b43512706a9cdf66cd21f723fe0ec3,0xfe20ebe388149fb2e158b9d10cb95bcfa652262d)
        UNION ALL
        SELECT CAST("from" AS VARCHAR), -CAST(value AS DOUBLE)/1e5
        FROM evms.erc20_transfers
        WHERE evt_block_time >= TIMESTAMP '2024-04-30'
          AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
          AND "from" != 0x0000000000000000000000000000000000000000
          AND contract_address IN (0xc273986a91e4bfc543610a5cb5860b7cfefb6cc0,0x4fe515c67eeeadb3282780325f09bb7c244fe774,0xbe023308ac2ef7e1c3799f4e6a3003ee6d342635,0x2f6c0e5e06b43512706a9cdf66cd21f723fe0ec3,0xfe20ebe388149fb2e158b9d10cb95bcfa652262d)
    ) t GROUP BY 1 HAVING SUM(delta) > 0.001
),
starknet_net AS (
    SELECT CASE WHEN keys[2]=0x0000000000000000000000000000000000000000000000000000000000000000 THEN CAST(keys[3] AS VARCHAR) ELSE CAST(keys[2] AS VARCHAR) END AS address,
           SUM(CASE WHEN keys[2]=0x0000000000000000000000000000000000000000000000000000000000000000 THEN CAST(bytearray_to_uint256(data[1]) AS DOUBLE)/1e5
                    WHEN keys[3]=0x0000000000000000000000000000000000000000000000000000000000000000 THEN -CAST(bytearray_to_uint256(data[1]) AS DOUBLE)/1e5 ELSE 0 END) AS net_balance
    FROM starknet.events
    WHERE block_date >= DATE '2024-04-30' AND cardinality(keys)>=3 AND cardinality(data)>=1
      AND keys[1]=0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9
      AND from_address=0x006e8a99926ff6d56f4cb93c37b63286d736cdf1f81740d53f88b4875b4cbe7f49
    GROUP BY 1 HAVING SUM(CASE WHEN keys[2]=0x0000000000000000000000000000000000000000000000000000000000000000 THEN CAST(bytearray_to_uint256(data[1]) AS DOUBLE)/1e5 WHEN keys[3]=0x0000000000000000000000000000000000000000000000000000000000000000 THEN -CAST(bytearray_to_uint256(data[1]) AS DOUBLE)/1e5 ELSE 0 END) > 0.001
),
stellar_net AS (
    SELECT json_extract_scalar(key_decoded,'$.vec[1].address') AS address,
           MAX_BY(TRY_CAST(json_extract_scalar(val_decoded,'$.i128') AS DOUBLE)/1e5, closed_at) AS net_balance
    FROM stellar.contract_data
    WHERE closed_at_date >= DATE '2024-04-30'
      AND contract_id='CAGYRRKPFSWKM6SJOE4QAAVYMOSHMDS5WOQ4T5A2E6XNCU7LZZKUNQKP'
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