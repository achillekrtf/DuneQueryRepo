-- part of a query repo
-- query name: Monthly Holder Growth
-- query link: https://dune.com/queries/6737468


-- ============================================================
-- KPI #4 — 30d Rolling Holder Growth
-- ============================================================

WITH

-- ==========================================================
-- 1. EVM — single scan via UNNEST
-- ==========================================================
evm_raw AS (
    SELECT
        CAST(evt_block_time AS DATE)                        AS day,
        wallet                                              AS address,
        CASE WHEN side = 'in'
             THEN  CAST(value AS DOUBLE) / 1e5
             ELSE -CAST(value AS DOUBLE) / 1e5
        END                                                 AS delta
    FROM evms.erc20_transfers
    CROSS JOIN UNNEST(ARRAY[
        CASE WHEN "to"   != 0x0000000000000000000000000000000000000000 THEN 'in'  END,
        CASE WHEN "from" != 0x0000000000000000000000000000000000000000 THEN 'out' END
    ], ARRAY[
        CASE WHEN "to"   != 0x0000000000000000000000000000000000000000 THEN CAST("to"   AS VARCHAR) END,
        CASE WHEN "from" != 0x0000000000000000000000000000000000000000 THEN CAST("from" AS VARCHAR) END
    ]) AS t(side, wallet)
    WHERE evt_block_time >= TIMESTAMP '2024-04-30'
      AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
      AND contract_address IN (
          0xe4880249745eAc5F1eD9d8F7DF844792D560e750, 0x021289588cd81dC1AC87ea91e91607eEF68303F5,
          0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80, 0xCBeb19549054CC0a6257A77736FC78C367216cE7,
          0xf695Df6c0f3bB45918A7A82e83348FC59517734E, 0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D,
          0x903d5990119bC799423e9C25c56518Ba7DD19474, 0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068,
          0x4f33aCf823E6eEb697180d553cE0c710124C8D59, 0x99F70A0e1786402a6796c6B0AA997ef340a5c6da,
          0x3868D4e336d14D38031cf680329d31e4712e11cC, 0x0e389C83Bc1d16d86412476F6103027555C03265,
          -- SAFO
          0x0990b149e915cb08e2143a5c6f669c907eddc8b0, 0x272ea767712cc4839f4a27ee35eb73116158c8a2,
          0x1412632f2b89e87bfa20c1318a43ced25f1d7b76, 0xd879846cbe20751bde8a9342a3cca00a3e56ca47,
          0x35dfec1813c43d82e6b87c682f560bbb8ea0c121, 0xcbade7d9bdee88411cb6cbcbb29952b742036992,
          0x6f64f47f95cf656f21b40e14798f6b49f80b3dc5, 0x0c709396739b9cfb72bcea6ac691ce0ddf66479c,
          0x0bb754d8940e283d9ff6855ab5dafbc14165c059, 0x5677a4dc7484762ffccee13cba20b5c979def446,
          0xc273986a91e4bfc543610a5cb5860b7cfefb6cc0, 0x4fe515c67eeeadb3282780325f09bb7c244fe774,
          0xbe023308ac2ef7e1c3799f4e6a3003ee6d342635, 0x2f6c0e5e06b43512706a9cdf66cd21f723fe0ec3,
          0xfe20ebe388149fb2e158b9d10cb95bcfa652262d, 0x18b5c15e5196a38a162b1787875295b76e4313fb,
          0x9de2b2dcdcf43540e47143f28484b6d15118f089, 0x97e7962bcd091e7ecfb583fc96289b1e1553ac6e,
          0xd9aa2300e126869182dfb6ecf54984e4c687f36b, 0xef53e7d17822b641c6481837238a64a688709301
      )
      AND side IS NOT NULL
      AND wallet IS NOT NULL
),
evm_balances AS (
    SELECT address,
        SUM(delta) FILTER (WHERE day <= current_date)                      AS bal_now,
        SUM(delta) FILTER (WHERE day <= current_date - INTERVAL '30' DAY) AS bal_30
    FROM evm_raw
    GROUP BY 1
),
evm_counts AS (
    SELECT
        COUNT(*) FILTER (WHERE bal_now > 0.001) AS holders_now,
        COUNT(*) FILTER (WHERE bal_30  > 0.001) AS holders_30
    FROM evm_balances
),

-- ==========================================================
-- 2. STARKNET — single UNNEST scan, cardinality-safe
--    FIX: tracks both sender and receiver for all transfer types
--    FIX: cardinality guard on keys[3] before access
-- ==========================================================
starknet_raw AS (
    SELECT
        block_date AS day,
        wallet     AS address,
        CASE WHEN side = 'in'
             THEN  CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5
             ELSE -CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5
        END        AS delta
    FROM starknet.events
    CROSS JOIN UNNEST(ARRAY[
        CASE WHEN cardinality(keys) >= 3
              AND keys[3] != 0x0000000000000000000000000000000000000000000000000000000000000000
             THEN 'in'  END,
        CASE WHEN cardinality(keys) >= 2
              AND keys[2] != 0x0000000000000000000000000000000000000000000000000000000000000000
             THEN 'out' END
    ], ARRAY[
        CASE WHEN cardinality(keys) >= 3 THEN CAST(keys[3] AS VARCHAR) END,
        CASE WHEN cardinality(keys) >= 2 THEN CAST(keys[2] AS VARCHAR) END
    ]) AS t(side, wallet)
    WHERE block_date >= DATE '2024-04-30'
      AND cardinality(keys) >= 2
      AND cardinality(data) >= 1
      AND keys[1] = 0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9
      AND from_address IN (
          0x020ff2f6021ada9edbceaf31b96f9f67b746662a6e6b2bc9d30c0d3e290a71f6,
          0x04f5e0de717daa6aa8de63b1bf2e8d7823ec5b21a88461b1519d9dbc956fb7f2,
          0x0153d6e0462080bb2842109e9b64f589ef5aa06bb32b26bbdb894aca92674395,
          0x04bade88e79a6120f893d64e51006ac6853eceeefa1a50868d19601b1f0a567d,
          0x06472cabc51a3805975b9c60c7dec63897c9a287f2db173a1d6c589d18dd1e07,
          -- SAFO
          0x0128f41ef8017ab56140ffad6439305a3196ed862841ba61ff4d78e380c346a6,
          0x035bdc17f7a7d09c45d31ab476a576d4f7aad916676b2948fe172c3bcb33725a,
          0x006e8a99926ff6d56f4cb93c37b63286d736cdf1f81740d53f88b4875b4cbe7f49,
          0x06723dcb428eddb160c5adfc2d0a5e5adc184bf6a7298780c3cbf3fa764f709b
      )
      AND side IS NOT NULL
      AND wallet IS NOT NULL
),
starknet_balances AS (
    SELECT address,
        SUM(delta) FILTER (WHERE day <= current_date)                      AS bal_now,
        SUM(delta) FILTER (WHERE day <= current_date - INTERVAL '30' DAY) AS bal_30
    FROM starknet_raw
    GROUP BY 1
),
starknet_counts AS (
    SELECT
        COUNT(*) FILTER (WHERE bal_now > 0.001) AS holders_now,
        COUNT(*) FILTER (WHERE bal_30  > 0.001) AS holders_30
    FROM starknet_balances
),

-- ==========================================================
-- 3. STELLAR — latest balance per wallet at each cutoff
--    (unchanged — correct as written)
-- ==========================================================
stellar_raw AS (
    SELECT
        closed_at_date AS day,
        json_extract_scalar(key_decoded, '$.vec[1].address')                 AS address,
        TRY_CAST(json_extract_scalar(val_decoded, '$.i128') AS DOUBLE) / 1e5 AS balance,
        closed_at
    FROM stellar.contract_data
    WHERE closed_at_date >= DATE '2024-04-30'
      AND contract_id IN (
          'CARUUX2FZNPH6DGJOEUFSIUQWYHNL5AVDV7PMVSHWL7OBYIBFC76F4TO',
          'CBGV2QFQBBGEQRUKUMCPO3SZOHDDYO6SCP5CH6TW7EALKVHCXTMWDDOF',
          'CDT3KU6TQZNOHKNOHNAFFDQZDURVC3MSTL4ML7TUTZGNOPBZCLABP4FR',
          'CDS2GCAQTNQINSCJUJIVBJXILKBWP5PU7LOBGHMP3X47QCQBFKPMTCNT',
          'CDWOB6T7SVSMMQN5V3P2OPTBAXOP7DAZHGVW3PYTZIKHVFKN6TBSXR6A',
          'CBOOCGZSVRSZFRE4U2NWR2B4RXYVJWRCBTGOUD2JPI2TDJPWMTJX7FZP',
          'CDGSC6BA4TCAOVSFQCUEHDMOIIHYYVNYBT6YEARS4MX3ITAHUINVGQHX',
          'CAGYRRKPFSWKM6SJOE4QAAVYMOSHMDS5WOQ4T5A2E6XNCU7LZZKUNQKP',
          'CAJD2IBSP7VO2VYJQUYJSOGPJINTUYV7MQITIINXVPTIH3CCLCUENNMW4'
      )
      AND deleted = false
      AND contract_key_type = 'ScValTypeScvVec'
      AND json_extract_scalar(key_decoded, '$.vec[0].symbol') = 'Balance'
),
stellar_balances AS (
    SELECT address,
        MAX_BY(balance, closed_at) FILTER (WHERE day <= current_date)                      AS bal_now,
        MAX_BY(balance, closed_at) FILTER (WHERE day <= current_date - INTERVAL '30' DAY) AS bal_30
    FROM stellar_raw
    GROUP BY 1
),
stellar_counts AS (
    SELECT
        COUNT(*) FILTER (WHERE bal_now > 0.001) AS holders_now,
        COUNT(*) FILTER (WHERE bal_30  > 0.001) AS holders_30
    FROM stellar_balances
),

-- ==========================================================
-- 4. COMBINE
-- ==========================================================
all_counts AS (
    SELECT holders_now, holders_30 FROM evm_counts
    UNION ALL
    SELECT holders_now, holders_30 FROM starknet_counts
    UNION ALL
    SELECT holders_now, holders_30 FROM stellar_counts
)

SELECT
    current_date                                                                               AS "As of Date",
    current_date - INTERVAL '30' DAY                                                           AS "Date 30d Ago",
    SUM(holders_now)                                                                           AS "Total Holders",
    SUM(holders_30)                                                                            AS "Holders 30d Ago",
    ROUND((CAST(SUM(holders_now) AS DOUBLE) - SUM(holders_30)) / NULLIF(SUM(holders_30), 0) * 100.0, 2) AS "30d Growth (%)"
FROM all_counts