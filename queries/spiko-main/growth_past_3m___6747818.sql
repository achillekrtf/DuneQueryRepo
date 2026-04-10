-- part of a query repo
-- query name: Growth past 3m
-- query link: https://dune.com/queries/6747818


-- ============================================================
-- Competitive TVL Growth Rate — 90d Benchmark (Standalone)
-- Spiko vs 8 competitors | EVM chains | DuneSQL
-- ============================================================

WITH
-- ==========================================================
-- 1. RAW MINT/BURN EVENTS — all projects, all EVM chains
--    ✅ Only mint (from=0x0) and burn (to=0x0) → minimal scan
-- ==========================================================
raw_events AS (
    SELECT
        DATE_TRUNC('day', evt_block_time) AS day,
        contract_address,
        CASE
            WHEN "from" = 0x0000000000000000000000000000000000000000
                THEN  CAST(value AS DOUBLE)
            WHEN "to"   = 0x0000000000000000000000000000000000000000
                THEN -CAST(value AS DOUBLE)
        END AS raw_change
    FROM evms.erc20_transfers
    WHERE evt_block_time >= TIMESTAMP '2024-01-01'
      AND ("from" = 0x0000000000000000000000000000000000000000
           OR "to" = 0x0000000000000000000000000000000000000000)
      AND blockchain IN (
          'ethereum','polygon','arbitrum','base','etherlink',
          'bnb','mantle','avalanche_c','optimism','celo'
      )
      AND contract_address IN (
          -- ── Spiko (5 dec) ──
          0xe4880249745eAc5F1eD9d8F7DF844792D560e750,
          0x021289588cd81dC1AC87ea91e91607eEF68303F5,
          0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80,
          0xCBeb19549054CC0a6257A77736FC78C367216cE7,
          0xf695Df6c0f3bB45918A7A82e83348FC59517734E,
          0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D,
          0x903d5990119bC799423e9C25c56518Ba7DD19474,
          0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068,
          0x4f33aCf823E6eEb697180d553cE0c710124C8D59,
          0x99F70A0e1786402a6796c6B0AA997ef340a5c6da,
          0x3868D4e336d14D38031cf680329d31e4712e11cC,
          0x0e389C83Bc1d16d86412476F6103027555C03265,
          0x8226E968eFD24d9bAF156Eca15179D1cc1bFD828,
          -- ── BlackRock BUIDL (6 dec) ──
          0x7712c34205737192402172409a8f7ccef8aa2aec,
          0x6a9da2d710bb9b700acde7cb81f10f1ff8c89041,
          0xa6525ae43edcd03dc08e775774dcabd3bb925872,
          0x53fc82f14f009009b440a706e31c9021e1196a2f,
          0x2d5bdc96d9c8aabbdb38c9a27398513e7e5ef84f,
          0xa1cdab15bba75a80df4089cafba013e376957cf5,
          0x2893ef551b6dd69f661ac00f11d93e5dc5dc0e99,
          -- ── Franklin Templeton BENJI (18 dec) ──
          0x408A634B8a8f0dE729B48574a3a7Ec3fE820B00A,
          0xB9e4765BCE2609bC1949592059B17Ea72fEe6C6A,
          0x5096b85Ed11798fDdCB8b5CB27C399c04689c435,
          0x3DDc84940Ab509C11B20B76B466933f40b750dc9,
          0xe08b4c1005603427420e64252a8b120cace4d122,
          -- ── Ondo: OUSG + rOUSG (18 dec) ──
          0x1B19C19393e2d034D8Ff31ff34c81252FcBbee92,
          0x54043c656F0FAd0652D9Ae2603cDF347c5578d00,
          0xbA11C5effA33c4D6F8f593CFA394241CfE925811,
          -- ── Superstate: USTB + USCC (6 dec) ──
          0x43415eB6ff9DB7E26A15b704e7A3eDCe97d31C4e,
          0x14d60e7fdc0d71d8611742720e4c50e7a974020c,
          -- ── Hashnote USYC (6 dec) ──
          0x136471a34f6ef19fE571EFFC1CA711fdb8E49f2b,
          0x8D0fA28f221eB5735BC71d3a0Da67EE5bC821311,
          -- ── WisdomTree WTGXX (6 dec) ──
          0x1feCF3d9d4Fee7f2c02917A66028a48C6706c179,
          0xfeb26f0943c3885b2cb85a9f933975356c81c33d,
          0x870FD36B3bf7f5abeEEa2C8D4abdF1dc4E33109d,
          0xCF7a8813bD3bdAF70A9f46d310Ce1EE8D80a4F5a,
          -- ── Janus Henderson JTRSY (6 dec) ──
          0x8c213ee79581Ff4984583C6a801e5263418C4b86,
          0x27e8c820d05aea8824b1ac35116f63f9833b54c8,
          -- ── Fidelity FDIT (18 dec) ──
          0x48aB4e39AC59F4E88974804B04A991b3a402717f
      )
),

-- ==========================================================
-- 2. MAP → project + normalize decimals
-- ==========================================================
mapped AS (
    SELECT
        day,
        -- ── Project classification ──
        CASE
            WHEN contract_address IN (
                0xe4880249745eAc5F1eD9d8F7DF844792D560e750,
                0x021289588cd81dC1AC87ea91e91607eEF68303F5,
                0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80,
                0xCBeb19549054CC0a6257A77736FC78C367216cE7,
                0xf695Df6c0f3bB45918A7A82e83348FC59517734E,
                0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D,
                0x903d5990119bC799423e9C25c56518Ba7DD19474,
                0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068,
                0x4f33aCf823E6eEb697180d553cE0c710124C8D59,
                0x99F70A0e1786402a6796c6B0AA997ef340a5c6da,
                0x3868D4e336d14D38031cf680329d31e4712e11cC,
                0x0e389C83Bc1d16d86412476F6103027555C03265,
                0x8226E968eFD24d9bAF156Eca15179D1cc1bFD828
            ) THEN 'Spiko'
            WHEN contract_address IN (
                0x7712c34205737192402172409a8f7ccef8aa2aec,
                0x6a9da2d710bb9b700acde7cb81f10f1ff8c89041,
                0xa6525ae43edcd03dc08e775774dcabd3bb925872,
                0x53fc82f14f009009b440a706e31c9021e1196a2f,
                0x2d5bdc96d9c8aabbdb38c9a27398513e7e5ef84f,
                0xa1cdab15bba75a80df4089cafba013e376957cf5,
                0x2893ef551b6dd69f661ac00f11d93e5dc5dc0e99
            ) THEN 'BlackRock (BUIDL)'
            WHEN contract_address IN (
                0x408A634B8a8f0dE729B48574a3a7Ec3fE820B00A,
                0xB9e4765BCE2609bC1949592059B17Ea72fEe6C6A,
                0x5096b85Ed11798fDdCB8b5CB27C399c04689c435,
                0x3DDc84940Ab509C11B20B76B466933f40b750dc9,
                0xe08b4c1005603427420e64252a8b120cace4d122
            ) THEN 'Franklin Templeton (BENJI)'
            WHEN contract_address IN (
                0x1B19C19393e2d034D8Ff31ff34c81252FcBbee92,
                0x54043c656F0FAd0652D9Ae2603cDF347c5578d00,
                0xbA11C5effA33c4D6F8f593CFA394241CfE925811
            ) THEN 'Ondo (OUSG)'
            WHEN contract_address IN (
                0x43415eB6ff9DB7E26A15b704e7A3eDCe97d31C4e,
                0x14d60e7fdc0d71d8611742720e4c50e7a974020c
            ) THEN 'Superstate (USTB + USCC)'
            WHEN contract_address IN (
                0x136471a34f6ef19fE571EFFC1CA711fdb8E49f2b,
                0x8D0fA28f221eB5735BC71d3a0Da67EE5bC821311
            ) THEN 'Hashnote (USYC)'
            WHEN contract_address IN (
                0x1feCF3d9d4Fee7f2c02917A66028a48C6706c179,
                0xfeb26f0943c3885b2cb85a9f933975356c81c33d,
                0x870FD36B3bf7f5abeEEa2C8D4abdF1dc4E33109d,
                0xCF7a8813bD3bdAF70A9f46d310Ce1EE8D80a4F5a
            ) THEN 'WisdomTree (WTGXX)'
            WHEN contract_address IN (
                0x8c213ee79581Ff4984583C6a801e5263418C4b86,
                0x27e8c820d05aea8824b1ac35116f63f9833b54c8
            ) THEN 'Janus Henderson (JTRSY)'
            WHEN contract_address = 0x48aB4e39AC59F4E88974804B04A991b3a402717f
            THEN 'Fidelity (FDIT)'
        END AS project,
        -- ── Normalized supply change ──
        raw_change / CASE
            WHEN contract_address IN (
                0xe4880249745eAc5F1eD9d8F7DF844792D560e750,
                0x021289588cd81dC1AC87ea91e91607eEF68303F5,
                0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80,
                0xCBeb19549054CC0a6257A77736FC78C367216cE7,
                0xf695Df6c0f3bB45918A7A82e83348FC59517734E,
                0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D,
                0x903d5990119bC799423e9C25c56518Ba7DD19474,
                0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068,
                0x4f33aCf823E6eEb697180d553cE0c710124C8D59,
                0x99F70A0e1786402a6796c6B0AA997ef340a5c6da,
                0x3868D4e336d14D38031cf680329d31e4712e11cC,
                0x0e389C83Bc1d16d86412476F6103027555C03265,
                0x8226E968eFD24d9bAF156Eca15179D1cc1bFD828
            ) THEN 1e5
            WHEN contract_address IN (
                -- Ondo (18 dec)
                0x1B19C19393e2d034D8Ff31ff34c81252FcBbee92,
                0x54043c656F0FAd0652D9Ae2603cDF347c5578d00,
                0xbA11C5effA33c4D6F8f593CFA394241CfE925811,
                -- FDIT (18 dec)
                0x48aB4e39AC59F4E88974804B04A991b3a402717f,
                -- Franklin Templeton BENJI (18 dec)
                0x408A634B8a8f0dE729B48574a3a7Ec3fE820B00A,
                0xB9e4765BCE2609bC1949592059B17Ea72fEe6C6A,
                0x5096b85Ed11798fDdCB8b5CB27C399c04689c435,
                0x3DDc84940Ab509C11B20B76B466933f40b750dc9,
                0xe08b4c1005603427420e64252a8b120cace4d122,
                -- WisdomTree WTGXX (18 dec)
                0x1feCF3d9d4Fee7f2c02917A66028a48C6706c179,
                0xfeb26f0943c3885b2cb85a9f933975356c81c33d,
                0x870FD36B3bf7f5abeEEa2C8D4abdF1dc4E33109d,
                0xCF7a8813bD3bdAF70A9f46d310Ce1EE8D80a4F5a
            ) THEN 1e18  -- Ondo, FDIT, BENJI, WTGXX
            ELSE 1e6    -- BUIDL, Superstate, USYC, JTRSY
        END AS change_tokens
    FROM raw_events
    WHERE raw_change IS NOT NULL
),

-- ==========================================================
-- 2b. STARKNET MINT/BURN — Spiko only (5 dec)
--     Transfer event: keys[1]=selector, keys[2]=from, keys[3]=to
--     data[1]=amount_low, data[2]=amount_high
-- ==========================================================
starknet_daily_supply AS (
    SELECT
        DATE_TRUNC('day', block_time) AS day,
        'Spiko'                       AS project,
        SUM(
            CASE
                -- mint: from = 0x0 (keys[2] = zero address as varbinary)
                WHEN keys[2] = 0x0000000000000000000000000000000000000000000000000000000000000000
                THEN  CAST(bytearray_to_bigint(bytearray_substring(data[1], 25, 8)) AS DOUBLE) / 1e5
                -- burn: to = 0x0 (keys[3] = zero address as varbinary)
                ELSE -CAST(bytearray_to_bigint(bytearray_substring(data[1], 25, 8)) AS DOUBLE) / 1e5
            END
        ) AS daily_change
    FROM starknet.events
    WHERE block_time >= TIMESTAMP '2024-01-01'
      -- Transfer event selector (felt252, padded to 32 bytes)
      AND keys[1] = 0x0099cd8bde557814842a3121e8ddfd433a539b8c9f4bd1e6a03e51b3af0ccd13
      -- from_address = the contract that emitted the event (Dune starknet.events schema)
      AND from_address IN (
          0x020ff2f6021ada9edbceaf31b96f9f67b746662a6e6b2bc9d30c0d3e290a71f6,  -- USTBL
          0x04f5e0de717daa6aa8de63b1bf2e8d7823ec5b21a88461b1519d9dbc956fb7f2,  -- EUTBL
          0x0153d6e0462080bb2842109e9b64f589ef5aa06bb32b26bbdb894aca92674395,  -- UKTBL
          0x04bade88e79a6120f893d64e51006ac6853eceeefa1a50868d19601b1f0a567d,  -- SPKCC
          0x06472cabc51a3805975b9c60c7dec63897c9a287f2db173a1d6c589d18dd1e07   -- eurSPKCC
      )
      AND (
          keys[2] = 0x0000000000000000000000000000000000000000000000000000000000000000
          OR keys[3] = 0x0000000000000000000000000000000000000000000000000000000000000000
      )
    GROUP BY 1, 2
),
-- ==========================================================
-- 2c. STELLAR SUPPLY — Spiko (stellar.contract_data snapshots)
--     Same methodology as TVL Daily Query (balance delta approach)
--     Exclude Spiko redemption vault: CAVZK26...
-- ==========================================================
stellar_snapshot AS (
    SELECT
        closed_at,
        DATE_TRUNC('day', closed_at) AS day,
        contract_id,
        json_extract_scalar(key_decoded, '$.vec[1].address') AS holder,
        TRY_CAST(json_extract_scalar(val_decoded, '$.i128') AS DOUBLE) / 1e5 AS balance
    FROM stellar.contract_data
    WHERE closed_at_date >= DATE '2024-01-01'
      AND contract_id IN (
          'CARUUX2FZNPH6DGJOEUFSIUQWYHNL5AVDV7PMVSHWL7OBYIBFC76F4TO',  -- USTBL
          'CBGV2QFQBBGEQRUKUMCPO3SZOHDDYO6SCP5CH6TW7EALKVHCXTMWDDOF',  -- EUTBL
          'CDT3KU6TQZNOHKNOHNAFFDQZDURVC3MSTL4ML7TUTZGNOPBZCLABP4FR',  -- UKTBL
          'CDS2GCAQTNQINSCJUJIVBJXILKBWP5PU7LOBGHMP3X47QCQBFKPMTCNT',  -- SPKCC
          'CDWOB6T7SVSMMQN5V3P2OPTBAXOP7DAZHGVW3PYTZIKHVFKN6TBSXR6A'   -- eurSPKCC
      )
      AND deleted = false
      AND contract_key_type = 'ScValTypeScvVec'
      AND json_extract_scalar(key_decoded, '$.vec[0].symbol') = 'Balance'
      -- Exclude redemption vault from circulating supply
      AND json_extract_scalar(key_decoded, '$.vec[1].address')
          != 'CAVZK26ERVGKGXLQGEOKPAQGIZS2YKN4BSXOYDMUY365EV66ZNSFEDBS'
),
stellar_deltas AS (
    SELECT
        day, contract_id, holder,
        balance - COALESCE(
            LAG(balance) OVER (PARTITION BY contract_id, holder ORDER BY closed_at), 0
        ) AS delta
    FROM stellar_snapshot
),
stellar_daily_supply AS (
    SELECT day, 'Spiko' AS project, SUM(delta) AS daily_change
    FROM stellar_deltas
    GROUP BY 1, 2
),
-- ==========================================================
-- 2d. SOLANA SUPPLY — Competitors (action = mint/burn)
--     ⚠️ FIX: from_owner IS NULL captait les transfers → action column
--     ⚠️ amount est en raw units → diviser par 1e6 (SPL standard 6 dec)
-- ==========================================================
solana_supply AS (
    SELECT
        DATE_TRUNC('day', block_time) AS day,
        CASE token_mint_address
            WHEN 'GyWgeqpy5GueU2YbkE8xqUeVEokCMMCEeUrfbtMw6phr' THEN 'BlackRock (BUIDL)'
            WHEN '5Tu84fKBpe9vfXeotjvfvWdWbAjy3hqsExvuHgFqFxA1'  THEN 'Franklin Templeton (BENJI)'
            WHEN 'i7u4r16TcsJTgq1kAG8opmVZyVnAKBwLKu6ZPMwzxNc'  THEN 'Ondo (OUSG)'

            WHEN '7LWanZteUKtvFjv4MHYgKXXdAuCQYFPJysL9pxxdRQGn'  THEN 'Hashnote (USYC)'
        END AS project,
        SUM(CASE
            WHEN action = 'mint' THEN  amount / 1e6
            WHEN action = 'burn' THEN -amount / 1e6
        END) AS daily_change
    FROM tokens_solana.transfers
    WHERE block_time >= TIMESTAMP '2024-01-01'
      AND token_mint_address IN (
          'GyWgeqpy5GueU2YbkE8xqUeVEokCMMCEeUrfbtMw6phr',  -- BUIDL
          '5Tu84fKBpe9vfXeotjvfvWdWbAjy3hqsExvuHgFqFxA1',  -- BENJI
          'i7u4r16TcsJTgq1kAG8opmVZyVnAKBwLKu6ZPMwzxNc',  -- OUSG

          '7LWanZteUKtvFjv4MHYgKXXdAuCQYFPJysL9pxxdRQGn'   -- USYC
      )
      AND action IN ('mint', 'burn')
    GROUP BY 1, 2
),
-- ==========================================================
-- 3. DAILY NET SUPPLY per project (EVM + Starknet + Stellar + Solana)
-- ==========================================================
daily_supply AS (
    SELECT day, project, SUM(daily_change) AS daily_change
    FROM (
        -- EVM (all chains)
        SELECT day, project, SUM(change_tokens) AS daily_change
        FROM mapped
        WHERE project IS NOT NULL
        GROUP BY 1, 2
        UNION ALL
        -- Starknet (Spiko)
        SELECT day, project, daily_change
        FROM starknet_daily_supply
        UNION ALL
        -- Stellar (Spiko + BENJI when contract added)
        SELECT day, project, daily_change
        FROM stellar_daily_supply
        UNION ALL
        -- Solana (BUIDL, BENJI, OUSG, USYC) — concurrents uniquement
        SELECT day, project, daily_change
        FROM solana_supply
        WHERE project IS NOT NULL
    ) combined
    GROUP BY 1, 2
),

-- ==========================================================
-- 4. CUMULATIVE SUPPLY per project
--    Cross join calendar × projects so every day has a row
--    (days with no activity carry forward the last supply)
-- ==========================================================
growth_calendar AS (
    SELECT date_column AS day
    FROM (VALUES (sequence(DATE '2024-01-01', current_date, INTERVAL '1' DAY))) AS t(date_array)
    CROSS JOIN UNNEST(date_array) AS t2(date_column)
),
all_projects_list AS (
    SELECT DISTINCT project FROM daily_supply
),
cumulative AS (
    SELECT
        c.day,
        p.project,
        SUM(COALESCE(ds.daily_change, 0)) OVER (
            PARTITION BY p.project ORDER BY c.day
        ) AS total_supply
    FROM growth_calendar c
    CROSS JOIN all_projects_list p
    LEFT JOIN daily_supply ds ON c.day = ds.day AND p.project = ds.project
),

-- ==========================================================
-- 5. REFERENCE SUPPLY at T-90 (start of window)
-- ==========================================================
ref_supply AS (
    SELECT
        project,
        total_supply AS supply_start
    FROM (
        SELECT
            project,
            total_supply,
            ROW_NUMBER() OVER (
                PARTITION BY project
                ORDER BY ABS(
                    DATE_DIFF('day', day, current_date - INTERVAL '90' DAY)
                )
            ) AS rk
        FROM cumulative
    )
    WHERE rk = 1
)

-- ==========================================================
-- 6. OUTPUT — last 90 days, indexed to 100
-- ==========================================================
SELECT
    c.day,
    c.project,
    ROUND(c.total_supply, 0)                                           AS "Total Supply (tokens)",
    ROUND(c.total_supply / NULLIF(r.supply_start, 0) * 100, 2)        AS "TVL Index (100 = T-90)",
    ROUND((c.total_supply / NULLIF(r.supply_start, 0) - 1) * 100, 2)  AS "Growth Rate %"
FROM cumulative c
JOIN ref_supply r ON c.project = r.project
WHERE c.day >= current_date - INTERVAL '90' DAY
  AND r.supply_start > 0  -- only projects that existed at T-90
ORDER BY c.day DESC, c.project