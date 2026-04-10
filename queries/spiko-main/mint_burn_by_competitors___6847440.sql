-- part of a query repo
-- query name: Mint Burn by Competitors
-- query link: https://dune.com/queries/6847440


-- ============================================================
-- Competitive Mints, Burns & Transfers — by Brand
-- EVM + Starknet + Stellar (Spiko) + Solana | DuneSQL
-- ============================================================

WITH

-- ==========================================================
-- 1. FUND → CONTRACT MAPPING (Spiko + Competitors)
-- ==========================================================
fund_map AS (
    SELECT * FROM (VALUES
        -- Spiko
        (0xe4880249745eAc5F1eD9d8F7DF844792D560e750, 'ethereum',    'Spiko (USTBL)', 5),
        (0xe4880249745eAc5F1eD9d8F7DF844792D560e750, 'polygon',     'Spiko (USTBL)', 5),
        (0xe4880249745eAc5F1eD9d8F7DF844792D560e750, 'base',        'Spiko (USTBL)', 5),
        (0x021289588cd81dC1AC87ea91e91607eEF68303F5, 'arbitrum',    'Spiko (USTBL)', 5),
        (0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80, 'ethereum',    'Spiko (EUTBL)', 5),
        (0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80, 'polygon',     'Spiko (EUTBL)', 5),
        (0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80, 'base',        'Spiko (EUTBL)', 5),
        (0xCBeb19549054CC0a6257A77736FC78C367216cE7, 'arbitrum',    'Spiko (EUTBL)', 5),
        (0xf695Df6c0f3bB45918A7A82e83348FC59517734E, 'ethereum',    'Spiko (UKTBL)', 5),
        (0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D, 'polygon',     'Spiko (UKTBL)', 5),
        (0x903d5990119bC799423e9C25c56518Ba7DD19474, 'arbitrum',    'Spiko (UKTBL)', 5),
        (0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068, 'base',        'Spiko (UKTBL)', 5),
        (0x4f33aCf823E6eEb697180d553cE0c710124C8D59, 'ethereum',    'Spiko (SPKCC)', 5),
        (0x903d5990119bC799423e9C25c56518Ba7DD19474, 'polygon',     'Spiko (SPKCC)', 5),
        (0x99F70A0e1786402a6796c6B0AA997ef340a5c6da, 'arbitrum',    'Spiko (SPKCC)', 5),
        (0xf695Df6c0f3bB45918A7A82e83348FC59517734E, 'base',        'Spiko (SPKCC)', 5),
        (0x3868D4e336d14D38031cf680329d31e4712e11cC, 'ethereum',    'Spiko (eurSPKCC)', 5),
        (0x99F70A0e1786402a6796c6B0AA997ef340a5c6da, 'polygon',     'Spiko (eurSPKCC)', 5),
        (0x0e389C83Bc1d16d86412476F6103027555C03265, 'arbitrum',    'Spiko (eurSPKCC)', 5),
        (0x4f33aCf823E6eEb697180d553cE0c710124C8D59, 'base',        'Spiko (eurSPKCC)', 5),
        -- BUIDL (BlackRock)
        (0x7712c34205737192402172409a8f7ccef8aa2aec, 'ethereum',    'BUIDL (BlackRock)', 6),
        (0x6a9da2d710bb9b700acde7cb81f10f1ff8c89041, 'ethereum',    'BUIDL (BlackRock)', 6),
        (0xa6525ae43edcd03dc08e775774dcabd3bb925872, 'arbitrum',    'BUIDL (BlackRock)', 6),
        (0x53fc82f14f009009b440a706e31c9021e1196a2f, 'avalanche_c', 'BUIDL (BlackRock)', 6),
        (0x2d5bdc96d9c8aabbdb38c9a27398513e7e5ef84f, 'bnb',         'BUIDL (BlackRock)', 6),
        (0xa1cdab15bba75a80df4089cafba013e376957cf5, 'optimism',    'BUIDL (BlackRock)', 6),
        (0x2893ef551b6dd69f661ac00f11d93e5dc5dc0e99, 'polygon',     'BUIDL (BlackRock)', 6),
        -- BENJI (Franklin Templeton)
        (0x3ddc84940ab509c11b20b76b466933f40b750dc9, 'ethereum',    'BENJI (Franklin)', 18),
        (0xb9e4765bce2609bc1949592059b17ea72fee6c6a, 'arbitrum',    'BENJI (Franklin)', 18),
        (0xe08b4c1005603427420e64252a8b120cace4d122, 'avalanche_c', 'BENJI (Franklin)', 18),
        (0x60cfc2b186a4cf647486e42c42b11cc6d571d1e4, 'base',        'BENJI (Franklin)', 18),
        (0x408a634b8a8f0de729b48574a3a7ec3fe820b00a, 'polygon',     'BENJI (Franklin)', 18),
        -- Ondo
        (0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f, 'ethereum',    'OUSG (Ondo)', 18),
        -- Hashnote USYC
        (0x136471a34f6ef19fe571effc1ca711fdb8e49f2b, 'ethereum',    'USYC (Hashnote)', 6),
        (0x8d0fa28f221eb5735bc71d3a0da67ee5bc821311, 'bnb',         'USYC (Hashnote)', 6),
        -- Superstate
        (0x43415eb6ff9db7e26a15b704e7a3edce97d31c4e, 'ethereum',    'USTB (Superstate)', 6),
        (0xe4fa682f94610ccd170680cc3b045d77d9e528a8, 'plume',       'USTB (Superstate)', 6),
        (0x14d60e7fdc0d71d8611742720e4c50e7a974020c, 'ethereum',    'USCC (Superstate)', 6),
        (0x4c21B7577C8FE8b0B0669165ee7C8f67fa1454Cf, 'plume',       'USCC (Superstate)', 6),
        -- WisdomTree WTGXX
        (0x1fecf3d9d4fee7f2c02917a66028a48c6706c179, 'ethereum',    'WTGXX (WisdomTree)', 18),
        (0xfeb26f0943c3885b2cb85a9f933975356c81c33d, 'arbitrum',    'WTGXX (WisdomTree)', 18),
        (0x5096b85ed11798fddcb8b5cb27c399c04689c435, 'base',        'WTGXX (WisdomTree)', 18),
        (0x870fd36b3bf7f5abeeea2c8d4abdf1dc4e33109d, 'optimism',    'WTGXX (WisdomTree)', 18),
        (0x870fd36b3bf7f5abeeea2c8d4abdf1dc4e33109d, 'avalanche_c', 'WTGXX (WisdomTree)', 18),
        (0xcf7a8813bd3bdaf70a9f46d310ce1ee8d80a4f5a, 'plume',       'WTGXX (WisdomTree)', 18),
        -- Janus Henderson JTRSY
        (0x8c213ee79581ff4984583c6a801e5263418c4b86, 'ethereum',    'JTRSY (Janus)', 6),
        (0x8c213ee79581ff4984583c6a801e5263418c4b86, 'base',        'JTRSY (Janus)', 6),
        (0x27e8c820d05aea8824b1ac35116f63f9833b54c8, 'celo',        'JTRSY (Janus)', 6),
        -- Fidelity FDIT
        (0x48ab4e39ac59f4e88974804b04a991b3a402717f, 'ethereum',    'FDIT (Fidelity)', 18)
    ) AS t(contract_address, blockchain, fund, decimals)
),

-- ==========================================================
-- 2. DECIMALS + BRAND DERIVATION
-- ==========================================================
fund_info AS (
    SELECT
        fm.contract_address, fm.blockchain, fm.fund,
        fm.decimals,
        CASE
            WHEN fm.fund LIKE 'Spiko%'           THEN 'Spiko'
            WHEN fm.fund LIKE 'BUIDL%'           THEN 'BUIDL'
            WHEN fm.fund LIKE 'BENJI%'           THEN 'BENJI'
            WHEN fm.fund LIKE 'OUSG%'            THEN 'OUSG'
            WHEN fm.fund LIKE 'USYC%'            THEN 'USYC'
            WHEN fm.fund LIKE 'USTB%'            THEN 'USTB'
            WHEN fm.fund LIKE 'USCC%'            THEN 'USCC'
            WHEN fm.fund LIKE 'WTGXX%'           THEN 'WTGXX'
            WHEN fm.fund LIKE 'JTRSY%'           THEN 'JTRSY'
            WHEN fm.fund LIKE 'FDIT%'            THEN 'FDIT'
        END AS brand
    FROM fund_map fm
),

-- ==========================================================
-- 3a. EVM TRANSFERS
-- ==========================================================
evm_transfers AS (
    SELECT
        DATE_TRUNC('day', e.evt_block_time) AS day,
        fi.brand,
        CASE
            WHEN e."from" = 0x0000000000000000000000000000000000000000 THEN 'mint'
            WHEN e."to"   = 0x0000000000000000000000000000000000000000 THEN 'burn'
            ELSE 'transfer'
        END AS tx_type,
        CAST(e.value AS DOUBLE) / POWER(10, fi.decimals) AS volume_tokens
    FROM evms.erc20_transfers e
    INNER JOIN fund_info fi
        ON e.contract_address = fi.contract_address
        AND e.blockchain = fi.blockchain
    WHERE e.evt_block_time >= TIMESTAMP '2024-01-01'
      AND e.blockchain IN (
          'ethereum','polygon','arbitrum','base','optimism',
          'avalanche_c','bnb','mantle','celo','plume'
      )
      AND CAST(e.value AS DOUBLE) / POWER(10, fi.decimals) >= 50
),

-- ==========================================================
-- 3b. STARKNET TRANSFERS (Spiko only)
--     FIX 1: correct Transfer topic (0x...96e9, aligned with TVL query)
--     FIX 2: use bytearray_to_uint256(data[1]) for amount
--     FIX 3: cardinality guard before keys[3] access
--     keys[2]=from, keys[3]=to
--     mint: keys[2]=0x0 | burn: keys[3]=0x0 | else: transfer
-- ==========================================================
starknet_transfers AS (
    SELECT DATE_TRUNC('day', block_time) AS day,
        'Spiko' AS brand,
        CASE
            WHEN keys[2] = 0x0000000000000000000000000000000000000000000000000000000000000000
                THEN 'mint'
            WHEN cardinality(keys) >= 3
                 AND keys[3] = 0x0000000000000000000000000000000000000000000000000000000000000000
                THEN 'burn'
            ELSE 'transfer'
        END AS tx_type,
        CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5 AS volume_tokens
    FROM starknet.events
    WHERE block_time >= TIMESTAMP '2024-01-01'
      AND cardinality(keys) >= 2
      AND cardinality(data) >= 1
      AND keys[1] = 0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9
      AND from_address IN (
          0x020ff2f6021ada9edbceaf31b96f9f67b746662a6e6b2bc9d30c0d3e290a71f6,
          0x04f5e0de717daa6aa8de63b1bf2e8d7823ec5b21a88461b1519d9dbc956fb7f2,
          0x0153d6e0462080bb2842109e9b64f589ef5aa06bb32b26bbdb894aca92674395,
          0x04bade88e79a6120f893d64e51006ac6853eceeefa1a50868d19601b1f0a567d,
          0x06472cabc51a3805975b9c60c7dec63897c9a287f2db173a1d6c589d18dd1e07
      )
      AND CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5 >= 50
),

-- ==========================================================
-- 3c. STELLAR TRANSFERS (Spiko only — balance delta proxy)
--     NOTE: cannot distinguish mint/burn from wallet-to-wallet
--     transfers using balance snapshots. A send from A→B appears
--     as -delta on A (burn) and +delta on B (mint).
--     Net issuance is correct; individual mint/burn counts are
--     overstated by transfer activity.
-- ==========================================================
stl_raw AS (
    SELECT
        closed_at,
        DATE_TRUNC('day', closed_at) AS day,
        json_extract_scalar(key_decoded, '$.vec[1].address') AS holder,
        TRY_CAST(json_extract_scalar(val_decoded, '$.i128') AS DOUBLE) / 1e5 AS balance
    FROM stellar.contract_data
    WHERE closed_at_date >= DATE '2024-01-01'
      AND contract_id IN (
          'CARUUX2FZNPH6DGJOEUFSIUQWYHNL5AVDV7PMVSHWL7EALKVHCXTMWDDOF',
          'CBGV2QFQBBGEQRUKUMCPO3SZOHDDYO6SCP5CH6TW7EALKVHCXTMWDDOF',
          'CDT3KU6TQZNOHKNOHNAFFDQZDURVC3MSTL4ML7TUTZGNOPBZCLABP4FR',
          'CDS2GCAQTNQINSCJUJIVBJXILKBWP5PU7LOBGHMP3X47QCQBFKPMTCNT',
          'CDWOB6T7SVSMMQN5V3P2OPTBAXOP7DAZHGVW3PYTZIKHVFKN6TBSXR6A'
      )
      AND deleted = false AND contract_key_type = 'ScValTypeScvVec'
      AND json_extract_scalar(key_decoded, '$.vec[0].symbol') = 'Balance'
      AND json_extract_scalar(key_decoded, '$.vec[1].address')
          != 'CAVZK26ERVGKGXLQGEOKPAQGIZS2YKN4BSXOYDMUY365EV66ZNSFEDBS'
),
stl_deltas AS (
    SELECT day, holder,
        balance - COALESCE(
            LAG(balance) OVER (PARTITION BY holder ORDER BY closed_at), 0
        ) AS delta
    FROM stl_raw
),
stellar_transfers AS (
    SELECT day,
        'Spiko' AS brand,
        CASE WHEN delta > 0 THEN 'mint' ELSE 'burn' END AS tx_type,
        ABS(delta) AS volume_tokens
    FROM stl_deltas
    WHERE delta != 0
      AND ABS(delta) >= 50
),

-- ==========================================================
-- 3d. SOLANA TRANSFERS (BUIDL, BENJI, OUSG, USYC)
-- ==========================================================
solana_transfers AS (
    SELECT
        DATE_TRUNC('day', block_time) AS day,
        CASE token_mint_address
            WHEN 'GyWgeqpy5GueU2YbkE8xqUeVEokCMMCEeUrfbtMw6phr' THEN 'BUIDL'
            WHEN '5Tu84fKBpe9vfXeotjvfvWdWbAjy3hqsExvuHgFqFxA1'  THEN 'BENJI'
            WHEN 'i7u4r16TcsJTgq1kAG8opmVZyVnAKBwLKu6ZPMwzxNc'   THEN 'OUSG'
            WHEN '7LWanZteUKtvFjv4MHYgKXXdAuCQYFPJysL9pxxdRQGn'  THEN 'USYC'
        END AS brand,
        CASE
            WHEN from_owner IS NULL THEN 'mint'
            WHEN to_owner   IS NULL THEN 'burn'
            ELSE 'transfer'
        END AS tx_type,
        amount AS volume_tokens
    FROM tokens_solana.transfers
    WHERE block_time >= TIMESTAMP '2024-01-01'
      AND token_mint_address IN (
          'GyWgeqpy5GueU2YbkE8xqUeVEokCMMCEeUrfbtMw6phr',
          '5Tu84fKBpe9vfXeotjvfvWdWbAjy3hqsExvuHgFqFxA1',
          'i7u4r16TcsJTgq1kAG8opmVZyVnAKBwLKu6ZPMwzxNc',
          '7LWanZteUKtvFjv4MHYgKXXdAuCQYFPJysL9pxxdRQGn'
      )
),

-- ==========================================================
-- 4. ALL TRANSFERS
-- ==========================================================
all_transfers AS (
    SELECT day, brand, tx_type, volume_tokens FROM evm_transfers     WHERE brand IS NOT NULL
    UNION ALL
    SELECT day, brand, tx_type, volume_tokens FROM starknet_transfers
    UNION ALL
    SELECT day, brand, tx_type, volume_tokens FROM stellar_transfers
    UNION ALL
    SELECT day, brand, tx_type, volume_tokens FROM solana_transfers   WHERE brand IS NOT NULL
),

-- ==========================================================
-- 5. DAILY AGGREGATION BY BRAND × TYPE
-- ==========================================================
daily_activity AS (
    SELECT
        day, brand, tx_type,
        COUNT(*)           AS tx_count,
        SUM(volume_tokens) AS volume
    FROM all_transfers
    GROUP BY 1, 2, 3
)

-- ==========================================================
-- 6. OUTPUT — PIVOT mint / burn / transfer
-- ==========================================================
SELECT
    day,
    brand                       AS "Brand",
    SUM(CASE WHEN tx_type = 'mint'     THEN tx_count ELSE 0 END) AS "Mint Count",
    SUM(CASE WHEN tx_type = 'burn'     THEN tx_count ELSE 0 END) AS "Burn Count",
    SUM(CASE WHEN tx_type = 'transfer' THEN tx_count ELSE 0 END) AS "Transfer Count",
    ROUND(SUM(CASE WHEN tx_type = 'mint'     THEN volume ELSE 0 END), 2) AS "Mint Volume",
    ROUND(SUM(CASE WHEN tx_type = 'burn'     THEN volume ELSE 0 END), 2) AS "Burn Volume",
    ROUND(SUM(CASE WHEN tx_type = 'transfer' THEN volume ELSE 0 END), 2) AS "Transfer Volume",
    ROUND(
        SUM(CASE WHEN tx_type = 'mint' THEN volume ELSE 0 END)
      - SUM(CASE WHEN tx_type = 'burn' THEN volume ELSE 0 END)
    , 2) AS "Net Issuance"
FROM daily_activity
GROUP BY 1, 2
ORDER BY day DESC, brand