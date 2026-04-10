-- part of a query repo
-- query name: TVL Competitors
-- query link: https://dune.com/queries/6847260


-- ============================================================
-- Competitive TVL Evolution — INCREMENTAL
-- Spiko products (incl. SAFO) × NAV × FX → USD
-- ============================================================

WITH

-- ==========================================================
-- 0. PREVIOUS RESULTS
-- ==========================================================
prev AS (
    SELECT * FROM TABLE(previous.query.result(DESCRIPTOR(
        day        DATE,
        ticker     VARCHAR,
        tvl_tokens DOUBLE
    )))
),

-- ==========================================================
-- 1. CHECKPOINT — gap-safe split
-- ==========================================================
checkpoint AS (
    SELECT
        COALESCE(MAX(day), DATE '2023-12-31')                           AS output_cutoff,
        COALESCE(MAX(day), DATE '2023-12-31') + INTERVAL '1' DAY        AS new_day_start
    FROM prev
),

prev_supply AS (
    SELECT ticker, tvl_tokens AS seed_supply
    FROM prev
    WHERE day = (SELECT output_cutoff FROM checkpoint)
),

-- ==========================================================
-- 2. CALENDAR — new days only
-- ==========================================================
calendar AS (
    SELECT CAST(date_column AS DATE) AS day
    FROM checkpoint
    CROSS JOIN UNNEST(sequence(
        new_day_start,
        current_date - INTERVAL '1' DAY,
        INTERVAL '1' DAY
    )) AS t(date_column)
    WHERE new_day_start <= current_date - INTERVAL '1' DAY
),

-- ==========================================================
-- 3. FUND MAP — Spiko + SAFO + competitors
-- ==========================================================
fund_map AS (
    SELECT * FROM (VALUES
        -- ── USTBL (5 dec) ──
        (0xe4880249745eAc5F1eD9d8F7DF844792D560e750, 'ethereum',    'USTBL', 5),
        (0xe4880249745eAc5F1eD9d8F7DF844792D560e750, 'polygon',     'USTBL', 5),
        (0x021289588cd81dC1AC87ea91e91607eEF68303F5, 'arbitrum',    'USTBL', 5),
        (0xe4880249745eAc5F1eD9d8F7DF844792D560e750, 'base',        'USTBL', 5),
        (0xe4880249745eAc5F1eD9d8F7DF844792D560e750, 'etherlink',   'USTBL', 5),
        -- ── EUTBL (5 dec) ──
        (0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80, 'ethereum',    'EUTBL', 5),
        (0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80, 'polygon',     'EUTBL', 5),
        (0xCBeb19549054CC0a6257A77736FC78C367216cE7, 'arbitrum',    'EUTBL', 5),
        (0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80, 'base',        'EUTBL', 5),
        (0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80, 'etherlink',   'EUTBL', 5),
        -- ── UKTBL (5 dec) ──
        (0xf695Df6c0f3bB45918A7A82e83348FC59517734E, 'ethereum',    'UKTBL', 5),
        (0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D, 'polygon',     'UKTBL', 5),
        (0x903d5990119bC799423e9C25c56518Ba7DD19474, 'arbitrum',    'UKTBL', 5),
        (0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068, 'base',        'UKTBL', 5),
        (0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D, 'etherlink',   'UKTBL', 5),
        -- ── SPKCC (5 dec) ──
        (0x4f33aCf823E6eEb697180d553cE0c710124C8D59, 'ethereum',    'SPKCC', 5),
        (0x903d5990119bC799423e9C25c56518Ba7DD19474, 'polygon',     'SPKCC', 5),
        (0x99F70A0e1786402a6796c6B0AA997ef340a5c6da, 'arbitrum',    'SPKCC', 5),
        (0xf695Df6c0f3bB45918A7A82e83348FC59517734E, 'base',        'SPKCC', 5),
        (0x4f33aCf823E6eEb697180d553cE0c710124C8D59, 'etherlink',   'SPKCC', 5),
        -- ── eurSPKCC (5 dec) ──
        (0x3868D4e336d14D38031cf680329d31e4712e11cC, 'ethereum',    'eurSPKCC', 5),
        (0x99F70A0e1786402a6796c6B0AA997ef340a5c6da, 'polygon',     'eurSPKCC', 5),
        (0x0e389C83Bc1d16d86412476F6103027555C03265, 'arbitrum',    'eurSPKCC', 5),
        (0x4f33aCf823E6eEb697180d553cE0c710124C8D59, 'base',        'eurSPKCC', 5),
        (0x3868D4e336d14D38031cf680329d31e4712e11cC, 'etherlink',   'eurSPKCC', 5),
        -- ── SAFO-USD (5 dec) ──
        (0xcbade7d9bdee88411cb6cbcbb29952b742036992, 'ethereum',    'SAFO-USD', 5),
        (0x6f64f47f95cf656f21b40e14798f6b49f80b3dc5, 'polygon',     'SAFO-USD', 5),
        (0x0c709396739b9cfb72bcea6ac691ce0ddf66479c, 'arbitrum',    'SAFO-USD', 5),
        (0x0bb754d8940e283d9ff6855ab5dafbc14165c059, 'base',        'SAFO-USD', 5),
        (0x5677a4dc7484762ffccee13cba20b5c979def446, 'etherlink',   'SAFO-USD', 5),
        -- ── SAFO-EUR (5 dec) ──
        (0x0990b149e915cb08e2143a5c6f669c907eddc8b0, 'ethereum',    'SAFO-EUR', 5),
        (0x272ea767712cc4839f4a27ee35eb73116158c8a2, 'polygon',     'SAFO-EUR', 5),
        (0x1412632f2b89e87bfa20c1318a43ced25f1d7b76, 'arbitrum',    'SAFO-EUR', 5),
        (0xd879846cbe20751bde8a9342a3cca00a3e56ca47, 'base',        'SAFO-EUR', 5),
        (0x35dfec1813c43d82e6b87c682f560bbb8ea0c121, 'etherlink',   'SAFO-EUR', 5),
        -- ── SAFO-GBP (5 dec) ──
        (0xc273986a91e4bfc543610a5cb5860b7cfefb6cc0, 'ethereum',    'SAFO-GBP', 5),
        (0x4fe515c67eeeadb3282780325f09bb7c244fe774, 'polygon',     'SAFO-GBP', 5),
        (0xbe023308ac2ef7e1c3799f4e6a3003ee6d342635, 'arbitrum',    'SAFO-GBP', 5),
        (0x2f6c0e5e06b43512706a9cdf66cd21f723fe0ec3, 'base',        'SAFO-GBP', 5),
        (0xfe20ebe388149fb2e158b9d10cb95bcfa652262d, 'etherlink',   'SAFO-GBP', 5),
        -- ── SAFO-CHF (5 dec) ──
        (0x18b5c15e5196a38a162b1787875295b76e4313fb, 'ethereum',    'SAFO-CHF', 5),
        (0x9de2b2dcdcf43540e47143f28484b6d15118f089, 'polygon',     'SAFO-CHF', 5),
        (0x97e7962bcd091e7ecfb583fc96289b1e1553ac6e, 'arbitrum',    'SAFO-CHF', 5),
        (0xd9aa2300e126869182dfb6ecf54984e4c687f36b, 'base',        'SAFO-CHF', 5),
        (0xef53e7d17822b641c6481837238a64a688709301, 'etherlink',   'SAFO-CHF', 5),
        -- ── BUIDL (6 dec) ──
        (0x7712c34205737192402172409a8f7ccef8aa2aec, 'ethereum',    'BUIDL', 6),
        (0x6a9da2d710bb9b700acde7cb81f10f1ff8c89041, 'ethereum',    'BUIDL', 6),
        (0xa6525ae43edcd03dc08e775774dcabd3bb925872, 'arbitrum',    'BUIDL', 6),
        (0x53fc82f14f009009b440a706e31c9021e1196a2f, 'avalanche_c', 'BUIDL', 6),
        (0x2d5bdc96d9c8aabbdb38c9a27398513e7e5ef84f, 'bnb',         'BUIDL', 6),
        (0xa1cdab15bba75a80df4089cafba013e376957cf5, 'optimism',    'BUIDL', 6),
        (0x2893ef551b6dd69f661ac00f11d93e5dc5dc0e99, 'polygon',     'BUIDL', 6),
        -- ── BENJI (18 dec) ──
        (0x3ddc84940ab509c11b20b76b466933f40b750dc9, 'ethereum',    'BENJI', 18),
        (0xb9e4765bce2609bc1949592059b17ea72fee6c6a, 'arbitrum',    'BENJI', 18),
        (0xe08b4c1005603427420e64252a8b120cace4d122, 'avalanche_c', 'BENJI', 18),
        (0x60cfc2b186a4cf647486e42c42b11cc6d571d1e4, 'base',        'BENJI', 18),
        (0x408a634b8a8f0de729b48574a3a7ec3fe820b00a, 'polygon',     'BENJI', 18),
        -- ── OUSG (18 dec) ──
        (0x1B19C19393e2d034D8Ff31ff34c81252FcBbee92, 'ethereum',    'OUSG', 18),
        -- ── USYC (6 dec) ──
        (0x136471a34f6ef19fe571effc1ca711fdb8e49f2b, 'ethereum',    'USYC', 6),
        (0x8d0fa28f221eb5735bc71d3a0da67ee5bc821311, 'bnb',         'USYC', 6),
        -- ── USTB (6 dec) ──
        (0x43415eb6ff9db7e26a15b704e7a3edce97d31c4e, 'ethereum',    'USTB', 6),
        -- ── USCC (6 dec) ──
        (0x14d60e7fdc0d71d8611742720e4c50e7a974020c, 'ethereum',    'USCC', 6),
        -- ── WTGXX (18 dec) ──
        (0x1fecf3d9d4fee7f2c02917a66028a48c6706c179, 'ethereum',    'WTGXX', 18),
        (0xfeb26f0943c3885b2cb85a9f933975356c81c33d, 'arbitrum',    'WTGXX', 18),
        (0x870fd36b3bf7f5abeeea2c8d4abdf1dc4e33109d, 'optimism',    'WTGXX', 18),
        (0x870fd36b3bf7f5abeeea2c8d4abdf1dc4e33109d, 'avalanche_c', 'WTGXX', 18),
        -- ── JTRSY (6 dec) ──
        (0x8c213ee79581ff4984583c6a801e5263418c4b86, 'ethereum',    'JTRSY', 6),
        (0x8c213ee79581ff4984583c6a801e5263418c4b86, 'base',        'JTRSY', 6),
        (0x27e8c820d05aea8824b1ac35116f63f9833b54c8, 'celo',        'JTRSY', 6),
        -- ── FDIT (18 dec) ──
        (0x48ab4e39ac59f4e88974804b04a991b3a402717f, 'ethereum',    'FDIT', 18)
    ) AS t(contract_address, blockchain, ticker, decimals)
),

-- ==========================================================
-- 4. EVM DELTAS
-- ==========================================================
evm_deltas AS (
    SELECT DATE_TRUNC('day', e.evt_block_time) AS day,
           fm.ticker,
           SUM(CASE
               WHEN e."from" = 0x0000000000000000000000000000000000000000
                   THEN  CAST(e.value AS DOUBLE) / POWER(10, fm.decimals)
               WHEN e."to"   = 0x0000000000000000000000000000000000000000
                   THEN -CAST(e.value AS DOUBLE) / POWER(10, fm.decimals)
           END) AS daily_change
    FROM evms.erc20_transfers e
    INNER JOIN fund_map fm
        ON e.contract_address = fm.contract_address
       AND e.blockchain = fm.blockchain
    WHERE e.evt_block_time >= (SELECT CAST(new_day_start AS TIMESTAMP) FROM checkpoint)
      AND e.blockchain IN (
          'ethereum','polygon','arbitrum','base','etherlink',
          'optimism','avalanche_c','bnb','mantle','celo'
      )
      AND (e."from" = 0x0000000000000000000000000000000000000000
           OR e."to"   = 0x0000000000000000000000000000000000000000)
    GROUP BY 1, 2
),

-- ==========================================================
-- 5. STARKNET DELTAS — product-level (incl. SAFO)
-- ==========================================================
starknet_deltas AS (
    SELECT DATE_TRUNC('day', block_date) AS day,
           CASE
               WHEN from_address = 0x020ff2f6021ada9edbceaf31b96f9f67b746662a6e6b2bc9d30c0d3e290a71f6 THEN 'USTBL'
               WHEN from_address = 0x04f5e0de717daa6aa8de63b1bf2e8d7823ec5b21a88461b1519d9dbc956fb7f2 THEN 'EUTBL'
               WHEN from_address = 0x0153d6e0462080bb2842109e9b64f589ef5aa06bb32b26bbdb894aca92674395 THEN 'UKTBL'
               WHEN from_address = 0x04bade88e79a6120f893d64e51006ac6853eceeefa1a50868d19601b1f0a567d THEN 'SPKCC'
               WHEN from_address = 0x06472cabc51a3805975b9c60c7dec63897c9a287f2db173a1d6c589d18dd1e07 THEN 'eurSPKCC'
               WHEN from_address = 0x035bdc17f7a7d09c45d31ab476a576d4f7aad916676b2948fe172c3bcb33725a THEN 'SAFO-USD'
               WHEN from_address = 0x0128f41ef8017ab56140ffad6439305a3196ed862841ba61ff4d78e380c346a6 THEN 'SAFO-EUR'
               WHEN from_address = 0x006e8a99926ff6d56f4cb93c37b63286d736cdf1f81740d53f88b4875b4cbe7f49 THEN 'SAFO-GBP'
               WHEN from_address = 0x06723dcb428eddb160c5adfc2d0a5e5adc184bf6a7298780c3cbf3fa764f709b THEN 'SAFO-CHF'
           END AS ticker,
           SUM(CASE
               WHEN cardinality(keys) >= 2
                    AND keys[2] = 0x0000000000000000000000000000000000000000000000000000000000000000
                   THEN  CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5
               WHEN cardinality(keys) >= 3
                    AND keys[3] = 0x0000000000000000000000000000000000000000000000000000000000000000
                   THEN -CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5
               ELSE 0
           END) AS daily_change
    FROM starknet.events
    WHERE block_date >= (SELECT new_day_start FROM checkpoint)
      AND cardinality(keys) >= 2
      AND cardinality(data) >= 1
      AND keys[1] = 0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9
      AND from_address IN (
          0x020ff2f6021ada9edbceaf31b96f9f67b746662a6e6b2bc9d30c0d3e290a71f6,
          0x04f5e0de717daa6aa8de63b1bf2e8d7823ec5b21a88461b1519d9dbc956fb7f2,
          0x0153d6e0462080bb2842109e9b64f589ef5aa06bb32b26bbdb894aca92674395,
          0x04bade88e79a6120f893d64e51006ac6853eceeefa1a50868d19601b1f0a567d,
          0x06472cabc51a3805975b9c60c7dec63897c9a287f2db173a1d6c589d18dd1e07,
          0x035bdc17f7a7d09c45d31ab476a576d4f7aad916676b2948fe172c3bcb33725a,
          0x0128f41ef8017ab56140ffad6439305a3196ed862841ba61ff4d78e380c346a6,
          0x006e8a99926ff6d56f4cb93c37b63286d736cdf1f81740d53f88b4875b4cbe7f49,
          0x06723dcb428eddb160c5adfc2d0a5e5adc184bf6a7298780c3cbf3fa764f709b
      )
      AND (
          (cardinality(keys) >= 2 AND keys[2] = 0x0000000000000000000000000000000000000000000000000000000000000000)
          OR (cardinality(keys) >= 3 AND keys[3] = 0x0000000000000000000000000000000000000000000000000000000000000000)
      )
    GROUP BY 1, 2
),

-- ==========================================================
-- 6. STELLAR SPIKO DELTAS — product-level (incl. SAFO)
-- ==========================================================
stellar_contract_product AS (
    SELECT * FROM (VALUES
        ('CARUUX2FZNPH6DGJOEUFSIUQWYHNL5AVDV7PMVSHWL7OBYIBFC76F4TO', 'USTBL'),
        ('CBGV2QFQBBGEQRUKUMCPO3SZOHDDYO6SCP5CH6TW7EALKVHCXTMWDDOF', 'EUTBL'),
        ('CDT3KU6TQZNOHKNOHNAFFDQZDURVC3MSTL4ML7TUTZGNOPBZCLABP4FR',  'UKTBL'),
        ('CDS2GCAQTNQINSCJUJIVBJXILKBWP5PU7LOBGHMP3X47QCQBFKPMTCNT', 'SPKCC'),
        ('CDWOB6T7SVSMMQN5V3P2OPTBAXOP7DAZHGVW3PYTZIKHVFKN6TBSXR6A', 'eurSPKCC'),
        ('CDGSC6BA4TCAOVSFQCUEHDMOIIHYYVNYBT6YEARS4MX3ITAHUINVGQHX', 'SAFO-USD'),
        ('CBOOCGZSVRSZFRE4U2NWR2B4RXYVJWRCBTGOUD2JPI2TDJPWMTJX7FZP', 'SAFO-EUR'),
        ('CAGYRRKPFSWKM6SJOE4QAAVYMOSHMDS5WOQ4T5A2E6XNCU7LZZKUNQKP', 'SAFO-GBP'),
        ('CAJD2IBSP7VO2VYJQUYJSOGPJINTUYV7MQITIINXVPTIH3CCLCUENNMW4', 'SAFO-CHF')
    ) AS t(contract_id, product)
),
stellar_baseline AS (
    SELECT sd.contract_id,
           scp.product AS ticker,
           json_extract_scalar(sd.key_decoded, '$.vec[1].address') AS holder,
           TRY_CAST(json_extract_scalar(sd.val_decoded, '$.i128') AS DOUBLE) / 1e5 AS balance
    FROM (
        SELECT contract_id, key_decoded, val_decoded,
               ROW_NUMBER() OVER (
                   PARTITION BY contract_id,
                   json_extract_scalar(key_decoded, '$.vec[1].address')
                   ORDER BY closed_at DESC
               ) AS rn
        FROM stellar.contract_data
        WHERE closed_at_date < (SELECT new_day_start FROM checkpoint)
          AND contract_id IN (
              'CARUUX2FZNPH6DGJOEUFSIUQWYHNL5AVDV7PMVSHWL7OBYIBFC76F4TO',
              'CBGV2QFQBBGEQRUKUMCPO3SZOHDDYO6SCP5CH6TW7EALKVHCXTMWDDOF',
              'CDT3KU6TQZNOHKNOHNAFFDQZDURVC3MSTL4ML7TUTZGNOPBZCLABP4FR',
              'CDS2GCAQTNQINSCJUJIVBJXILKBWP5PU7LOBGHMP3X47QCQBFKPMTCNT',
              'CDWOB6T7SVSMMQN5V3P2OPTBAXOP7DAZHGVW3PYTZIKHVFKN6TBSXR6A',
              'CDGSC6BA4TCAOVSFQCUEHDMOIIHYYVNYBT6YEARS4MX3ITAHUINVGQHX',
              'CBOOCGZSVRSZFRE4U2NWR2B4RXYVJWRCBTGOUD2JPI2TDJPWMTJX7FZP',
              'CAGYRRKPFSWKM6SJOE4QAAVYMOSHMDS5WOQ4T5A2E6XNCU7LZZKUNQKP',
              'CAJD2IBSP7VO2VYJQUYJSOGPJINTUYV7MQITIINXVPTIH3CCLCUENNMW4'
          )
          AND deleted = false AND contract_key_type = 'ScValTypeScvVec'
          AND json_extract_scalar(key_decoded, '$.vec[0].symbol') = 'Balance'
          AND json_extract_scalar(key_decoded, '$.vec[1].address')
              != 'CAVZK26ERVGKGXLQGEOKPAQGIZS2YKN4BSXOYDMUY365EV66ZNSFEDBS'
    ) sd
    INNER JOIN stellar_contract_product scp ON sd.contract_id = scp.contract_id
    WHERE sd.rn = 1
),
stellar_raw_new AS (
    SELECT closed_at,
           DATE_TRUNC('day', closed_at) AS day,
           sd.contract_id,
           scp.product AS ticker,
           json_extract_scalar(sd.key_decoded, '$.vec[1].address') AS holder,
           TRY_CAST(json_extract_scalar(sd.val_decoded, '$.i128') AS DOUBLE) / 1e5 AS balance
    FROM stellar.contract_data sd
    INNER JOIN stellar_contract_product scp ON sd.contract_id = scp.contract_id
    WHERE closed_at_date >= (SELECT new_day_start FROM checkpoint)
      AND sd.contract_id IN (
          'CARUUX2FZNPH6DGJOEUFSIUQWYHNL5AVDV7PMVSHWL7OBYIBFC76F4TO',
          'CBGV2QFQBBGEQRUKUMCPO3SZOHDDYO6SCP5CH6TW7EALKVHCXTMWDDOF',
          'CDT3KU6TQZNOHKNOHNAFFDQZDURVC3MSTL4ML7TUTZGNOPBZCLABP4FR',
          'CDS2GCAQTNQINSCJUJIVBJXILKBWP5PU7LOBGHMP3X47QCQBFKPMTCNT',
          'CDWOB6T7SVSMMQN5V3P2OPTBAXOP7DAZHGVW3PYTZIKHVFKN6TBSXR6A',
          'CDGSC6BA4TCAOVSFQCUEHDMOIIHYYVNYBT6YEARS4MX3ITAHUINVGQHX',
          'CBOOCGZSVRSZFRE4U2NWR2B4RXYVJWRCBTGOUD2JPI2TDJPWMTJX7FZP',
          'CAGYRRKPFSWKM6SJOE4QAAVYMOSHMDS5WOQ4T5A2E6XNCU7LZZKUNQKP',
          'CAJD2IBSP7VO2VYJQUYJSOGPJINTUYV7MQITIINXVPTIH3CCLCUENNMW4'
      )
      AND deleted = false AND contract_key_type = 'ScValTypeScvVec'
      AND json_extract_scalar(sd.key_decoded, '$.vec[0].symbol') = 'Balance'
      AND json_extract_scalar(sd.key_decoded, '$.vec[1].address')
          != 'CAVZK26ERVGKGXLQGEOKPAQGIZS2YKN4BSXOYDMUY365EV66ZNSFEDBS'
    UNION ALL
    -- Sentinel rows
    SELECT TIMESTAMP '1970-01-01 00:00:00.000', DATE '1970-01-01',
           contract_id, ticker, holder, balance
    FROM stellar_baseline
),
stellar_deltas AS (
    SELECT day, ticker, contract_id,
        balance - COALESCE(
            LAG(balance) OVER (PARTITION BY contract_id, holder ORDER BY closed_at), 0
        ) AS delta
    FROM stellar_raw_new
    WHERE day >= (SELECT new_day_start FROM checkpoint)
),
stellar_spiko_deltas AS (
    SELECT day, ticker, SUM(delta) AS daily_change
    FROM stellar_deltas
    GROUP BY 1, 2
),

-- ==========================================================
-- 7. SOLANA DELTAS
-- ==========================================================
solana_deltas AS (
    SELECT DATE_TRUNC('day', block_time) AS day,
        CASE token_mint_address
            WHEN 'GyWgeqpy5GueU2YbkE8xqUeVEokCMMCEeUrfbtMw6phr' THEN 'BUIDL'
            WHEN 'i7u4r16TcsJTgq1kAG8opmVZyVnAKBwLKu6ZPMwzxNc'   THEN 'OUSG'
            WHEN '7LWanZteUKtvFjv4MHYgKXXdAuCQYFPJysL9pxxdRQGn'  THEN 'USYC'
        END AS ticker,
        SUM(CASE
            WHEN action = 'mint' THEN  amount / 1e6
            WHEN action = 'burn' THEN -amount / 1e6
        END) AS daily_change
    FROM tokens_solana.transfers
    WHERE block_time >= (SELECT CAST(new_day_start AS TIMESTAMP) FROM checkpoint)
      AND token_mint_address IN (
          'GyWgeqpy5GueU2YbkE8xqUeVEokCMMCEeUrfbtMw6phr',
          'i7u4r16TcsJTgq1kAG8opmVZyVnAKBwLKu6ZPMwzxNc',
          '7LWanZteUKtvFjv4MHYgKXXdAuCQYFPJysL9pxxdRQGn'
      )
      AND action IN ('mint', 'burn')
    GROUP BY 1, 2
),

-- ==========================================================
-- 8. STELLAR COMPETITIVE DELTAS
-- ==========================================================
stellar_comp_deltas AS (
    SELECT DATE_TRUNC('day', closed_at) AS day,
        CASE
            WHEN asset_code IN ('BENJI','gBENJI','sgBENJI') THEN 'BENJI'
            WHEN asset_code = 'WTGX'                        THEN 'WTGXX'
        END AS ticker,
        SUM(CASE
            WHEN "from" = asset_issuer THEN  amount
            WHEN "to"   = asset_issuer THEN -amount
        END) AS daily_change
    FROM stellar.history_operations
    WHERE closed_at >= (SELECT CAST(new_day_start AS TIMESTAMP) FROM checkpoint)
      AND type_string = 'payment'
      AND (
          (asset_code = 'BENJI'   AND asset_issuer = 'GBHNGLLIE3KWGKCHIKMHJ5HVZHYIK7WTBE4QF5PLAKL4CJGSEU7HZIW5')
       OR (asset_code = 'gBENJI'  AND asset_issuer = 'GD5J73EKK5IYL5XS3FBTHHX7CZIYRP7QXDL57XFWGC2WVYWT326OBXRP')
       OR (asset_code = 'sgBENJI' AND asset_issuer = 'GAGICV3VBJSKKH5H5MQQIUTUP462YVHC23KUHZY6FJERRJFBDIVZBM5C')
       OR (asset_code = 'WTGX'    AND asset_issuer = 'GDMBNMFJ3TRFLASJ6UGETFME3PJPNKPU24C7KFDBEBPQFG2CI6UC3JG6')
      )
      AND ("from" = asset_issuer OR "to" = asset_issuer)
    GROUP BY 1, 2
),

-- ==========================================================
-- 9. ALL NEW DELTAS COMBINED
-- ==========================================================
all_new_deltas AS (
    SELECT day, ticker, SUM(daily_change) AS daily_change
    FROM (
        SELECT day, ticker, daily_change FROM evm_deltas             WHERE ticker IS NOT NULL
        UNION ALL
        SELECT day, ticker, daily_change FROM starknet_deltas        WHERE ticker IS NOT NULL
        UNION ALL
        SELECT day, ticker, daily_change FROM stellar_spiko_deltas
        UNION ALL
        SELECT day, ticker, daily_change FROM solana_deltas          WHERE ticker IS NOT NULL
        UNION ALL
        SELECT day, ticker, daily_change FROM stellar_comp_deltas    WHERE ticker IS NOT NULL
    ) t
    GROUP BY 1, 2
),

-- ==========================================================
-- 10. ALL TICKERS
-- ==========================================================
all_tickers AS (
    SELECT DISTINCT ticker FROM (
        SELECT ticker FROM prev
        UNION
        SELECT ticker FROM all_new_deltas WHERE ticker IS NOT NULL
    ) t
),

-- ==========================================================
-- 11. NEW CUMULATIVE SUPPLY (raw tokens)
-- ==========================================================
new_cumulative AS (
    SELECT c.day, tk.ticker,
        COALESCE(ps.seed_supply, 0)
        + SUM(COALESCE(d.daily_change, 0))
            OVER (PARTITION BY tk.ticker ORDER BY c.day)
        AS tvl_tokens
    FROM calendar c
    CROSS JOIN all_tickers tk
    LEFT JOIN prev_supply    ps ON tk.ticker = ps.ticker
    LEFT JOIN all_new_deltas d  ON c.day = d.day AND tk.ticker = d.ticker
),

-- ==========================================================
-- 12. DAY SPINE — shared calendar for fill-forward joins
-- ==========================================================
day_spine AS (
    SELECT CAST(d AS DATE) AS day
    FROM checkpoint
    CROSS JOIN UNNEST(sequence(
        new_day_start - INTERVAL '3' DAY,
        current_date,
        INTERVAL '1' DAY
    )) AS t(d)
),

-- ==========================================================
-- 13. SPIKO NAV ORACLE — daily, fill-forwarded (incl. SAFO)
-- ==========================================================
spiko_nav_raw AS (
    -- USTBL — arbitrum chainlink (preferred)
    SELECT DATE_TRUNC('day', block_time) AS day, 'USTBL' AS product,
           bytearray_to_uint256(bytearray_substring(data, 1, 32)) * 1e-6 AS nav
    FROM arbitrum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0xc797025feeeaf2cd924c99e9205acb8ec04d5cad21c41ce637a38fb6dee6016a
      AND contract_address = 0xA260D72df8FF2696f3A8d0BE46B7bc4d743Be764
    UNION ALL
    -- USTBL — arbitrum old oracle (fallback)
    SELECT DATE_TRUNC('day', block_time), 'USTBL',
           bytearray_to_uint256(bytearray_substring(data, 1+32, 32)) * 1e-6
    FROM arbitrum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0xA260D72df8FF2696f3A8d0BE46B7bc4d743Be764
    UNION ALL
    -- EUTBL — ethereum chainlink (preferred)
    SELECT DATE_TRUNC('day', block_time), 'EUTBL',
           bytearray_to_uint256(bytearray_substring(data, 1, 32)) * 1e-6
    FROM ethereum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0xc797025feeeaf2cd924c99e9205acb8ec04d5cad21c41ce637a38fb6dee6016a
      AND contract_address = 0xdaA1c6511Aa051e9e83Dd7Ac2D65d5E41D1f6b98
    UNION ALL
    -- EUTBL — arbitrum chainlink (fallback)
    SELECT DATE_TRUNC('day', block_time), 'EUTBL',
           bytearray_to_uint256(bytearray_substring(data, 1, 32)) * 1e-6
    FROM arbitrum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0xc797025feeeaf2cd924c99e9205acb8ec04d5cad21c41ce637a38fb6dee6016a
      AND contract_address = 0xe4880249745eAc5F1eD9d8F7DF844792D560e750
    UNION ALL
    -- EUTBL — arbitrum old oracle (fallback 2)
    SELECT DATE_TRUNC('day', block_time), 'EUTBL',
           bytearray_to_uint256(bytearray_substring(data, 1+32, 32)) * 1e-6
    FROM arbitrum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0xe4880249745eAc5F1eD9d8F7DF844792D560e750
    UNION ALL
    -- UKTBL — arbitrum old oracle
    SELECT DATE_TRUNC('day', block_time), 'UKTBL',
           bytearray_to_uint256(bytearray_substring(data, 1+32, 32)) * 1e-6
    FROM arbitrum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59
    UNION ALL
    -- SPKCC — redstone (preferred)
    SELECT DATE_TRUNC('day', block_time), 'SPKCC',
           bytearray_to_int256(topic1) * 1e-8
    FROM ethereum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f
      AND contract_address = 0x9e37DBF40fE5Fe9320E45fe6B95b000aa05459A9
    UNION ALL
    -- SPKCC — arbitrum old oracle (fallback)
    SELECT DATE_TRUNC('day', block_time), 'SPKCC',
           bytearray_to_uint256(bytearray_substring(data, 1+32, 32)) * 1e-6
    FROM arbitrum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0x3868D4e336d14D38031cf680329d31e4712e11cC
    UNION ALL
    -- eurSPKCC — redstone (preferred)
    SELECT DATE_TRUNC('day', block_time), 'eurSPKCC',
           bytearray_to_int256(topic1) * 1e-8
    FROM ethereum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f
      AND contract_address = 0x4B2C406f0Dbf7624a32971277DA7B4C43A7A942b
    UNION ALL
    -- eurSPKCC — arbitrum old oracle (fallback)
    SELECT DATE_TRUNC('day', block_time), 'eurSPKCC',
           bytearray_to_uint256(bytearray_substring(data, 1+32, 32)) * 1e-6
    FROM arbitrum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0x7A16Df1C2Cd8B9EEb9ED9942c82C2e7c90Bb93Db
    UNION ALL
    -- SAFO-USD — arbitrum old oracle
    SELECT DATE_TRUNC('day', block_time), 'SAFO-USD',
           bytearray_to_uint256(bytearray_substring(data, 1+32, 32)) * 1e-6
    FROM arbitrum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0x372e37cA79747A2d1671EDBC5f1e2853B96BA351
    UNION ALL
    -- SAFO-EUR — arbitrum old oracle
    SELECT DATE_TRUNC('day', block_time), 'SAFO-EUR',
           bytearray_to_uint256(bytearray_substring(data, 1+32, 32)) * 1e-6
    FROM arbitrum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0x385D443ffA5b6Fb462b988D023a5DC3b37Ef1644
    UNION ALL
    -- SAFO-GBP — arbitrum old oracle
    SELECT DATE_TRUNC('day', block_time), 'SAFO-GBP',
           bytearray_to_uint256(bytearray_substring(data, 1+32, 32)) * 1e-6
    FROM arbitrum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0x835B48E97CBF727e23E7AA3bD40248818d20A2b0
    UNION ALL
    -- SAFO-CHF — arbitrum old oracle
    SELECT DATE_TRUNC('day', block_time), 'SAFO-CHF',
           bytearray_to_uint256(bytearray_substring(data, 1+32, 32)) * 1e-6
    FROM arbitrum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address = 0xD1F12049cC311DfB177f168046Ed8e2bd341a7AF
),
-- Pick best NAV per product per day (MAX = prefer chainlink/redstone over old oracle)
spiko_nav_daily AS (
    SELECT day, product, MAX(nav) AS nav
    FROM spiko_nav_raw
    GROUP BY 1, 2
),
-- Fill-forward NAV per product across calendar days
spiko_nav_filled AS (
    SELECT d.day, p.product,
        COALESCE(
            last_value(sn.nav) IGNORE NULLS
                OVER (PARTITION BY p.product ORDER BY d.day
                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
            1.0
        ) AS nav
    FROM day_spine d
    CROSS JOIN (
        SELECT DISTINCT product FROM spiko_nav_daily
    ) p
    LEFT JOIN spiko_nav_daily sn ON d.day = sn.day AND p.product = sn.product
),

-- ==========================================================
-- 14. FX ORACLE — EUR/USD & GBP/USD, daily fill-forward
-- ==========================================================
fx_raw AS (
    SELECT DATE_TRUNC('day', block_time) AS day,
           CASE contract_address
               WHEN 0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871 THEN 'EUR_USD'
               WHEN 0x78f28D363533695458696b42577D2e1728cEa3D1 THEN 'GBP_USD'
           END AS pair,
           bytearray_to_int256(topic1) * 1e-8 AS rate
    FROM arbitrum.logs
    WHERE block_date >= (SELECT new_day_start - INTERVAL '5' DAY FROM checkpoint)
      AND topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f
      AND contract_address IN (
          0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871,
          0x78f28D363533695458696b42577D2e1728cEa3D1
      )
),
fx_daily AS (
    SELECT day, pair, MAX(rate) AS rate FROM fx_raw GROUP BY 1, 2
),
fx_filled AS (
    SELECT d.day,
        COALESCE(
            last_value(eur.rate) IGNORE NULLS
                OVER (ORDER BY d.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
            1.08
        ) AS eur_usd,
        COALESCE(
            last_value(gbp.rate) IGNORE NULLS
                OVER (ORDER BY d.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
            1.27
        ) AS gbp_usd
    FROM day_spine d
    LEFT JOIN fx_daily eur ON d.day = eur.day AND eur.pair = 'EUR_USD'
    LEFT JOIN fx_daily gbp ON d.day = gbp.day AND gbp.pair = 'GBP_USD'
),

-- ==========================================================
-- 15. SPIKO PRODUCT SET (for filtering)
-- ==========================================================
spiko_products AS (
    SELECT product FROM (VALUES
        ('USTBL'), ('EUTBL'), ('UKTBL'), ('SPKCC'), ('eurSPKCC'),
        ('SAFO-USD'), ('SAFO-EUR'), ('SAFO-GBP'), ('SAFO-CHF')
    ) AS t(product)
)

-- ==========================================================
-- 16. OUTPUT
-- ==========================================================

-- ── Previous results: pass through as-is
SELECT day, ticker, tvl_tokens
FROM prev
WHERE day <= (SELECT output_cutoff FROM checkpoint)

UNION ALL

-- ── New days: non-Spiko tickers (raw token supply ≈ USD, NAV ≈ $1)
SELECT
    nc.day,
    nc.ticker,
    nc.tvl_tokens
FROM new_cumulative nc
WHERE nc.tvl_tokens > 0
  AND nc.ticker NOT IN (SELECT product FROM spiko_products)

UNION ALL

-- ── New days: all Spiko products (incl. SAFO) → tokens × NAV × FX → aggregate as 'Spiko'
SELECT
    nc.day,
    'Spiko' AS ticker,
    ROUND(SUM(
        nc.tvl_tokens
        * snf.nav
        * CASE nc.ticker
            WHEN 'USTBL'    THEN 1.0
            WHEN 'SPKCC'    THEN 1.0
            WHEN 'SAFO-USD' THEN 1.0
            WHEN 'EUTBL'    THEN fxf.eur_usd
            WHEN 'eurSPKCC' THEN fxf.eur_usd
            WHEN 'SAFO-EUR' THEN fxf.eur_usd
            WHEN 'UKTBL'    THEN fxf.gbp_usd
            WHEN 'SAFO-GBP' THEN fxf.gbp_usd
            WHEN 'SAFO-CHF' THEN 1.13
          END
    ), 2) AS tvl_tokens
FROM new_cumulative nc
INNER JOIN spiko_nav_filled snf ON nc.day = snf.day AND nc.ticker = snf.product
INNER JOIN fx_filled        fxf ON nc.day = fxf.day
WHERE nc.ticker IN (SELECT product FROM spiko_products)
  AND nc.tvl_tokens > 0
GROUP BY nc.day

ORDER BY day DESC, ticker