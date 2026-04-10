-- part of a query repo
-- query name: Cumulative Net Inflow 30d by Product
-- query link: https://dune.com/queries/6845793


-- ============================================================
-- Flow Momentum — 7d / 30d Rolling Net Flows — Incremental ⚡
-- Net flow = mints − burns (supply changes only)
-- All chains | Valued in USD | DuneSQL
-- Fixed: Trino alias scoping, Starknet array bounds,
--        checkpoint gap, ETH mainnet USTBL oracle,
--        partial today excluded from calendar
-- ============================================================

WITH

-- ── 1) PREVIOUS RESULTS ──────────────────────────────────────
prev AS (
    SELECT * FROM TABLE(previous.query.result(DESCRIPTOR(
        day                  DATE,
        product              VARCHAR,
        currency             VARCHAR,
        daily_net_flow       DOUBLE,
        daily_net_flow_usd   DOUBLE,
        rolling_7d_tokens    DOUBLE,
        rolling_7d_usd       DOUBLE,
        rolling_30d_tokens   DOUBLE,
        rolling_30d_usd      DOUBLE,
        cum_net_flow         DOUBLE,
        momentum_signal      VARCHAR
    )))
),

-- ── 2) CHECKPOINT ────────────────────────────────────────────
--    output_cutoff : true last known day → UNION boundary
--    scan_cutoff   : 1 day before → recompute window
--    price_lookback: 5 days before scan for fill-forward safety
checkpoint AS (
    SELECT
        COALESCE(MAX(day), DATE '2024-05-29')                              AS output_cutoff,
        COALESCE(MAX(day), DATE '2024-05-29') - INTERVAL '1' DAY          AS scan_cutoff,
        COALESCE(MAX(day), DATE '2024-05-29') - INTERVAL '5' DAY          AS price_lookback
    FROM prev
),

-- ── 3) CALENDAR — scan_cutoff → yesterday (no partial today) ─
calendar_new AS (
    SELECT CAST(date_trunc('day', period) AS DATE) AS day
    FROM checkpoint
    CROSS JOIN UNNEST(sequence(
        scan_cutoff,
        current_date - INTERVAL '1' DAY,
        INTERVAL '1' DAY
    )) AS t(period)
),

-- ── 4) FOREX — Chainlink EUR/USD & GBP/USD ───────────────────
forex_raw AS (
    SELECT
        date_trunc('day', block_time) AS day,
        contract_address,
        bytearray_to_int256(topic1)   AS rate_raw
    FROM arbitrum.logs
    WHERE block_date >= (SELECT price_lookback FROM checkpoint)
      AND topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f
      AND contract_address IN (
          0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871,
          0x78f28D363533695458696b42577D2e1728cEa3D1
      )
),
daily_forex AS (
    SELECT day,
        arbitrary(rate_raw) FILTER (WHERE contract_address = 0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871) AS eur_raw,
        arbitrary(rate_raw) FILTER (WHERE contract_address = 0x78f28D363533695458696b42577D2e1728cEa3D1) AS gbp_raw
    FROM forex_raw GROUP BY 1
),
filled_forex AS (
    SELECT c.day,
        COALESCE(last_value(f.eur_raw) IGNORE NULLS OVER w * 1e-8, 1.08) AS rate_eur_usd,
        COALESCE(last_value(f.gbp_raw) IGNORE NULLS OVER w * 1e-8, 1.27) AS rate_gbp_usd
    FROM calendar_new c
    LEFT JOIN daily_forex f ON c.day = f.day
    WINDOW w AS (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

-- ── 5) NAV PRICES ────────────────────────────────────────────
--    FIX: added ETH mainnet Chainlink for USTBL (was missing,
--    only Arbitrum fallback was present in original)

-- ETH mainnet USTBL (Chainlink post-Dec 2024)
eth_ustbl_logs AS (
    SELECT date_trunc('day', block_time) AS day,
        bytearray_to_uint256(bytearray_substring(data, 1, 32)) AS price_raw
    FROM ethereum.logs
    WHERE block_date >= (SELECT price_lookback FROM checkpoint)
      AND contract_address = 0xC1C24f0f2103F5899b7AB415A1930E519B7D3423
      AND topic0 = 0xc797025feeeaf2cd924c99e9205acb8ec04d5cad21c41ce637a38fb6dee6016a
),
eth_ustbl_prices AS (
    SELECT day, arbitrary(price_raw) AS ustbl_raw FROM eth_ustbl_logs GROUP BY 1
),

-- ETH mainnet EUTBL (Chainlink post-Dec 2024)
eth_eutbl_logs AS (
    SELECT date_trunc('day', block_time) AS day,
        bytearray_to_uint256(bytearray_substring(data, 1, 32)) AS price_raw
    FROM ethereum.logs
    WHERE block_date >= (SELECT price_lookback FROM checkpoint)
      AND contract_address = 0xdaA1c6511Aa051e9e83Dd7Ac2D65d5E41D1f6b98
      AND topic0 = 0xc797025feeeaf2cd924c99e9205acb8ec04d5cad21c41ce637a38fb6dee6016a
),
eth_eutbl_prices AS (
    SELECT day, arbitrary(price_raw) AS eutbl_raw FROM eth_eutbl_logs GROUP BY 1
),

-- Arbitrum fallback (all products, 6 dec)
arb_price_logs AS (
    SELECT date_trunc('day', block_time) AS day,
        contract_address,
        bytearray_to_uint256(bytearray_substring(data, 1+32, 32)) AS price_raw
    FROM arbitrum.logs
    WHERE block_date >= (SELECT price_lookback FROM checkpoint)
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address IN (
          0xA260D72df8FF2696f3A8d0BE46B7bc4d743Be764,
          0xe4880249745eAc5F1eD9d8F7DF844792D560e750,
          0x4f33aCf823E6eEb697180d553cE0c710124C8D59,
          0x3868D4e336d14D38031cf680329d31e4712e11cC,
          0x7A16Df1C2Cd8B9EEb9ED9942c82C2e7c90Bb93Db,
          0x372e37cA79747A2d1671EDBC5f1e2853B96BA351,
          0x385D443ffA5b6Fb462b988D023a5DC3b37Ef1644,
          0x835B48E97CBF727e23E7AA3bD40248818d20A2b0,
          0xD1F12049cC311DfB177f168046Ed8e2bd341a7AF
      )
    UNION ALL
    SELECT date_trunc('day', block_time) AS day,
        contract_address,
        bytearray_to_uint256(bytearray_substring(data, 1, 32)) AS price_raw
    FROM arbitrum.logs
    WHERE block_date >= (SELECT price_lookback FROM checkpoint)
      AND topic0 = 0xc797025feeeaf2cd924c99e9205acb8ec04d5cad21c41ce637a38fb6dee6016a
      AND contract_address IN (
          0xA260D72df8FF2696f3A8d0BE46B7bc4d743Be764,
          0xe4880249745eAc5F1eD9d8F7DF844792D560e750
      )
),
arb_prices AS (
    SELECT day,
        arbitrary(price_raw) FILTER (WHERE contract_address = 0xA260D72df8FF2696f3A8d0BE46B7bc4d743Be764) AS ustbl_raw,
        arbitrary(price_raw) FILTER (WHERE contract_address = 0xe4880249745eAc5F1eD9d8F7DF844792D560e750) AS eutbl_raw,
        arbitrary(price_raw) FILTER (WHERE contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59) AS uktbl_raw,
        arbitrary(price_raw) FILTER (WHERE contract_address = 0x3868D4e336d14D38031cf680329d31e4712e11cC) AS spkcc_raw,
        arbitrary(price_raw) FILTER (WHERE contract_address = 0x7A16Df1C2Cd8B9EEb9ED9942c82C2e7c90Bb93Db) AS eurspkcc_raw,
        arbitrary(price_raw) FILTER (WHERE contract_address = 0x372e37cA79747A2d1671EDBC5f1e2853B96BA351) AS safo_usd_raw,
        arbitrary(price_raw) FILTER (WHERE contract_address = 0x385D443ffA5b6Fb462b988D023a5DC3b37Ef1644) AS safo_eur_raw,
        arbitrary(price_raw) FILTER (WHERE contract_address = 0x835B48E97CBF727e23E7AA3bD40248818d20A2b0) AS safo_gbp_raw,
        arbitrary(price_raw) FILTER (WHERE contract_address = 0xD1F12049cC311DfB177f168046Ed8e2bd341a7AF) AS safo_chf_raw
    FROM arb_price_logs GROUP BY 1
),

-- Redstone SPKCC & eurSPKCC (8 dec, priority over Arbitrum)
redstone_price_logs AS (
    SELECT DATE_TRUNC('day', block_time) AS day,
        contract_address,
        bytearray_to_int256(topic1) AS price_raw
    FROM ethereum.logs
    WHERE block_date >= (SELECT price_lookback FROM checkpoint)
      AND topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f
      AND contract_address IN (
          0x9e37DBF40fE5Fe9320E45fe6B95b000aa05459A9,
          0x4B2C406f0Dbf7624a32971277DA7B4C43A7A942b
      )
),
redstone_prices AS (
    SELECT day,
        arbitrary(price_raw) FILTER (WHERE contract_address = 0x9e37DBF40fE5Fe9320E45fe6B95b000aa05459A9)  AS spkcc_rs_raw,
        arbitrary(price_raw) FILTER (WHERE contract_address = 0x4B2C406f0Dbf7624a32971277DA7B4C43A7A942b) AS eurspkcc_rs_raw
    FROM redstone_price_logs GROUP BY 1
),
filled_prices AS (
    SELECT c.day,
        COALESCE(
            last_value(eu.ustbl_raw) IGNORE NULLS OVER w,
            last_value(ap.ustbl_raw) IGNORE NULLS OVER w,
            1000000
        ) * 1e-6 AS p_ustbl,
        COALESCE(
            last_value(ee.eutbl_raw) IGNORE NULLS OVER w,
            last_value(ap.eutbl_raw) IGNORE NULLS OVER w,
            1000000
        ) * 1e-6 AS p_eutbl,
        COALESCE(last_value(ap.uktbl_raw)       IGNORE NULLS OVER w, 1000000) * 1e-6 AS p_uktbl,
        COALESCE(
            last_value(rs.spkcc_rs_raw)    IGNORE NULLS OVER w * 1e-8,
            last_value(ap.spkcc_raw)       IGNORE NULLS OVER w * 1e-6,
            1.0
        ) AS p_spkcc,
        COALESCE(
            last_value(rs.eurspkcc_rs_raw) IGNORE NULLS OVER w * 1e-8,
            last_value(ap.eurspkcc_raw)    IGNORE NULLS OVER w * 1e-6,
            1.0
        ) AS p_eurspkcc,
        COALESCE(last_value(ap.safo_usd_raw) IGNORE NULLS OVER w, 1000000) * 1e-6 AS p_safo_usd,
        COALESCE(last_value(ap.safo_eur_raw) IGNORE NULLS OVER w, 1000000) * 1e-6 AS p_safo_eur,
        COALESCE(last_value(ap.safo_gbp_raw) IGNORE NULLS OVER w, 1000000) * 1e-6 AS p_safo_gbp,
        COALESCE(last_value(ap.safo_chf_raw) IGNORE NULLS OVER w, 1000000) * 1e-6 AS p_safo_chf
    FROM calendar_new c
    LEFT JOIN eth_ustbl_prices eu ON c.day = eu.day
    LEFT JOIN eth_eutbl_prices ee ON c.day = ee.day
    LEFT JOIN arb_prices       ap ON c.day = ap.day
    LEFT JOIN redstone_prices  rs ON c.day = rs.day
    WINDOW w AS (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

-- ── 6) EVM NET FLOWS ─────────────────────────────────────────
--    FIX: subquery wrap so product alias resolves in GROUP BY
evm_flows AS (
    SELECT day, product, SUM(net_flow) AS net_flow
    FROM (
        SELECT
            DATE_TRUNC('day', evt_block_time) AS day,
            CASE
                WHEN contract_address = 0xe4880249745eAc5F1eD9d8F7DF844792D560e750 AND blockchain IN ('ethereum','polygon','base','etherlink') THEN 'USTBL'
                WHEN contract_address = 0x021289588cd81dC1AC87ea91e91607eEF68303F5 AND blockchain = 'arbitrum'                               THEN 'USTBL'
                WHEN contract_address = 0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80 AND blockchain IN ('ethereum','polygon','base','etherlink') THEN 'EUTBL'
                WHEN contract_address = 0xCBeb19549054CC0a6257A77736FC78C367216cE7 AND blockchain = 'arbitrum'                               THEN 'EUTBL'
                WHEN contract_address = 0xf695Df6c0f3bB45918A7A82e83348FC59517734E AND blockchain = 'ethereum'                               THEN 'UKTBL'
                WHEN contract_address = 0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D AND blockchain IN ('polygon','etherlink')                  THEN 'UKTBL'
                WHEN contract_address = 0x903d5990119bC799423e9C25c56518Ba7DD19474 AND blockchain = 'arbitrum'                               THEN 'UKTBL'
                WHEN contract_address = 0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068 AND blockchain = 'base'                                   THEN 'UKTBL'
                WHEN contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59 AND blockchain IN ('ethereum','etherlink')                 THEN 'SPKCC'
                WHEN contract_address = 0x903d5990119bC799423e9C25c56518Ba7DD19474 AND blockchain = 'polygon'                                THEN 'SPKCC'
                WHEN contract_address = 0x99F70A0e1786402a6796c6B0AA997ef340a5c6da AND blockchain = 'arbitrum'                               THEN 'SPKCC'
                WHEN contract_address = 0xf695Df6c0f3bB45918A7A82e83348FC59517734E AND blockchain = 'base'                                   THEN 'SPKCC'
                WHEN contract_address = 0x3868D4e336d14D38031cf680329d31e4712e11cC AND blockchain IN ('ethereum','etherlink')                 THEN 'eurSPKCC'
                WHEN contract_address = 0x99F70A0e1786402a6796c6B0AA997ef340a5c6da AND blockchain = 'polygon'                                THEN 'eurSPKCC'
                WHEN contract_address = 0x0e389C83Bc1d16d86412476F6103027555C03265 AND blockchain = 'arbitrum'                               THEN 'eurSPKCC'
                WHEN contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59 AND blockchain = 'base'                                   THEN 'eurSPKCC'
                -- SAFO-EUR
                WHEN contract_address = 0x0990b149e915cb08e2143a5c6f669c907eddc8b0 AND blockchain = 'ethereum'  THEN 'SAFO-EUR'
                WHEN contract_address = 0x272ea767712cc4839f4a27ee35eb73116158c8a2 AND blockchain = 'polygon'   THEN 'SAFO-EUR'
                WHEN contract_address = 0x1412632f2b89e87bfa20c1318a43ced25f1d7b76 AND blockchain = 'arbitrum'  THEN 'SAFO-EUR'
                WHEN contract_address = 0xd879846cbe20751bde8a9342a3cca00a3e56ca47 AND blockchain = 'base'      THEN 'SAFO-EUR'
                WHEN contract_address = 0x35dfec1813c43d82e6b87c682f560bbb8ea0c121 AND blockchain = 'etherlink' THEN 'SAFO-EUR'
                -- SAFO-USD
                WHEN contract_address = 0xcbade7d9bdee88411cb6cbcbb29952b742036992 AND blockchain = 'ethereum'  THEN 'SAFO-USD'
                WHEN contract_address = 0x6f64f47f95cf656f21b40e14798f6b49f80b3dc5 AND blockchain = 'polygon'   THEN 'SAFO-USD'
                WHEN contract_address = 0x0c709396739b9cfb72bcea6ac691ce0ddf66479c AND blockchain = 'arbitrum'  THEN 'SAFO-USD'
                WHEN contract_address = 0x0bb754d8940e283d9ff6855ab5dafbc14165c059 AND blockchain = 'base'      THEN 'SAFO-USD'
                WHEN contract_address = 0x5677a4dc7484762ffccee13cba20b5c979def446 AND blockchain = 'etherlink' THEN 'SAFO-USD'
                -- SAFO-GBP
                WHEN contract_address = 0xc273986a91e4bfc543610a5cb5860b7cfefb6cc0 AND blockchain = 'ethereum'  THEN 'SAFO-GBP'
                WHEN contract_address = 0x4fe515c67eeeadb3282780325f09bb7c244fe774 AND blockchain = 'polygon'   THEN 'SAFO-GBP'
                WHEN contract_address = 0xbe023308ac2ef7e1c3799f4e6a3003ee6d342635 AND blockchain = 'arbitrum'  THEN 'SAFO-GBP'
                WHEN contract_address = 0x2f6c0e5e06b43512706a9cdf66cd21f723fe0ec3 AND blockchain = 'base'      THEN 'SAFO-GBP'
                WHEN contract_address = 0xfe20ebe388149fb2e158b9d10cb95bcfa652262d AND blockchain = 'etherlink' THEN 'SAFO-GBP'
                -- SAFO-CHF
                WHEN contract_address = 0x18b5c15e5196a38a162b1787875295b76e4313fb AND blockchain = 'ethereum'  THEN 'SAFO-CHF'
                WHEN contract_address = 0x9de2b2dcdcf43540e47143f28484b6d15118f089 AND blockchain = 'polygon'   THEN 'SAFO-CHF'
                WHEN contract_address = 0x97e7962bcd091e7ecfb583fc96289b1e1553ac6e AND blockchain = 'arbitrum'  THEN 'SAFO-CHF'
                WHEN contract_address = 0xd9aa2300e126869182dfb6ecf54984e4c687f36b AND blockchain = 'base'      THEN 'SAFO-CHF'
                WHEN contract_address = 0xef53e7d17822b641c6481837238a64a688709301 AND blockchain = 'etherlink' THEN 'SAFO-CHF'
            END AS product,
            SUM(CASE
                WHEN "from" = 0x0000000000000000000000000000000000000000 THEN  CAST(value AS DOUBLE)
                WHEN "to"   = 0x0000000000000000000000000000000000000000 THEN -CAST(value AS DOUBLE)
                ELSE 0
            END) / 1e5 AS net_flow
        FROM evms.erc20_transfers
        WHERE evt_block_time >= (SELECT CAST(scan_cutoff AS TIMESTAMP) FROM checkpoint)
          AND blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
          AND contract_address IN (
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
              -- SAFO-EUR
              0x0990b149e915cb08e2143a5c6f669c907eddc8b0,
              0x272ea767712cc4839f4a27ee35eb73116158c8a2,
              0x1412632f2b89e87bfa20c1318a43ced25f1d7b76,
              0xd879846cbe20751bde8a9342a3cca00a3e56ca47,
              0x35dfec1813c43d82e6b87c682f560bbb8ea0c121,
              -- SAFO-USD
              0xcbade7d9bdee88411cb6cbcbb29952b742036992,
              0x6f64f47f95cf656f21b40e14798f6b49f80b3dc5,
              0x0c709396739b9cfb72bcea6ac691ce0ddf66479c,
              0x0bb754d8940e283d9ff6855ab5dafbc14165c059,
              0x5677a4dc7484762ffccee13cba20b5c979def446,
              -- SAFO-GBP
              0xc273986a91e4bfc543610a5cb5860b7cfefb6cc0,
              0x4fe515c67eeeadb3282780325f09bb7c244fe774,
              0xbe023308ac2ef7e1c3799f4e6a3003ee6d342635,
              0x2f6c0e5e06b43512706a9cdf66cd21f723fe0ec3,
              0xfe20ebe388149fb2e158b9d10cb95bcfa652262d,
              -- SAFO-CHF
              0x18b5c15e5196a38a162b1787875295b76e4313fb,
              0x9de2b2dcdcf43540e47143f28484b6d15118f089,
              0x97e7962bcd091e7ecfb583fc96289b1e1553ac6e,
              0xd9aa2300e126869182dfb6ecf54984e4c687f36b,
              0xef53e7d17822b641c6481837238a64a688709301
          )
          AND (
              "from" = 0x0000000000000000000000000000000000000000
              OR "to" = 0x0000000000000000000000000000000000000000
          )
        GROUP BY 1, 2
    ) t
    WHERE product IS NOT NULL
    GROUP BY 1, 2
),

-- ── 7) STARKNET NET FLOWS ────────────────────────────────────
--    FIX: subquery wrap + cardinality guard before keys[3] access
starknet_flows AS (
    SELECT day, product, SUM(net_flow) AS net_flow
    FROM (
        SELECT
            DATE_TRUNC('day', block_date) AS day,
            CASE
                WHEN from_address = 0x020ff2f6021ada9edbceaf31b96f9f67b746662a6e6b2bc9d30c0d3e290a71f6 THEN 'USTBL'
                WHEN from_address = 0x04f5e0de717daa6aa8de63b1bf2e8d7823ec5b21a88461b1519d9dbc956fb7f2 THEN 'EUTBL'
                WHEN from_address = 0x0153d6e0462080bb2842109e9b64f589ef5aa06bb32b26bbdb894aca92674395 THEN 'UKTBL'
                WHEN from_address = 0x04bade88e79a6120f893d64e51006ac6853eceeefa1a50868d19601b1f0a567d THEN 'SPKCC'
                WHEN from_address = 0x06472cabc51a3805975b9c60c7dec63897c9a287f2db173a1d6c589d18dd1e07 THEN 'eurSPKCC'
                WHEN from_address = 0x0128f41ef8017ab56140ffad6439305a3196ed862841ba61ff4d78e380c346a6 THEN 'SAFO-EUR'
                WHEN from_address = 0x035bdc17f7a7d09c45d31ab476a576d4f7aad916676b2948fe172c3bcb33725a THEN 'SAFO-USD'
                WHEN from_address = 0x006e8a99926ff6d56f4cb93c37b63286d736cdf1f81740d53f88b4875b4cbe7f49 THEN 'SAFO-GBP'
                WHEN from_address = 0x06723dcb428eddb160c5adfc2d0a5e5adc184bf6a7298780c3cbf3fa764f709b THEN 'SAFO-CHF'
            END AS product,
            SUM(CASE
                -- Mint: from = zero address (keys[2])
                WHEN cardinality(keys) >= 2
                     AND keys[2] = 0x0000000000000000000000000000000000000000000000000000000000000000
                    THEN  CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5
                -- Burn: to = zero address (keys[3])
                WHEN cardinality(keys) >= 3
                     AND keys[3] = 0x0000000000000000000000000000000000000000000000000000000000000000
                    THEN -CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5
                ELSE 0
            END) AS net_flow
        FROM starknet.events
        WHERE block_date >= (SELECT scan_cutoff FROM checkpoint)
          AND cardinality(keys) >= 2
          AND cardinality(data) >= 1
          AND keys[1] = 0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9
          AND from_address IN (
              0x020ff2f6021ada9edbceaf31b96f9f67b746662a6e6b2bc9d30c0d3e290a71f6,
              0x04f5e0de717daa6aa8de63b1bf2e8d7823ec5b21a88461b1519d9dbc956fb7f2,
              0x0153d6e0462080bb2842109e9b64f589ef5aa06bb32b26bbdb894aca92674395,
              0x04bade88e79a6120f893d64e51006ac6853eceeefa1a50868d19601b1f0a567d,
              0x06472cabc51a3805975b9c60c7dec63897c9a287f2db173a1d6c589d18dd1e07,
              0x0128f41ef8017ab56140ffad6439305a3196ed862841ba61ff4d78e380c346a6,
              0x035bdc17f7a7d09c45d31ab476a576d4f7aad916676b2948fe172c3bcb33725a,
              0x006e8a99926ff6d56f4cb93c37b63286d736cdf1f81740d53f88b4875b4cbe7f49,
              0x06723dcb428eddb160c5adfc2d0a5e5adc184bf6a7298780c3cbf3fa764f709b
          )
        GROUP BY 1, 2
    ) t
    WHERE product IS NOT NULL
    GROUP BY 1, 2
),

-- ── 8) STELLAR NET FLOWS ─────────────────────────────────────
--    Full history scan required (LAG needs origin)
stellar_raw AS (
    SELECT
        closed_at,
        DATE_TRUNC('day', closed_at) AS day,
        contract_id,
        json_extract_scalar(key_decoded, '$.vec[1].address') AS holder,
        TRY_CAST(json_extract_scalar(val_decoded, '$.i128') AS DOUBLE) / 1e5 AS balance
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
stellar_deltas AS (
    SELECT day, contract_id,
        balance - COALESCE(
            LAG(balance) OVER (PARTITION BY contract_id, holder ORDER BY closed_at), 0
        ) AS delta
    FROM stellar_raw
),
stellar_flows AS (
    SELECT day,
        CASE
            WHEN contract_id = 'CARUUX2FZNPH6DGJOEUFSIUQWYHNL5AVDV7PMVSHWL7OBYIBFC76F4TO' THEN 'USTBL'
            WHEN contract_id = 'CBGV2QFQBBGEQRUKUMCPO3SZOHDDYO6SCP5CH6TW7EALKVHCXTMWDDOF' THEN 'EUTBL'
            WHEN contract_id = 'CDT3KU6TQZNOHKNOHNAFFDQZDURVC3MSTL4ML7TUTZGNOPBZCLABP4FR' THEN 'UKTBL'
            WHEN contract_id = 'CDS2GCAQTNQINSCJUJIVBJXILKBWP5PU7LOBGHMP3X47QCQBFKPMTCNT' THEN 'SPKCC'
            WHEN contract_id = 'CDWOB6T7SVSMMQN5V3P2OPTBAXOP7DAZHGVW3PYTZIKHVFKN6TBSXR6A' THEN 'eurSPKCC'
            WHEN contract_id = 'CBOOCGZSVRSZFRE4U2NWR2B4RXYVJWRCBTGOUD2JPI2TDJPWMTJX7FZP' THEN 'SAFO-EUR'
            WHEN contract_id = 'CDGSC6BA4TCAOVSFQCUEHDMOIIHYYVNYBT6YEARS4MX3ITAHUINVGQHX' THEN 'SAFO-USD'
            WHEN contract_id = 'CAGYRRKPFSWKM6SJOE4QAAVYMOSHMDS5WOQ4T5A2E6XNCU7LZZKUNQKP' THEN 'SAFO-GBP'
            WHEN contract_id = 'CAJD2IBSP7VO2VYJQUYJSOGPJINTUYV7MQITIINXVPTIH3CCLCUENNMW4' THEN 'SAFO-CHF'
        END AS product,
        SUM(delta) AS net_flow
    FROM stellar_deltas
    WHERE day >= (SELECT scan_cutoff FROM checkpoint)
    GROUP BY 1, 2
),

-- ── 9) ALL NEW FLOWS — combined ──────────────────────────────
new_flows AS (
    SELECT day, product, SUM(net_flow) AS daily_net_flow
    FROM (
        SELECT day, product, net_flow FROM evm_flows
        UNION ALL
        SELECT day, product, net_flow FROM starknet_flows  WHERE product IS NOT NULL
        UNION ALL
        SELECT day, product, net_flow FROM stellar_flows   WHERE product IS NOT NULL
    ) t
    GROUP BY 1, 2
),

-- ── 10) FILL CALENDAR × PRODUCTS (0 if no flow that day) ─────
products AS (
    SELECT product FROM (VALUES
        ('USTBL'),('EUTBL'),('UKTBL'),('SPKCC'),('eurSPKCC'),
        ('SAFO-EUR'),('SAFO-USD'),('SAFO-GBP'),('SAFO-CHF')
    ) AS t(product)
),
new_daily AS (
    SELECT
        c.day,
        p.product,
        COALESCE(a.daily_net_flow, 0) AS daily_net_flow
    FROM calendar_new c
    CROSS JOIN products p
    LEFT JOIN new_flows a ON c.day = a.day AND p.product = a.product
),

-- ── 11) USD VALUATION — new flows ────────────────────────────
new_valued AS (
    SELECT
        f.day,
        f.product,
        f.daily_net_flow,
        CASE
            WHEN f.product IN ('USTBL','SPKCC','SAFO-USD')    THEN 'USD'
            WHEN f.product IN ('EUTBL','eurSPKCC','SAFO-EUR') THEN 'EUR'
            WHEN f.product IN ('UKTBL','SAFO-GBP')             THEN 'GBP'
            WHEN f.product = 'SAFO-CHF'                        THEN 'CHF'
        END AS currency,
        f.daily_net_flow
            * CASE f.product
                WHEN 'USTBL'    THEN pr.p_ustbl
                WHEN 'EUTBL'    THEN pr.p_eutbl
                WHEN 'UKTBL'    THEN pr.p_uktbl
                WHEN 'SPKCC'    THEN pr.p_spkcc
                WHEN 'eurSPKCC' THEN pr.p_eurspkcc
                WHEN 'SAFO-USD' THEN pr.p_safo_usd
                WHEN 'SAFO-EUR' THEN pr.p_safo_eur
                WHEN 'SAFO-GBP' THEN pr.p_safo_gbp
                WHEN 'SAFO-CHF' THEN pr.p_safo_chf
            END
            * CASE f.product
                WHEN 'USTBL'    THEN 1.0
                WHEN 'EUTBL'    THEN fx.rate_eur_usd
                WHEN 'UKTBL'    THEN fx.rate_gbp_usd
                WHEN 'SPKCC'    THEN 1.0
                WHEN 'eurSPKCC' THEN fx.rate_eur_usd
                WHEN 'SAFO-EUR' THEN fx.rate_eur_usd
                WHEN 'SAFO-USD' THEN 1.0
                WHEN 'SAFO-GBP' THEN fx.rate_gbp_usd
                WHEN 'SAFO-CHF' THEN 1.13
            END AS daily_net_flow_usd
    FROM new_daily f
    LEFT JOIN filled_prices pr ON f.day = pr.day
    LEFT JOIN filled_forex  fx ON f.day = fx.day
),

-- ── 12) COMBINE prev + new ────────────────────────────────────
--    FIX: prev keeps rows up to AND INCLUDING output_cutoff
--    new_valued emits only days strictly AFTER output_cutoff
--    Rolling windows need full history → compute over all_daily
all_daily AS (
    SELECT day, product, currency, daily_net_flow, daily_net_flow_usd
    FROM prev
    WHERE day <= (SELECT output_cutoff FROM checkpoint)

    UNION ALL

    SELECT day, product, currency, daily_net_flow, daily_net_flow_usd
    FROM new_valued
    WHERE day > (SELECT output_cutoff FROM checkpoint)
),

-- ── 13) ROLLING WINDOWS — full recompute over all history ────
--    Intentional: 7d/30d windows must span the prev/new boundary
with_rolling AS (
    SELECT
        day, product, currency,
        daily_net_flow,
        daily_net_flow_usd,
        SUM(daily_net_flow) OVER (
            PARTITION BY product ORDER BY day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_tokens,
        SUM(daily_net_flow) OVER (
            PARTITION BY product ORDER BY day
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_30d_tokens,
        SUM(daily_net_flow_usd) OVER (
            PARTITION BY product ORDER BY day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_usd,
        SUM(daily_net_flow_usd) OVER (
            PARTITION BY product ORDER BY day
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_30d_usd,
        SUM(daily_net_flow) OVER (
            PARTITION BY product ORDER BY day
        ) AS cum_net_flow
    FROM all_daily
)

-- ── 14) FINAL OUTPUT ─────────────────────────────────────────
SELECT
    day,
    product,
    currency,
    daily_net_flow,
    daily_net_flow_usd,
    rolling_7d_tokens,
    rolling_7d_usd,
    rolling_30d_tokens,
    rolling_30d_usd,
    cum_net_flow,
    CASE
        WHEN rolling_7d_usd > rolling_30d_usd THEN '🟢 Accelerating'
        WHEN rolling_7d_usd < rolling_30d_usd THEN '🔴 Decelerating'
        ELSE                                        '⚪ Neutral'
    END AS momentum_signal
FROM with_rolling
WHERE day >= DATE '2024-05-30'  -- skip 30d warmup
ORDER BY day DESC, product