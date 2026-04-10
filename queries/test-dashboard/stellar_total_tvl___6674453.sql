-- part of a query repo
-- query name: Stellar Total TVL
-- query link: https://dune.com/queries/6674453


WITH 
-- 1. Master Time Series
master_calendar AS (
    SELECT date_trunc('day', period) as day
    FROM unnest(sequence(date('2024-04-30'), current_date, interval '1' day)) as t(period)
),

-- 2. Forex Rates (Arbitrum)
forex_raw AS (
    SELECT 
        date_trunc('day', block_time) as day,
        arbitrary(bytearray_to_int256(topic1)) FILTER (WHERE contract_address = 0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871) as eur_raw,
        arbitrary(bytearray_to_int256(topic1)) FILTER (WHERE contract_address = 0x78f28D363533695458696b42577D2e1728cEa3D1) as gbp_raw
    FROM arbitrum.logs
    WHERE topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f 
      AND contract_address IN (0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871, 0x78f28D363533695458696b42577D2e1728cEa3D1)
    GROUP BY 1
),
filled_forex_rates AS (
    SELECT 
        mc.day,
        last_value(fr.eur_raw) IGNORE NULLS OVER (ORDER BY mc.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 1e-8 as rate_eur,
        last_value(fr.gbp_raw) IGNORE NULLS OVER (ORDER BY mc.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 1e-8 as rate_gbp
    FROM master_calendar mc
    LEFT JOIN forex_raw fr ON mc.day = fr.day
),

-- 3. Product Prices (Ethereum/Arbitrum)
price_logs_raw AS (
    SELECT day,
        arbitrary(bytearray_to_uint256(bytearray_substring(data, 1, 32))) FILTER (WHERE contract_address = 0xC1C24f0f2103F5899b7AB415A1930E519B7D3423) as ustbl_raw,
        arbitrary(bytearray_to_uint256(bytearray_substring(data, 1, 32))) FILTER (WHERE contract_address = 0xdaA1c6511Aa051e9e83Dd7Ac2D65d5E41D1f6b98) as eutbl_raw,
        arbitrary(bytearray_to_int256(topic1)) FILTER (WHERE contract_address = 0x9e37DBF40fE5Fe9320E45fe6B95b000aa05459A9) as cc_usd_raw,
        arbitrary(COALESCE(bytearray_to_int256(topic1), bytearray_to_int256(bytearray_substring(data, 1, 32)))) FILTER (WHERE contract_address = 0x4B2C406f0Dbf7624a32971277DA7B4C43A7A942b) as cc_eur_raw
    FROM (
        SELECT date_trunc('day', block_time) as day, contract_address, data, topic1
        FROM ethereum.logs
        WHERE contract_address IN (0xC1C24f0f2103F5899b7AB415A1930E519B7D3423, 0xdaA1c6511Aa051e9e83Dd7Ac2D65d5E41D1f6b98, 0x9e37DBF40fE5Fe9320E45fe6B95b000aa05459A9, 0x4B2C406f0Dbf7624a32971277DA7B4C43A7A942b)
    ) t
    GROUP BY day
),
uktbl_log_raw AS (
    SELECT date_trunc('day', block_time) as day, arbitrary(bytearray_to_uint256(bytearray_substring(data, 1 + 32, 32))) as uktbl_raw
    FROM arbitrum.logs 
    WHERE contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59 AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
    GROUP BY 1
),
filled_product_prices AS (
    SELECT mc.day,
        COALESCE(last_value(p.ustbl_raw) IGNORE NULLS OVER (ORDER BY mc.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 1000000) * 1e-6 as price_ustbl,
        COALESCE(last_value(p.eutbl_raw) IGNORE NULLS OVER (ORDER BY mc.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 1000000) * 1e-6 as price_eutbl,
        COALESCE(last_value(u.uktbl_raw) IGNORE NULLS OVER (ORDER BY mc.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 1000000) * 1e-6 as price_uktbl,
        COALESCE(last_value(p.cc_usd_raw) IGNORE NULLS OVER (ORDER BY mc.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 100000000) * 1e-8 as price_cc_usd,
        COALESCE(last_value(p.cc_eur_raw) IGNORE NULLS OVER (ORDER BY mc.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 100000000) * 1e-8 as price_cc_eur
    FROM master_calendar mc
    LEFT JOIN price_logs_raw p ON mc.day = p.day
    LEFT JOIN uktbl_log_raw u ON mc.day = u.day
),

-- 4. Stellar Supply Calculation (Nested to avoid LAG in SUM error)
stellar_balances_base AS (
    SELECT
        closed_at,
        DATE_TRUNC('day', closed_at) AS time,
        contract_id,
        json_extract_scalar(key_decoded, '$.vec[1].address') AS holder,
        TRY_CAST(json_extract_scalar(val_decoded, '$.i128') AS DOUBLE) / 1e5 AS balance -- UKTBL and others set to 1e5
    FROM stellar.contract_data
    WHERE contract_id IN (
        'CARUUX2FZNPH6DGJOEUFSIUQWYHNL5AVDV7PMVSHWL7OBYIBFC76F4TO',
        'CBGV2QFQBBGEQRUKUMCPO3SZOHDDYO6SCP5CH6TW7EALKVHCXTMWDDOF',
        'CDT3KU6TQZNOHKNOHNAFFDQZDURVC3MSTL4ML7TUTZGNOPBZCLABP4FR',
        'CDS2GCAQTNQINSCJUJIVBJXILKBWP5PU7LOBGHMP3X47QCQBFKPMTCNT',
        'CDWOB6T7SVSMMQN5V3P2OPTBAXOP7DAZHGVW3PYTZIKHVFKN6TBSXR6A'
    )
      AND deleted = false
      AND closed_at_date >= DATE '2024-04-30'
      AND contract_key_type = 'ScValTypeScvVec'
      AND json_extract_scalar(key_decoded, '$.vec[0].symbol') = 'Balance'
),
stellar_deltas_final AS (
    SELECT 
        time,
        SUM(CASE WHEN contract_id = 'CARUUX2FZNPH6DGJOEUFSIUQWYHNL5AVDV7PMVSHWL7OBYIBFC76F4TO' THEN delta ELSE 0 END) as d_ustbl,
        SUM(CASE WHEN contract_id = 'CBGV2QFQBBGEQRUKUMCPO3SZOHDDYO6SCP5CH6TW7EALKVHCXTMWDDOF' THEN delta ELSE 0 END) as d_eurtbl,
        SUM(CASE WHEN contract_id = 'CDT3KU6TQZNOHKNOHNAFFDQZDURVC3MSTL4ML7TUTZGNOPBZCLABP4FR' THEN delta ELSE 0 END) as d_uktbl,
        SUM(CASE WHEN contract_id = 'CDS2GCAQTNQINSCJUJIVBJXILKBWP5PU7LOBGHMP3X47QCQBFKPMTCNT' THEN delta ELSE 0 END) as d_cc_usd,
        SUM(CASE WHEN contract_id = 'CDWOB6T7SVSMMQN5V3P2OPTBAXOP7DAZHGVW3PYTZIKHVFKN6TBSXR6A' THEN delta ELSE 0 END) as d_cc_eur
    FROM (
        SELECT time, contract_id, 
               (balance - COALESCE(LAG(balance) OVER (PARTITION BY contract_id, holder ORDER BY closed_at), 0)) as delta
        FROM stellar_balances_base
    ) t
    GROUP BY 1
),
stellar_cumulative_supply AS (
    SELECT 
        mc.day,
        SUM(COALESCE(sdf.d_ustbl, 0)) OVER (ORDER BY mc.day) as s_ustbl,
        SUM(COALESCE(sdf.d_eurtbl, 0)) OVER (ORDER BY mc.day) as s_eurtbl,
        SUM(COALESCE(sdf.d_uktbl, 0)) OVER (ORDER BY mc.day) as s_uktbl,
        SUM(COALESCE(sdf.d_cc_usd, 0)) OVER (ORDER BY mc.day) as s_cc_usd,
        SUM(COALESCE(sdf.d_cc_eur, 0)) OVER (ORDER BY mc.day) as s_cc_eur
    FROM master_calendar mc
    LEFT JOIN stellar_deltas_final sdf ON mc.day = sdf.time
)

-- 5. Final Output
SELECT 
    scs.day,
    (scs.s_ustbl * fpp.price_ustbl) +
    (scs.s_eurtbl * fpp.price_eutbl * COALESCE(ffr.rate_eur, 1.05)) +
    (scs.s_uktbl * fpp.price_uktbl * COALESCE(ffr.rate_gbp, 1.25)) +
    (scs.s_cc_usd * fpp.price_cc_usd) +
    (scs.s_cc_eur * fpp.price_cc_eur * COALESCE(ffr.rate_eur, 1.05)) as "Total Stellar AuM (USD)"
FROM stellar_cumulative_supply scs
JOIN filled_product_prices fpp ON scs.day = fpp.day
JOIN filled_forex_rates ffr ON scs.day = ffr.day
ORDER BY scs.day DESC
LIMIT 1;