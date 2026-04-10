-- part of a query repo
-- query name: Net new TVL since 13th October 2025
-- query link: https://dune.com/queries/6663391


WITH 
calendar AS (
    SELECT date_trunc('day', period) as day
    FROM unnest(sequence(date('2024-01-01'), current_date, interval '1' day)) as t(period)
),
forex_calendar AS (
    SELECT date_trunc('day', period) as day
    FROM unnest(sequence(date('2023-01-01'), current_date, interval '1' day)) as t(period)
),
daily_forex_sparse AS (
    SELECT 
        date_trunc('day', block_time) as day,
        arbitrary(bytearray_to_int256(topic1)) FILTER (WHERE contract_address = 0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871) as eur_raw,
        arbitrary(bytearray_to_int256(topic1)) FILTER (WHERE contract_address = 0x78f28D363533695458696b42577D2e1728cEa3D1) as gbp_raw
    FROM arbitrum.logs
    WHERE topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f 
      AND contract_address IN (0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871, 0x78f28D363533695458696b42577D2e1728cEa3D1)
    GROUP BY 1
),
filled_forex AS (
    SELECT c.day,
        last_value(f.eur_raw) IGNORE NULLS OVER (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 1e-8 as rate_eur,
        last_value(f.gbp_raw) IGNORE NULLS OVER (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 1e-8 as rate_gbp
    FROM forex_calendar c
    LEFT JOIN daily_forex_sparse f ON c.day = f.day
),
ustbl_moves AS (
    SELECT date_trunc('day', block_time) as day, 
    SUM(CASE 
        WHEN topic1 = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN bytearray_to_uint256(data)
        WHEN topic2 = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN -cast(bytearray_to_uint256(data) as double)
        ELSE 0 END) / 1e5 as change 
    FROM arbitrum.logs 
    WHERE contract_address = 0x021289588cd81dC1AC87ea91e91607eEF68303F5 AND topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
    GROUP BY 1
),
ustbl_prices AS (
    SELECT date_trunc('day', block_time) as day, arbitrary(bytearray_to_uint256(bytearray_substring(data, 1, 32))) as price FROM ethereum.logs WHERE contract_address = 0xC1C24f0f2103F5899b7AB415A1930E519B7D3423 AND topic0 = 0xc797025feeeaf2cd924c99e9205acb8ec04d5cad21c41ce637a38fb6dee6016a GROUP BY 1
),
data_ustbl AS (
    SELECT c.day, 'USTBL' as product, 'USD' as currency, SUM(COALESCE(m.change, 0)) OVER (ORDER BY c.day) as supply, COALESCE(last_value(p.price) IGNORE NULLS OVER (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 1000000) * 1e-6 as price 
    FROM calendar c LEFT JOIN ustbl_moves m ON c.day = m.day LEFT JOIN ustbl_prices p ON c.day = p.day
),
eutbl_moves AS (
    SELECT date_trunc('day', block_time) as day, 
    SUM(CASE 
        WHEN topic1 = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN bytearray_to_uint256(data)
        WHEN topic2 = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN -cast(bytearray_to_uint256(data) as double)
        ELSE 0 END) / 1e5 as change 
    FROM arbitrum.logs 
    WHERE contract_address = 0xcbeb19549054cc0a6257a77736fc78c367216ce7 AND topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
    GROUP BY 1
),
eutbl_prices AS (
    SELECT date_trunc('day', block_time) as day, arbitrary(bytearray_to_uint256(bytearray_substring(data, 1, 32))) as price FROM ethereum.logs WHERE contract_address = 0xdaA1c6511Aa051e9e83Dd7Ac2D65d5E41D1f6b98 AND topic0 = 0xc797025feeeaf2cd924c99e9205acb8ec04d5cad21c41ce637a38fb6dee6016a GROUP BY 1
),
data_eutbl AS (
    SELECT c.day, 'EUTBL' as product, 'EUR' as currency, SUM(COALESCE(m.change, 0)) OVER (ORDER BY c.day) as supply, COALESCE(last_value(p.price) IGNORE NULLS OVER (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 1000000) * 1e-6 as price 
    FROM calendar c LEFT JOIN eutbl_moves m ON c.day = m.day LEFT JOIN eutbl_prices p ON c.day = p.day
),
uktbl_moves AS (
    SELECT date_trunc('day', block_time) as day, 
    SUM(CASE 
        WHEN topic1 = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN bytearray_to_uint256(data)
        WHEN topic2 = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN -cast(bytearray_to_uint256(data) as double)
        ELSE 0 END) / 1e5 as change 
    FROM arbitrum.logs 
    WHERE contract_address = 0x903d5990119bC799423e9C25c56518Ba7DD19474 AND topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
    GROUP BY 1
),
uktbl_prices AS (
    SELECT date_trunc('day', block_time) as day, arbitrary(bytearray_to_uint256(bytearray_substring(data, 1 + 32, 32))) as price FROM arbitrum.logs WHERE contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59 AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6 GROUP BY 1
),
data_uktbl AS (
    SELECT c.day, 'UKTBL' as product, 'GBP' as currency, SUM(COALESCE(m.change, 0)) OVER (ORDER BY c.day) as supply, COALESCE(last_value(p.price) IGNORE NULLS OVER (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 1000000) * 1e-6 as price 
    FROM calendar c LEFT JOIN uktbl_moves m ON c.day = m.day LEFT JOIN uktbl_prices p ON c.day = p.day
),
cc_usd_moves AS (
    SELECT date_trunc('day', block_time) as day, 
    SUM(CASE 
        WHEN topic1 = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN bytearray_to_uint256(data)
        WHEN topic2 = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN -cast(bytearray_to_uint256(data) as double)
        ELSE 0 END) / 1e5 as change 
    FROM arbitrum.logs 
    WHERE contract_address = 0x99F70A0e1786402a6796c6B0AA997ef340a5c6da AND topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
    GROUP BY 1
),
cc_usd_prices AS (
    SELECT date_trunc('day', block_time) as day, arbitrary(bytearray_to_int256(topic1)) as price FROM ethereum.logs WHERE contract_address = 0x9e37DBF40fE5Fe9320E45fe6B95b000aa05459A9 AND topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f GROUP BY 1
),
data_cc_usd AS (
    SELECT c.day, 'C&C USD' as product, 'USD' as currency, SUM(COALESCE(m.change, 0)) OVER (ORDER BY c.day) as supply, COALESCE(last_value(p.price) IGNORE NULLS OVER (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 100000000) * 1e-8 as price 
    FROM calendar c LEFT JOIN cc_usd_moves m ON c.day = m.day LEFT JOIN cc_usd_prices p ON c.day = p.day
),
cc_eur_moves AS (
    SELECT date_trunc('day', block_time) as day, 
    SUM(CASE 
        WHEN topic1 = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN bytearray_to_uint256(data)
        WHEN topic2 = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN -cast(bytearray_to_uint256(data) as double)
        ELSE 0 END) / 1e5 as change 
    FROM arbitrum.logs 
    WHERE contract_address = 0x0e389C83Bc1d16d86412476F6103027555C03265 AND topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
    GROUP BY 1
),
cc_eur_prices AS (
    SELECT date_trunc('day', block_time) as day, arbitrary(COALESCE(bytearray_to_int256(topic1), bytearray_to_int256(bytearray_substring(data, 1, 32)))) as price FROM ethereum.logs WHERE contract_address = 0x4B2C406f0Dbf7624a32971277DA7B4C43A7A942b AND topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f GROUP BY 1
),
data_cc_eur AS (
    SELECT c.day, 'C&C EUR' as product, 'EUR' as currency, SUM(COALESCE(m.change, 0)) OVER (ORDER BY c.day) as supply, COALESCE(last_value(p.price) IGNORE NULLS OVER (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 100000000) * 1e-8 as price 
    FROM calendar c LEFT JOIN cc_eur_moves m ON c.day = m.day LEFT JOIN cc_eur_prices p ON c.day = p.day
),
all_products AS (
    SELECT * FROM data_ustbl
    UNION ALL SELECT * FROM data_eutbl
    UNION ALL SELECT * FROM data_uktbl
    UNION ALL SELECT * FROM data_cc_usd
    UNION ALL SELECT * FROM data_cc_eur
),
daily_aggregated AS (
    SELECT 
        d.day,
        SUM(
            (d.supply * d.price) * CASE 
                WHEN d.currency = 'EUR' THEN COALESCE(f.rate_eur, 1.05)
                WHEN d.currency = 'GBP' THEN COALESCE(f.rate_gbp, 1.25)
                ELSE 1 
            END
        ) as total_tvl_usd
    FROM all_products d
    LEFT JOIN filled_forex f ON d.day = f.day
    GROUP BY 1
),
baseline_calc AS (
    SELECT 
        day, 
        total_tvl_usd,
        first_value(total_tvl_usd) OVER (ORDER BY day ASC) as base_val
    FROM daily_aggregated
    WHERE day >= date('2025-10-13')
)
SELECT 
    day as "Date",
    total_tvl_usd - base_val as "Net AuM Gain (since Oct 13, 2025)"
FROM baseline_calc
ORDER BY day DESC
LIMIT 1