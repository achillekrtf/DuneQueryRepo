-- part of a query repo
-- query name: TVL by Spiko Product on Arbitrum (USD)
-- query link: https://dune.com/queries/6659119


WITH 

calendar AS (
    SELECT date_trunc('day', period) as day
    FROM unnest(sequence(DATE '2024-04-30', current_date, interval '1' day)) as t(period)
),


forex_logs_raw AS (
    SELECT 
        date_trunc('day', block_time) as day, 
        contract_address, 
        bytearray_to_int256(topic1) as rate_raw
    FROM arbitrum.logs
    WHERE block_date >= DATE '2024-04-30'
      AND topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f
      AND contract_address IN (0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871, 0x78f28D363533695458696b42577D2e1728cEa3D1)
),
daily_forex AS (
    SELECT day,
        arbitrary(rate_raw) FILTER (WHERE contract_address = 0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871) as eur_raw,
        arbitrary(rate_raw) FILTER (WHERE contract_address = 0x78f28D363533695458696b42577D2e1728cEa3D1) as gbp_raw
    FROM forex_logs_raw 
    GROUP BY 1
),
filled_forex AS (
    SELECT c.day,
        COALESCE(last_value(f.eur_raw) IGNORE NULLS OVER (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 1e-8, 1.08) as rate_eur_usd,
        COALESCE(last_value(f.gbp_raw) IGNORE NULLS OVER (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) * 1e-8, 1.27) as rate_gbp_usd
    FROM calendar c
    LEFT JOIN daily_forex f ON c.day = f.day
),


price_logs AS (
    SELECT date_trunc('day', block_time) as day, contract_address, topic0, data, topic1
    FROM ethereum.logs
    WHERE block_date >= DATE '2024-04-30'
      AND contract_address IN (
        0xC1C24f0f2103F5899b7AB415A1930E519B7D3423, -- USTBL
        0xdaA1c6511Aa051e9e83Dd7Ac2D65d5E41D1f6b98, -- EUTBL
        0x9e37DBF40fE5Fe9320E45fe6B95b000aa05459A9, -- SPKCC
        0x4B2C406f0Dbf7624a32971277DA7B4C43A7A942b  -- eurSPKCC
    )
),
daily_prices_raw AS (
    SELECT day,

        arbitrary(bytearray_to_uint256(bytearray_substring(data, 33, 32))) FILTER (WHERE contract_address = 0xC1C24f0f2103F5899b7AB415A1930E519B7D3423) as ustbl_raw,
        arbitrary(bytearray_to_uint256(bytearray_substring(data, 33, 32))) FILTER (WHERE contract_address = 0xdaA1c6511Aa051e9e83Dd7Ac2D65d5E41D1f6b98) as eutbl_raw,
        arbitrary(bytearray_to_int256(topic1)) FILTER (WHERE contract_address = 0x9e37DBF40fE5Fe9320E45fe6B95b000aa05459A9) as cc_usd_raw,
        arbitrary(COALESCE(bytearray_to_int256(topic1), bytearray_to_int256(bytearray_substring(data, 1, 32)))) FILTER (WHERE contract_address = 0x4B2C406f0Dbf7624a32971277DA7B4C43A7A942b) as cc_eur_raw
    FROM price_logs
    GROUP BY 1
),
uktbl_price_log AS (
    SELECT date_trunc('day', block_time) as day, arbitrary(bytearray_to_uint256(bytearray_substring(data, 1 + 32, 32))) as uktbl_raw
    FROM arbitrum.logs 
    WHERE block_date >= DATE '2024-04-30'
      AND contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59 
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
    GROUP BY 1
),
filled_prices AS (
    SELECT c.day,
        COALESCE(last_value(p.ustbl_raw) IGNORE NULLS OVER (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 1000000) * 1e-6 as p_ustbl,
        COALESCE(last_value(p.eutbl_raw) IGNORE NULLS OVER (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 1000000) * 1e-6 as p_eutbl,
        COALESCE(last_value(u.uktbl_raw) IGNORE NULLS OVER (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 1000000) * 1e-6 as p_uktbl,
        
        COALESCE(last_value(p.cc_usd_raw) IGNORE NULLS OVER (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 100000000) * 1e-8 as p_spkcc,
        COALESCE(last_value(p.cc_eur_raw) IGNORE NULLS OVER (ORDER BY c.day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 100000000) * 1e-8 as p_eurspkcc
    FROM calendar c
    LEFT JOIN daily_prices_raw p ON c.day = p.day
    LEFT JOIN uktbl_price_log u ON c.day = u.day
),


evm_transfers AS (
    SELECT 
        DATE_TRUNC('day', evt_block_time) AS day,
        CASE 
            WHEN contract_address = 0xe4880249745eAc5F1eD9d8F7DF844792D560e750 AND blockchain IN ('ethereum', 'polygon', 'base', 'etherlink') THEN 'USTBL'
            WHEN contract_address = 0x021289588cd81dC1AC87ea91e91607eEF68303F5 AND blockchain = 'arbitrum' THEN 'USTBL'
            WHEN contract_address = 0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80 AND blockchain IN ('ethereum', 'polygon', 'base', 'etherlink') THEN 'EUTBL'
            WHEN contract_address = 0xCBeb19549054CC0a6257A77736FC78C367216cE7 AND blockchain = 'arbitrum' THEN 'EUTBL'
            WHEN contract_address = 0xf695Df6c0f3bB45918A7A82e83348FC59517734E AND blockchain = 'ethereum' THEN 'UKTBL'
            WHEN contract_address = 0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D AND blockchain IN ('polygon', 'etherlink') THEN 'UKTBL'
            WHEN contract_address = 0x903d5990119bC799423e9C25c56518Ba7DD19474 AND blockchain = 'arbitrum' THEN 'UKTBL'
            WHEN contract_address = 0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068 AND blockchain = 'base' THEN 'UKTBL'
            WHEN contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59 AND blockchain IN ('ethereum', 'etherlink') THEN 'SPKCC'
            WHEN contract_address = 0x903d5990119bC799423e9C25c56518Ba7DD19474 AND blockchain = 'polygon' THEN 'SPKCC'
            WHEN contract_address = 0x99F70A0e1786402a6796c6B0AA997ef340a5c6da AND blockchain = 'arbitrum' THEN 'SPKCC'
            WHEN contract_address = 0xf695Df6c0f3bB45918A7A82e83348FC59517734E AND blockchain = 'base' THEN 'SPKCC'
            WHEN contract_address = 0x3868D4e336d14D38031cf680329d31e4712e11cC AND blockchain IN ('ethereum', 'etherlink') THEN 'eurSPKCC'
            WHEN contract_address = 0x99F70A0e1786402a6796c6B0AA997ef340a5c6da AND blockchain = 'polygon' THEN 'eurSPKCC'
            WHEN contract_address = 0x0e389C83Bc1d16d86412476F6103027555C03265 AND blockchain = 'arbitrum' THEN 'eurSPKCC'
            WHEN contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59 AND blockchain = 'base' THEN 'eurSPKCC'
        END as product,
        SUM(CASE WHEN "from" = 0x0000000000000000000000000000000000000000 THEN value WHEN "to" = 0x0000000000000000000000000000000000000000 THEN -value ELSE 0 END) / 1e5 as change 
    FROM evms.erc20_transfers
    WHERE evt_block_time >= TIMESTAMP '2024-04-30 00:00:00' 
    AND blockchain IN ('ethereum', 'polygon', 'arbitrum', 'base', 'etherlink') 
    AND contract_address IN (
        0xe4880249745eAc5F1eD9d8F7DF844792D560e750, 0x021289588cd81dC1AC87ea91e91607eEF68303F5,
        0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80, 0xCBeb19549054CC0a6257A77736FC78C367216cE7,
        0xf695Df6c0f3bB45918A7A82e83348FC59517734E, 0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D, 
        0x903d5990119bC799423e9C25c56518Ba7DD19474, 0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068,
        0x4f33aCf823E6eEb697180d553cE0c710124C8D59, 0x99F70A0e1786402a6796c6B0AA997ef340a5c6da,
        0x3868D4e336d14D38031cf680329d31e4712e11cC, 0x0e389C83Bc1d16d86412476F6103027555C03265
    )
    GROUP BY 1, 2
),


starknet_transfers AS (
    SELECT DATE_TRUNC('day', block_date) AS day,
    CASE 
        WHEN from_address = 0x020ff2f6021ada9edbceaf31b96f9f67b746662a6e6b2bc9d30c0d3e290a71f6 THEN 'USTBL' 
        WHEN from_address = 0x04f5e0de717daa6aa8de63b1bf2e8d7823ec5b21a88461b1519d9dbc956fb7f2 THEN 'EUTBL' 
        WHEN from_address = 0x0153d6e0462080bb2842109e9b64f589ef5aa06bb32b26bbdb894aca92674395 THEN 'UKTBL'
        WHEN from_address = 0x04bade88e79a6120f893d64e51006ac6853eceeefa1a50868d19601b1f0a567d THEN 'SPKCC'
        WHEN from_address = 0x06472cabc51a3805975b9c60c7dec63897c9a287f2db173a1d6c589d18dd1e07 THEN 'eurSPKCC'
    END as product,
    SUM(CASE 
        WHEN cardinality(data) >= 3 THEN
            CASE 
                WHEN TRY_CAST(element_at(data, 2) AS VARCHAR) = '0x0000000000000000000000000000000000000000000000000000000000000000' THEN -CAST(VARBINARY_TO_INT256(element_at(data, 3)) AS DOUBLE) / 1e5
                WHEN TRY_CAST(element_at(data, 1) AS VARCHAR) = '0x0000000000000000000000000000000000000000000000000000000000000000' THEN CAST(VARBINARY_TO_INT256(element_at(data, 3)) AS DOUBLE) / 1e5
                ELSE 0 
            END
        ELSE 0 
    END) as change
    FROM starknet.events
    WHERE block_date >= DATE '2024-04-30' 
    AND keys[1] = 0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9
    AND from_address IN (
        0x020ff2f6021ada9edbceaf31b96f9f67b746662a6e6b2bc9d30c0d3e290a71f6, 
        0x04f5e0de717daa6aa8de63b1bf2e8d7823ec5b21a88461b1519d9dbc956fb7f2, 
        0x0153d6e0462080bb2842109e9b64f589ef5aa06bb32b26bbdb894aca92674395, 
        0x04bade88e79a6120f893d64e51006ac6853eceeefa1a50868d19601b1f0a567d, 
        0x06472cabc51a3805975b9c60c7dec63897c9a287f2db173a1d6c589d18dd1e07
    )
    GROUP BY 1, 2
),


stellar_raw_updates AS (
    SELECT closed_at, DATE_TRUNC('day', closed_at) AS day, contract_id, json_extract_scalar(key_decoded, '$.vec[1].address') AS holder,
    TRY_CAST(json_extract_scalar(val_decoded, '$.i128') AS DOUBLE) / 1e5 AS balance
    FROM stellar.contract_data
    WHERE closed_at_date >= DATE '2024-04-30' 
    AND contract_id IN ('CARUUX2FZNPH6DGJOEUFSIUQWYHNL5AVDV7PMVSHWL7OBYIBFC76F4TO', 'CBGV2QFQBBGEQRUKUMCPO3SZOHDDYO6SCP5CH6TW7EALKVHCXTMWDDOF', 'CDT3KU6TQZNOHKNOHNAFFDQZDURVC3MSTL4ML7TUTZGNOPBZCLABP4FR', 'CDS2GCAQTNQINSCJUJIVBJXILKBWP5PU7LOBGHMP3X47QCQBFKPMTCNT', 'CDWOB6T7SVSMMQN5V3P2OPTBAXOP7DAZHGVW3PYTZIKHVFKN6TBSXR6A')
    AND deleted = false AND contract_key_type = 'ScValTypeScvVec' AND json_extract_scalar(key_decoded, '$.vec[0].symbol') = 'Balance'
),
stellar_balance_deltas AS (
    SELECT day, contract_id, holder, (balance - COALESCE(LAG(balance) OVER (PARTITION BY contract_id, holder ORDER BY closed_at), 0)) AS delta
    FROM stellar_raw_updates
),
stellar_transfers AS (
    SELECT day, 
    CASE 
        WHEN contract_id = 'CARUUX2FZNPH6DGJOEUFSIUQWYHNL5AVDV7PMVSHWL7OBYIBFC76F4TO' THEN 'USTBL' 
        WHEN contract_id = 'CBGV2QFQBBGEQRUKUMCPO3SZOHDDYO6SCP5CH6TW7EALKVHCXTMWDDOF' THEN 'EUTBL' 
        WHEN contract_id = 'CDT3KU6TQZNOHKNOHNAFFDQZDURVC3MSTL4ML7TUTZGNOPBZCLABP4FR' THEN 'UKTBL' 
        WHEN contract_id = 'CDS2GCAQTNQINSCJUJIVBJXILKBWP5PU7LOBGHMP3X47QCQBFKPMTCNT' THEN 'SPKCC' 
        WHEN contract_id = 'CDWOB6T7SVSMMQN5V3P2OPTBAXOP7DAZHGVW3PYTZIKHVFKN6TBSXR6A' THEN 'eurSPKCC' 
    END as product,
    SUM(delta) as change
    FROM stellar_balance_deltas GROUP BY 1, 2
),

aggregated_changes AS (
    SELECT day, product, SUM(daily_gross_change) as daily_gross_change
    FROM (
        SELECT day, product, change as daily_gross_change FROM evm_transfers
        UNION ALL SELECT day, product, change FROM starknet_transfers
        UNION ALL SELECT day, product, change FROM stellar_transfers
    ) t
    GROUP BY 1, 2
),
daily_supply_calc AS (
    SELECT 
        c.day,
        p.product,
        SUM(COALESCE(a.daily_gross_change, 0)) OVER (PARTITION BY p.product ORDER BY c.day) as gross_supply
    FROM calendar c
    CROSS JOIN (SELECT 'USTBL' as product UNION ALL SELECT 'EUTBL' UNION ALL SELECT 'UKTBL' UNION ALL SELECT 'SPKCC' UNION ALL SELECT 'eurSPKCC') p
    LEFT JOIN aggregated_changes a ON c.day = a.day AND p.product = a.product
),
final_view AS (
    SELECT 
        d.day, d.product,
        d.gross_supply as net_supply,
        
        CASE 
            WHEN d.product IN ('USTBL', 'SPKCC') THEN 'USD'
            WHEN d.product IN ('EUTBL', 'eurSPKCC') THEN 'EUR'
            WHEN d.product = 'UKTBL' THEN 'GBP'
        END as currency,
        
        CASE 
            WHEN d.product = 'USTBL' THEN p.p_ustbl
            WHEN d.product = 'EUTBL' THEN p.p_eutbl
            WHEN d.product = 'UKTBL' THEN p.p_uktbl
            WHEN d.product = 'SPKCC' THEN p.p_spkcc
            WHEN d.product = 'eurSPKCC' THEN p.p_eurspkcc
        END as nav_price,
        
        CASE 
            WHEN d.product IN ('USTBL', 'SPKCC') THEN 1.0
            WHEN d.product IN ('EUTBL', 'eurSPKCC') THEN f.rate_eur_usd
            WHEN d.product = 'UKTBL' THEN f.rate_gbp_usd
        END as fx_rate
    FROM daily_supply_calc d
    LEFT JOIN filled_prices p ON d.day = p.day
    LEFT JOIN filled_forex f ON d.day = f.day
)


SELECT 
    day,
    product,
    net_supply as "Net Circulating Supply",
    currency as "Original Currency",
    nav_price as "NAV Price (Native)",
    
    CASE 
        WHEN IS_NAN(net_supply * nav_price) OR IS_INFINITE(net_supply * nav_price) THEN 0 
        ELSE (net_supply * nav_price) 
    END as "AuM Native",
    
    fx_rate as "FX Rate (to USD)",
    

    CASE 
        WHEN IS_NAN(net_supply * nav_price * fx_rate) OR IS_INFINITE(net_supply * nav_price * fx_rate) THEN 0 
        ELSE (net_supply * nav_price * fx_rate) 
    END as "AuM USD (Final)"
FROM final_view
WHERE day >= date('2024-04-30')
ORDER BY day DESC, product