-- part of a query repo
-- query name: Investor Distribution
-- query link: https://dune.com/queries/6846216


-- Investor Distribution by Product (with SAFO)
-- Reconstructs current balances from on-chain transfer events
-- Uses proper DuneSQL: evms.erc20_transfers, starknet.events, stellar.contract_data
WITH evm_labeled AS (
  SELECT
    "from" AS sender,
    "to" AS receiver,
    CASE
      -- USTBL
      WHEN contract_address = 0xe4880249745eAc5F1eD9d8F7DF844792D560e750 AND blockchain IN ('ethereum','polygon','base','etherlink') THEN 'USTBL'
      WHEN contract_address = 0x021289588cd81dC1AC87ea91e91607eEF68303F5 AND blockchain = 'arbitrum' THEN 'USTBL'
      -- EUTBL
      WHEN contract_address = 0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80 AND blockchain IN ('ethereum','polygon','base','etherlink') THEN 'EUTBL'
      WHEN contract_address = 0xCBeb19549054CC0a6257A77736FC78C367216cE7 AND blockchain = 'arbitrum' THEN 'EUTBL'
      -- UKTBL
      WHEN contract_address = 0xf695Df6c0f3bB45918A7A82e83348FC59517734E AND blockchain = 'ethereum' THEN 'UKTBL'
      WHEN contract_address = 0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D AND blockchain IN ('polygon','etherlink') THEN 'UKTBL'
      WHEN contract_address = 0x903d5990119bC799423e9C25c56518Ba7DD19474 AND blockchain = 'arbitrum' THEN 'UKTBL'
      WHEN contract_address = 0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068 AND blockchain = 'base' THEN 'UKTBL'
      -- SPKCC
      WHEN contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59 AND blockchain IN ('ethereum','etherlink') THEN 'SPKCC'
      WHEN contract_address = 0x903d5990119bC799423e9C25c56518Ba7DD19474 AND blockchain = 'polygon' THEN 'SPKCC'
      WHEN contract_address = 0x99F70A0e1786402a6796c6B0AA997ef340a5c6da AND blockchain = 'arbitrum' THEN 'SPKCC'
      WHEN contract_address = 0xf695Df6c0f3bB45918A7A82e83348FC59517734E AND blockchain = 'base' THEN 'SPKCC'
      -- eurSPKCC
      WHEN contract_address = 0x3868D4e336d14D38031cf680329d31e4712e11cC AND blockchain IN ('ethereum','etherlink') THEN 'eurSPKCC'
      WHEN contract_address = 0x99F70A0e1786402a6796c6B0AA997ef340a5c6da AND blockchain = 'polygon' THEN 'eurSPKCC'
      WHEN contract_address = 0x0e389C83Bc1d16d86412476F6103027555C03265 AND blockchain = 'arbitrum' THEN 'eurSPKCC'
      WHEN contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59 AND blockchain = 'base' THEN 'eurSPKCC'
      -- SAFO-EUR
      WHEN contract_address = 0x0990b149e915cb08e2143a5c6f669c907eddc8b0 AND blockchain = 'ethereum' THEN 'SAFO-EUR'
      WHEN contract_address = 0x272ea767712cc4839f4a27ee35eb73116158c8a2 AND blockchain = 'polygon' THEN 'SAFO-EUR'
      WHEN contract_address = 0x1412632f2b89e87bfa20c1318a43ced25f1d7b76 AND blockchain = 'arbitrum' THEN 'SAFO-EUR'
      WHEN contract_address = 0xd879846cbe20751bde8a9342a3cca00a3e56ca47 AND blockchain = 'base' THEN 'SAFO-EUR'
      WHEN contract_address = 0x35dfec1813c43d82e6b87c682f560bbb8ea0c121 AND blockchain = 'etherlink' THEN 'SAFO-EUR'
      -- SAFO-USD
      WHEN contract_address = 0xcbade7d9bdee88411cb6cbcbb29952b742036992 AND blockchain = 'ethereum' THEN 'SAFO-USD'
      WHEN contract_address = 0x6f64f47f95cf656f21b40e14798f6b49f80b3dc5 AND blockchain = 'polygon' THEN 'SAFO-USD'
      WHEN contract_address = 0x0c709396739b9cfb72bcea6ac691ce0ddf66479c AND blockchain = 'arbitrum' THEN 'SAFO-USD'
      WHEN contract_address = 0x0bb754d8940e283d9ff6855ab5dafbc14165c059 AND blockchain = 'base' THEN 'SAFO-USD'
      WHEN contract_address = 0x5677a4dc7484762ffccee13cba20b5c979def446 AND blockchain = 'etherlink' THEN 'SAFO-USD'
      -- SAFO-GBP
      WHEN contract_address = 0xc273986a91e4bfc543610a5cb5860b7cfefb6cc0 AND blockchain = 'ethereum' THEN 'SAFO-GBP'
      WHEN contract_address = 0x4fe515c67eeeadb3282780325f09bb7c244fe774 AND blockchain = 'polygon' THEN 'SAFO-GBP'
      WHEN contract_address = 0xbe023308ac2ef7e1c3799f4e6a3003ee6d342635 AND blockchain = 'arbitrum' THEN 'SAFO-GBP'
      WHEN contract_address = 0x2f6c0e5e06b43512706a9cdf66cd21f723fe0ec3 AND blockchain = 'base' THEN 'SAFO-GBP'
      WHEN contract_address = 0xfe20ebe388149fb2e158b9d10cb95bcfa652262d AND blockchain = 'etherlink' THEN 'SAFO-GBP'
      -- SAFO-CHF
      WHEN contract_address = 0x18b5c15e5196a38a162b1787875295b76e4313fb AND blockchain = 'ethereum' THEN 'SAFO-CHF'
      WHEN contract_address = 0x9de2b2dcdcf43540e47143f28484b6d15118f089 AND blockchain = 'polygon' THEN 'SAFO-CHF'
      WHEN contract_address = 0x97e7962bcd091e7ecfb583fc96289b1e1553ac6e AND blockchain = 'arbitrum' THEN 'SAFO-CHF'
      WHEN contract_address = 0xd9aa2300e126869182dfb6ecf54984e4c687f36b AND blockchain = 'base' THEN 'SAFO-CHF'
      WHEN contract_address = 0xef53e7d17822b641c6481837238a64a688709301 AND blockchain = 'etherlink' THEN 'SAFO-CHF'
    END AS product,
    CAST(value AS DOUBLE) / 1e5 AS amount  -- all Spiko tokens: 5 decimals
  FROM evms.erc20_transfers
  WHERE blockchain IN ('ethereum','polygon','arbitrum','base','etherlink')
    AND contract_address IN (
      0xe4880249745eAc5F1eD9d8F7DF844792D560e750, 0x021289588cd81dC1AC87ea91e91607eEF68303F5,
      0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80, 0xCBeb19549054CC0a6257A77736FC78C367216cE7,
      0xf695Df6c0f3bB45918A7A82e83348FC59517734E, 0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D,
      0x903d5990119bC799423e9C25c56518Ba7DD19474, 0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068,
      0x4f33aCf823E6eEb697180d553cE0c710124C8D59, 0x99F70A0e1786402a6796c6B0AA997ef340a5c6da,
      0x3868D4e336d14D38031cf680329d31e4712e11cC, 0x0e389C83Bc1d16d86412476F6103027555C03265,
      0x0990b149e915cb08e2143a5c6f669c907eddc8b0, 0x272ea767712cc4839f4a27ee35eb73116158c8a2,
      0x1412632f2b89e87bfa20c1318a43ced25f1d7b76, 0xd879846cbe20751bde8a9342a3cca00a3e56ca47,
      0x35dfec1813c43d82e6b87c682f560bbb8ea0c121,
      0xcbade7d9bdee88411cb6cbcbb29952b742036992, 0x6f64f47f95cf656f21b40e14798f6b49f80b3dc5,
      0x0c709396739b9cfb72bcea6ac691ce0ddf66479c, 0x0bb754d8940e283d9ff6855ab5dafbc14165c059,
      0x5677a4dc7484762ffccee13cba20b5c979def446,
      0xc273986a91e4bfc543610a5cb5860b7cfefb6cc0, 0x4fe515c67eeeadb3282780325f09bb7c244fe774,
      0xbe023308ac2ef7e1c3799f4e6a3003ee6d342635, 0x2f6c0e5e06b43512706a9cdf66cd21f723fe0ec3,
      0xfe20ebe388149fb2e158b9d10cb95bcfa652262d,
      0x18b5c15e5196a38a162b1787875295b76e4313fb, 0x9de2b2dcdcf43540e47143f28484b6d15118f089,
      0x97e7962bcd091e7ecfb583fc96289b1e1553ac6e, 0xd9aa2300e126869182dfb6ecf54984e4c687f36b,
      0xef53e7d17822b641c6481837238a64a688709301
    )
),
evm_balances AS (
  SELECT user_address, product, SUM(net) AS balance
  FROM (
    -- Credits: tokens received
    SELECT CAST(receiver AS VARCHAR) AS user_address, product, amount AS net
    FROM evm_labeled
    WHERE receiver != 0x0000000000000000000000000000000000000000
    UNION ALL
    -- Debits: tokens sent
    SELECT CAST(sender AS VARCHAR) AS user_address, product, -amount AS net
    FROM evm_labeled
    WHERE sender != 0x0000000000000000000000000000000000000000
  ) t
  WHERE product IS NOT NULL
  GROUP BY user_address, product
  HAVING SUM(net) > 0.01
),
-- Starknet: reconstruct balances from Transfer events
starknet_labeled AS (
  SELECT
    keys[2] AS sender,
    keys[3] AS receiver,
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
    CAST(bytearray_to_uint256(data[1]) AS DOUBLE) / 1e5 AS amount  -- u256 low word, high always 0 for Spiko amounts
  FROM starknet.events
  WHERE keys[1] = 0x0099cd8bde557814842a3121e8ddfd433a539b8c9f14bf31ebf108d12e6196e9
    AND cardinality(keys) >= 3 AND cardinality(data) >= 1
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
),
sn_balances AS (
  SELECT user_address, product, SUM(net) AS balance
  FROM (
    SELECT CAST(receiver AS VARCHAR) AS user_address, product, amount AS net
    FROM starknet_labeled
    WHERE receiver != 0x0000000000000000000000000000000000000000000000000000000000000000
    UNION ALL
    SELECT CAST(sender AS VARCHAR) AS user_address, product, -amount AS net
    FROM starknet_labeled
    WHERE sender != 0x0000000000000000000000000000000000000000000000000000000000000000
  ) t
  WHERE product IS NOT NULL
  GROUP BY user_address, product
  HAVING SUM(net) > 0.01
),
-- Stellar: read current balances from contract state
sl_balances AS (
  SELECT
    json_extract_scalar(key_decoded, '$.vec[1].address') AS user_address,
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
    CAST(json_extract_scalar(val_decoded, '$.i128') AS DOUBLE) / 1e5 AS balance
  FROM stellar.contract_data
  WHERE contract_id IN (
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
    AND CAST(json_extract_scalar(val_decoded, '$.i128') AS DOUBLE) / 1e5 > 0.01
),
-- ============================================================
-- SAFO NAV oracle prices (Arbitrum, old format)
-- topic0 = 0x8c7e5cc...eddc6, price in data word 2 (bytes 33-64), 6 decimals
-- ============================================================
nav_prices AS (
    SELECT
        COALESCE(MAX(bytearray_to_uint256(bytearray_substring(data, 1+32, 32)))
            FILTER (WHERE contract_address = 0x372e37cA79747A2d1671EDBC5f1e2853B96BA351), 1000000) * 1e-6 AS p_safo_usd,
        COALESCE(MAX(bytearray_to_uint256(bytearray_substring(data, 1+32, 32)))
            FILTER (WHERE contract_address = 0x385D443ffA5b6Fb462b988D023a5DC3b37Ef1644), 1000000) * 1e-6 AS p_safo_eur,
        COALESCE(MAX(bytearray_to_uint256(bytearray_substring(data, 1+32, 32)))
            FILTER (WHERE contract_address = 0x835B48E97CBF727e23E7AA3bD40248818d20A2b0), 1000000) * 1e-6 AS p_safo_gbp,
        COALESCE(MAX(bytearray_to_uint256(bytearray_substring(data, 1+32, 32)))
            FILTER (WHERE contract_address = 0xD1F12049cC311DfB177f168046Ed8e2bd341a7AF), 1000000) * 1e-6 AS p_safo_chf
    FROM arbitrum.logs
    WHERE block_date >= current_date - INTERVAL '5' DAY
      AND topic0 = 0x8c7e5cc1d8e319b08a19c2d91194706fde5294e6181e0ab29669059f976eddc6
      AND contract_address IN (
          0x372e37cA79747A2d1671EDBC5f1e2853B96BA351,
          0x385D443ffA5b6Fb462b988D023a5DC3b37Ef1644,
          0x835B48E97CBF727e23E7AA3bD40248818d20A2b0,
          0xD1F12049cC311DfB177f168046Ed8e2bd341a7AF
      )
),
-- ============================================================
-- FX oracle rates (Arbitrum, Redstone format)
-- topic0 = 0x0559884...fc5f, price in topic1, 8 decimals
-- ============================================================
fx_prices AS (
    SELECT
        COALESCE(MAX(bytearray_to_int256(topic1))
            FILTER (WHERE contract_address = 0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871), 108000000) * 1e-8 AS eur_usd,
        COALESCE(MAX(bytearray_to_int256(topic1))
            FILTER (WHERE contract_address = 0x78f28D363533695458696b42577D2e1728cEa3D1), 127000000) * 1e-8 AS gbp_usd,
        1.13 AS chf_usd  -- no on-chain CHF/USD feed yet
    FROM arbitrum.logs
    WHERE block_date >= current_date - INTERVAL '5' DAY
      AND topic0 = 0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f
      AND contract_address IN (
          0x7AAeE6aD40a947A162DEAb5aFD0A1e12BE6FF871,
          0x78f28D363533695458696b42577D2e1728cEa3D1
      )
),
-- Combine all chains
all_balances AS (
  SELECT user_address, product, balance FROM evm_balances
  UNION ALL
  SELECT user_address, product, balance FROM sn_balances
  UNION ALL
  SELECT user_address, product, balance FROM sl_balances
),
-- Aggregate per user x product and convert to USD using oracle-derived NAV x FX
user_totals AS (
  SELECT
    a.user_address,
    a.product,
    SUM(a.balance) AS total_tokens,
    SUM(a.balance)
      * CASE a.product
          WHEN 'USTBL'    THEN 1.0  -- USD, NAV already ~1.08 but not tracked here
          WHEN 'SPKCC'    THEN 1.0  -- USD
          WHEN 'SAFO-USD' THEN np.p_safo_usd
          WHEN 'EUTBL'    THEN fx.eur_usd  -- NAV not included for simplicity
          WHEN 'eurSPKCC' THEN fx.eur_usd
          WHEN 'SAFO-EUR' THEN np.p_safo_eur * fx.eur_usd
          WHEN 'UKTBL'    THEN fx.gbp_usd
          WHEN 'SAFO-GBP' THEN np.p_safo_gbp * fx.gbp_usd
          WHEN 'SAFO-CHF' THEN np.p_safo_chf * fx.chf_usd
        END AS total_balance_usd
  FROM all_balances a
  CROSS JOIN nav_prices np
  CROSS JOIN fx_prices fx
  GROUP BY a.user_address, a.product, np.p_safo_usd, np.p_safo_eur, np.p_safo_gbp, np.p_safo_chf, fx.eur_usd, fx.gbp_usd, fx.chf_usd
  HAVING SUM(a.balance) > 0.01
),
bucketed AS (
  SELECT
    product,
    user_address,
    total_balance_usd,
    CASE
      WHEN total_balance_usd < 1000 THEN 'Micro (<1K)'
      WHEN total_balance_usd < 10000 THEN 'Small (1K-10K)'
      WHEN total_balance_usd < 100000 THEN 'Mid (10K-100K)'
      WHEN total_balance_usd < 1000000 THEN 'Large (100K-1M)'
      ELSE 'Whale (>1M)'
    END AS bucket
  FROM user_totals
),
bucket_counts AS (
  SELECT
    product,
    bucket,
    COUNT(DISTINCT user_address) AS holder_count
  FROM bucketed
  GROUP BY product, bucket
),
total_per_product AS (
  SELECT product, SUM(holder_count) AS total_holders
  FROM bucket_counts
  GROUP BY product
)
SELECT
  bc.product,
  bc.bucket,
  bc.holder_count,
  ROUND(100.0 * bc.holder_count / tp.total_holders, 2) AS percentage,
  CURRENT_DATE AS snapshot_date
FROM bucket_counts bc
JOIN total_per_product tp ON bc.product = tp.product
ORDER BY bc.product,
  CASE
    WHEN bc.bucket = 'Micro (<1K)' THEN 1
    WHEN bc.bucket = 'Small (1K-10K)' THEN 2
    WHEN bc.bucket = 'Mid (10K-100K)' THEN 3
    WHEN bc.bucket = 'Large (100K-1M)' THEN 4
    WHEN bc.bucket = 'Whale (>1M)' THEN 5
  END;