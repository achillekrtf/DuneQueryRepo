-- part of a query repo
-- query name: Active Wallet 30d
-- query link: https://dune.com/queries/6706994


-- ============================================================
-- Active Wallets — 30d Rolling — INCREMENTAL VERSION
-- Dune Materialized View | DuneSQL
-- Rescans last 30 days to correctly compute rolling window
-- ============================================================

WITH
-- ==========================================================
-- 0. PREVIOUS RESULTS
-- ==========================================================
prev AS (
    SELECT * FROM TABLE(previous.query.result(
        schema => DESCRIPTOR(
            day                  TIMESTAMP(3),
            product              VARCHAR,
            active_wallets_30d   BIGINT,
            daily_active_wallets BIGINT,
            new_wallets          BIGINT
        )
    ))
),

-- ==========================================================
-- 1. CUTOFF
-- ==========================================================
checkpoint AS (
    SELECT
        COALESCE(MAX(day), TIMESTAMP '2024-04-30 00:00:00.000') - INTERVAL '30' DAY  AS output_cutoff,
        CAST(COALESCE(MAX(day), TIMESTAMP '2024-04-30 00:00:00.000') - INTERVAL '30' DAY AS DATE)      AS output_cutoff_date,
        CAST(COALESCE(MAX(day), TIMESTAMP '2024-04-30 00:00:00.000') - INTERVAL '30' DAY AS TIMESTAMP) AS output_cutoff_ts,
        COALESCE(MAX(day), TIMESTAMP '2024-04-30 00:00:00.000') - INTERVAL '60' DAY  AS scan_cutoff,
        CAST(COALESCE(MAX(day), TIMESTAMP '2024-04-30 00:00:00.000') - INTERVAL '60' DAY AS DATE)      AS scan_cutoff_date,
        CAST(COALESCE(MAX(day), TIMESTAMP '2024-04-30 00:00:00.000') - INTERVAL '60' DAY AS TIMESTAMP) AS scan_cutoff_ts
    FROM prev
),

-- ==========================================================
-- 2. EVM ACTIVITY — from scan_cutoff (60d back)
-- ==========================================================
evm_activity AS (
    SELECT
        DATE_TRUNC('day', evt_block_time) AS day,
        CAST(wallet AS VARCHAR) AS wallet,
        product
    FROM (
        SELECT evt_block_time, blockchain, contract_address, "from" AS wallet
        FROM evms.erc20_transfers
        WHERE evt_block_time >= (SELECT scan_cutoff_ts FROM checkpoint)
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
          AND "from" != 0x0000000000000000000000000000000000000000
        UNION ALL
        SELECT evt_block_time, blockchain, contract_address, "to" AS wallet
        FROM evms.erc20_transfers
        WHERE evt_block_time >= (SELECT scan_cutoff_ts FROM checkpoint)
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
          AND "to" != 0x0000000000000000000000000000000000000000
    ) t
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN contract_address = 0xe4880249745eAc5F1eD9d8F7DF844792D560e750
                 AND blockchain IN ('ethereum','polygon','base','etherlink')  THEN 'USTBL'
            WHEN contract_address = 0x021289588cd81dC1AC87ea91e91607eEF68303F5
                 AND blockchain = 'arbitrum'                                   THEN 'USTBL'
            WHEN contract_address = 0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80
                 AND blockchain IN ('ethereum','polygon','base','etherlink')  THEN 'EUTBL'
            WHEN contract_address = 0xCBeb19549054CC0a6257A77736FC78C367216cE7
                 AND blockchain = 'arbitrum'                                   THEN 'EUTBL'
            WHEN contract_address = 0xf695Df6c0f3bB45918A7A82e83348FC59517734E
                 AND blockchain = 'ethereum'                                   THEN 'UKTBL'
            WHEN contract_address = 0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D
                 AND blockchain IN ('polygon','etherlink')                    THEN 'UKTBL'
            WHEN contract_address = 0x903d5990119bC799423e9C25c56518Ba7DD19474
                 AND blockchain = 'arbitrum'                                   THEN 'UKTBL'
            WHEN contract_address = 0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068
                 AND blockchain = 'base'                                       THEN 'UKTBL'
            WHEN contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59
                 AND blockchain IN ('ethereum','etherlink')                   THEN 'SPKCC'
            WHEN contract_address = 0x903d5990119bC799423e9C25c56518Ba7DD19474
                 AND blockchain = 'polygon'                                    THEN 'SPKCC'
            WHEN contract_address = 0x99F70A0e1786402a6796c6B0AA997ef340a5c6da
                 AND blockchain = 'arbitrum'                                   THEN 'SPKCC'
            WHEN contract_address = 0xf695Df6c0f3bB45918A7A82e83348FC59517734E
                 AND blockchain = 'base'                                       THEN 'SPKCC'
            WHEN contract_address = 0x3868D4e336d14D38031cf680329d31e4712e11cC
                 AND blockchain IN ('ethereum','etherlink')                   THEN 'eurSPKCC'
            WHEN contract_address = 0x99F70A0e1786402a6796c6B0AA997ef340a5c6da
                 AND blockchain = 'polygon'                                    THEN 'eurSPKCC'
            WHEN contract_address = 0x0e389C83Bc1d16d86412476F6103027555C03265
                 AND blockchain = 'arbitrum'                                   THEN 'eurSPKCC'
            WHEN contract_address = 0x4f33aCf823E6eEb697180d553cE0c710124C8D59
                 AND blockchain = 'base'                                       THEN 'eurSPKCC'
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
        END AS product
    ) p
    WHERE product IS NOT NULL
),

-- ==========================================================
-- 3. STARKNET ACTIVITY
-- ==========================================================
starknet_activity AS (
    SELECT DATE_TRUNC('day', block_date) AS day,
           CAST(keys[2] AS VARCHAR) AS wallet,
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
           END AS product
    FROM starknet.events
    WHERE block_date >= (SELECT scan_cutoff_date FROM checkpoint)
      AND cardinality(keys) >= 3
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
      AND keys[2] != 0x0000000000000000000000000000000000000000000000000000000000000000
    UNION ALL
    SELECT DATE_TRUNC('day', block_date) AS day,
           CAST(keys[3] AS VARCHAR) AS wallet,
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
           END AS product
    FROM starknet.events
    WHERE block_date >= (SELECT scan_cutoff_date FROM checkpoint)
      AND cardinality(keys) >= 3
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
      AND keys[3] != 0x0000000000000000000000000000000000000000000000000000000000000000
),

-- ==========================================================
-- 4. STELLAR ACTIVITY
-- ==========================================================
stellar_activity AS (
    SELECT
        DATE_TRUNC('day', closed_at) AS day,
        json_extract_scalar(key_decoded, '$.vec[1].address') AS wallet,
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
        END AS product
    FROM stellar.contract_data
    WHERE closed_at_date >= (SELECT scan_cutoff_date FROM checkpoint)
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

-- ==========================================================
-- 5. COMBINED ACTIVITY
-- ==========================================================
new_activity AS (
    SELECT DISTINCT day, wallet, product
    FROM (
        SELECT day, CAST(wallet AS VARCHAR) AS wallet, product FROM evm_activity
        UNION ALL
        SELECT day, CAST(wallet AS VARCHAR) AS wallet, product FROM starknet_activity
        UNION ALL
        SELECT day, wallet, product FROM stellar_activity WHERE product IS NOT NULL
    ) t
),

-- ==========================================================
-- 6. CALENDAR
-- ==========================================================
calendar AS (
    SELECT date_trunc('day', period) AS day
    FROM checkpoint
    CROSS JOIN UNNEST(sequence(
        output_cutoff_date, current_date, INTERVAL '1' DAY
    )) AS t(period)
),

-- ==========================================================
-- 7. FIRST SEEN
-- ==========================================================
first_seen_new AS (
    SELECT wallet, product, MIN(day) AS first_day
    FROM new_activity
    GROUP BY 1, 2
),

-- ==========================================================
-- 8. DAILY STATS
-- ==========================================================
daily_stats_new AS (
    SELECT
        a.day, a.product,
        COUNT(DISTINCT a.wallet) AS daily_active_wallets,
        COUNT(DISTINCT CASE WHEN f.first_day = a.day THEN a.wallet END) AS new_wallets
    FROM new_activity a
    LEFT JOIN first_seen_new f ON a.wallet = f.wallet AND a.product = f.product
    GROUP BY 1, 2
),

-- ==========================================================
-- 9. ROLLING 30d
-- ==========================================================
rolling_new AS (
    SELECT c.day, p.product,
        COUNT(DISTINCT a.wallet) AS active_wallets_30d
    FROM calendar c
    CROSS JOIN (SELECT DISTINCT product FROM new_activity) p
    LEFT JOIN new_activity a
        ON a.product = p.product
        AND a.day BETWEEN c.day - INTERVAL '29' DAY AND c.day
    GROUP BY 1, 2
),

-- ==========================================================
-- 10. NEW ROWS
-- ==========================================================
new_rows AS (
    SELECT r.day, r.product, r.active_wallets_30d,
        COALESCE(d.daily_active_wallets, 0) AS daily_active_wallets,
        COALESCE(d.new_wallets, 0) AS new_wallets
    FROM rolling_new r
    LEFT JOIN daily_stats_new d ON r.day = d.day AND r.product = d.product
    WHERE r.active_wallets_30d > 0
)

-- ==========================================================
-- 11. OUTPUT
-- ==========================================================
SELECT day, product, active_wallets_30d, daily_active_wallets, new_wallets
FROM prev
WHERE day < (SELECT output_cutoff FROM checkpoint)
UNION ALL
SELECT day, product, active_wallets_30d, daily_active_wallets, new_wallets
FROM new_rows
ORDER BY day DESC, product