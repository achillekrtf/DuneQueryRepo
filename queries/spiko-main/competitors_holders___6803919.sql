-- part of a query repo
-- query name: Competitors Holders
-- query link: https://dune.com/queries/6803919


-- ============================================================
-- Competitive Unique Wallet Count — Optimized
-- ============================================================

WITH fund_map AS (
  SELECT * FROM (VALUES
    -- Spiko (EVM)
    (0xe4880249745eAc5F1eD9d8F7DF844792D560e750,'ethereum','Spiko'),(0xe4880249745eAc5F1eD9d8F7DF844792D560e750,'polygon','Spiko'),(0xe4880249745eAc5F1eD9d8F7DF844792D560e750,'base','Spiko'),(0x021289588cd81dC1AC87ea91e91607eEF68303F5,'arbitrum','Spiko'),
    (0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80,'ethereum','Spiko'),(0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80,'polygon','Spiko'),(0xa0769f7A8fC65e47dE93797b4e21C073c117Fc80,'base','Spiko'),(0xCBeb19549054CC0a6257A77736FC78C367216cE7,'arbitrum','Spiko'),
    (0xf695Df6c0f3bB45918A7A82e83348FC59517734E,'ethereum','Spiko'),(0x970E2aDC2fdF53AEa6B5fa73ca6dc30eAFEDfe3D,'polygon','Spiko'),(0x903d5990119bC799423e9C25c56518Ba7DD19474,'arbitrum','Spiko'),(0xA8De1f55Aa0E381cb456e1DcC9ff781eA0079068,'base','Spiko'),
    (0x4f33aCf823E6eEb697180d553cE0c710124C8D59,'ethereum','Spiko'),(0x903d5990119bC799423e9C25c56518Ba7DD19474,'polygon','Spiko'),(0x99F70A0e1786402a6796c6B0AA997ef340a5c6da,'arbitrum','Spiko'),(0xf695Df6c0f3bB45918A7A82e83348FC59517734E,'base','Spiko'),
    (0x3868D4e336d14D38031cf680329d31e4712e11cC,'ethereum','Spiko'),(0x99F70A0e1786402a6796c6B0AA997ef340a5c6da,'polygon','Spiko'),(0x0e389C83Bc1d16d86412476F6103027555C03265,'arbitrum','Spiko'),(0x4f33aCf823E6eEb697180d553cE0c710124C8D59,'base','Spiko'),
    -- SAFO-USD (Spiko)
    (0xcbade7d9bdee88411cb6cbcbb29952b742036992,'ethereum','Spiko'),(0x6f64f47f95cf656f21b40e14798f6b49f80b3dc5,'polygon','Spiko'),(0x0c709396739b9cfb72bcea6ac691ce0ddf66479c,'arbitrum','Spiko'),(0x0bb754d8940e283d9ff6855ab5dafbc14165c059,'base','Spiko'),(0x5677a4dc7484762ffccee13cba20b5c979def446,'etherlink','Spiko'),
    -- SAFO-EUR (Spiko)
    (0x0990b149e915cb08e2143a5c6f669c907eddc8b0,'ethereum','Spiko'),(0x272ea767712cc4839f4a27ee35eb73116158c8a2,'polygon','Spiko'),(0x1412632f2b89e87bfa20c1318a43ced25f1d7b76,'arbitrum','Spiko'),(0xd879846cbe20751bde8a9342a3cca00a3e56ca47,'base','Spiko'),(0x35dfec1813c43d82e6b87c682f560bbb8ea0c121,'etherlink','Spiko'),
    -- SAFO-GBP (Spiko)
    (0xc273986a91e4bfc543610a5cb5860b7cfefb6cc0,'ethereum','Spiko'),(0x4fe515c67eeeadb3282780325f09bb7c244fe774,'polygon','Spiko'),(0xbe023308ac2ef7e1c3799f4e6a3003ee6d342635,'arbitrum','Spiko'),(0x2f6c0e5e06b43512706a9cdf66cd21f723fe0ec3,'base','Spiko'),(0xfe20ebe388149fb2e158b9d10cb95bcfa652262d,'etherlink','Spiko'),
    -- SAFO-CHF (Spiko)
    (0x18b5c15e5196a38a162b1787875295b76e4313fb,'ethereum','Spiko'),(0x9de2b2dcdcf43540e47143f28484b6d15118f089,'polygon','Spiko'),(0x97e7962bcd091e7ecfb583fc96289b1e1553ac6e,'arbitrum','Spiko'),(0xd9aa2300e126869182dfb6ecf54984e4c687f36b,'base','Spiko'),(0xef53e7d17822b641c6481837238a64a688709301,'etherlink','Spiko'),
    -- BUIDL
    (0x7712c34205737192402172409a8f7ccef8aa2aec,'ethereum','BUIDL'),(0x6a9da2d710bb9b700acde7cb81f10f1ff8c89041,'ethereum','BUIDL'),(0xa6525ae43edcd03dc08e775774dcabd3bb925872,'arbitrum','BUIDL'),(0x53fc82f14f009009b440a706e31c9021e1196a2f,'avalanche_c','BUIDL'),(0x2d5bdc96d9c8aabbdb38c9a27398513e7e5ef84f,'bnb','BUIDL'),(0xa1cdab15bba75a80df4089cafba013e376957cf5,'optimism','BUIDL'),(0x2893ef551b6dd69f661ac00f11d93e5dc5dc0e99,'polygon','BUIDL'),
    -- BENJI
    (0x3ddc84940ab509c11b20b76b466933f40b750dc9,'ethereum','BENJI'),(0xb9e4765bce2609bc1949592059b17ea72fee6c6a,'arbitrum','BENJI'),(0xe08b4c1005603427420e64252a8b120cace4d122,'avalanche_c','BENJI'),(0x60cfc2b186a4cf647486e42c42b11cc6d571d1e4,'base','BENJI'),(0x408a634b8a8f0de729b48574a3a7ec3fe820b00a,'polygon','BENJI'),
    -- OUSG
    (0x1B19C19393e2d034D8Ff31ff34c81252FcBbee92,'ethereum','OUSG'),
    -- USYC
    (0x136471a34f6ef19fe571effc1ca711fdb8e49f2b,'ethereum','USYC'),(0x8d0fa28f221eb5735bc71d3a0da67ee5bc821311,'bnb','USYC'),
    -- USTB
    (0x43415eb6ff9db7e26a15b704e7a3edce97d31c4e,'ethereum','USTB'),(0xe4fa682f94610ccd170680cc3b045d77d9e528a8,'plume','USTB'),
    -- USCC
    (0x14d60e7fdc0d71d8611742720e4c50e7a974020c,'ethereum','USCC'),(0x4c21B7577C8FE8b0B0669165ee7C8f67fa1454Cf,'plume','USCC'),
    -- WTGXX
    (0x1fecf3d9d4fee7f2c02917a66028a48c6706c179,'ethereum','WTGXX'),(0xfeb26f0943c3885b2cb85a9f933975356c81c33d,'arbitrum','WTGXX'),(0x870fd36b3bf7f5abeeea2c8d4abdf1dc4e33109d,'optimism','WTGXX'),(0x870fd36b3bf7f5abeeea2c8d4abdf1dc4e33109d,'avalanche_c','WTGXX'),(0xcf7a8813bd3bdaf70a9f46d310ce1ee8d80a4f5a,'plume','WTGXX'),
    -- JTRSY
    (0x8c213ee79581ff4984583c6a801e5263418c4b86,'ethereum','JTRSY'),(0x8c213ee79581ff4984583c6a801e5263418c4b86,'base','JTRSY'),(0x27e8c820d05aea8824b1ac35116f63f9833b54c8,'celo','JTRSY'),
    -- FDIT
    (0x48ab4e39ac59f4e88974804b04a991b3a402717f,'ethereum','FDIT')
  ) AS t(contract_address, blockchain, fund)
),

-- EVM: partition filter on evt_block_date + trimmed blockchain list
evm AS (
  SELECT DISTINCT fm.fund, CAST(e."to" AS VARCHAR) AS w
  FROM evms.erc20_transfers e
  JOIN fund_map fm
    ON e.contract_address = fm.contract_address
   AND e.blockchain = fm.blockchain
  WHERE e.evt_block_date >= DATE '2024-01-01'
    AND e.evt_block_time >= TIMESTAMP '2024-01-01'
    AND e.blockchain IN ('ethereum','polygon','arbitrum','base','optimism','avalanche_c','bnb','celo','plume','etherlink')
    AND e.value > UINT256 '0'
    AND e."to" NOT IN (
        0x0000000000000000000000000000000000000000,
        0xda5599f04e9b437c8394b0c2bc68b502a66ebfe8,
        0x15ea0ec460a0e6847ec0aa8d50a84b3a51b95f74
    )
),

-- Starknet: partition filter + corrected topic
stark AS (
  SELECT DISTINCT 'Spiko' AS fund, CAST(keys[3] AS VARCHAR) AS w
  FROM starknet.events
  WHERE block_date >= DATE '2024-01-01'
    AND block_time >= TIMESTAMP '2024-01-01'
    AND cardinality(keys) >= 3
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
    AND keys[3] != 0x0000000000000000000000000000000000000000000000000000000000000000
),

-- Stellar Spiko: partition filter on closed_at_date (already present)
stellar AS (
  SELECT DISTINCT 'Spiko' AS fund, json_extract_scalar(key_decoded,'$.vec[1].address') AS w
  FROM stellar.contract_data
  WHERE closed_at_date >= DATE '2024-01-01'
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
    AND deleted = false
    AND contract_key_type = 'ScValTypeScvVec'
    AND json_extract_scalar(key_decoded,'$.vec[0].symbol') = 'Balance'
    AND TRY_CAST(json_extract_scalar(val_decoded,'$.i128') AS DOUBLE) > 0
    AND json_extract_scalar(key_decoded,'$.vec[1].address') != 'CAVZK26ERVGKGXLQGEOKPAQGIZS2YKN4BSXOYDMUY365EV66ZNSFEDBS'
),

-- Stellar competitors: partition filter
stellar_comp AS (
  SELECT DISTINCT
    CASE
        WHEN asset_code IN ('BENJI','gBENJI','sgBENJI') THEN 'BENJI'
        WHEN asset_code = 'WTGX' THEN 'WTGXX'
    END AS fund,
    "to" AS w
  FROM stellar.history_operations
  WHERE closed_at >= TIMESTAMP '2024-01-01'
    AND type_string = 'payment'
    AND (
        (asset_code = 'BENJI'   AND asset_issuer = 'GBHNGLLIE3KWGKCHIKMHJ5HVZHYIK7WTBE4QF5PLAKL4CJGSEU7HZIW5')
     OR (asset_code = 'gBENJI'  AND asset_issuer = 'GD5J73EKK5IYL5XS3FBTHHX7CZIYRP7QXDL57XFWGC2WVYWT326OBXRP')
     OR (asset_code = 'sgBENJI' AND asset_issuer = 'GAGICV3VBJSKKH5H5MQQIUTUP462YVHC23KUHZY6FJERRJFBDIVZBM5C')
     OR (asset_code = 'WTGX'    AND asset_issuer = 'GDMBNMFJ3TRFLASJ6UGETFME3PJPNKPU24C7KFDBEBPQFG2CI6UC3JG6')
    )
    AND "to" != asset_issuer
),

-- Solana: partition filter
sol AS (
  SELECT DISTINCT
    CASE token_mint_address
      WHEN 'GyWgeqpy5GueU2YbkE8xqUeVEokCMMCEeUrfbtMw6phr' THEN 'BUIDL'
      WHEN 'i7u4r16TcsJTgq1kAG8opmVZyVnAKBwLKu6ZPMwzxNc'  THEN 'OUSG'
      WHEN '7LWanZteUKtvFjv4MHYgKXXdAuCQYFPJysL9pxxdRQGn'  THEN 'USYC'
      WHEN '5Tu84fKBpe9vfXeotjvfvWdWbAjy3hqsExvuHgFqFxA1'  THEN 'BENJI'
      WHEN 'Em46fxxwgY2RRoUbBMSbEjJwY62x3ESMNdhnsGpEKewm'  THEN 'WTGXX'
      WHEN 'CCz3SGVziFeLYk2xfEstkiqJfYkjaSWb2GCABYsVcjo2'  THEN 'USTB'
      WHEN 'BTRR3sj1Bn2ZjuemgbeQ6SCtf84iXS81CS7UDTSxUCaK'  THEN 'USCC'
    END AS fund,
    to_owner AS w
  FROM tokens_solana.transfers
  WHERE block_date >= DATE '2024-01-01'
    AND block_time >= TIMESTAMP '2024-01-01'
    AND token_mint_address IN (
        'GyWgeqpy5GueU2YbkE8xqUeVEokCMMCEeUrfbtMw6phr',
        'i7u4r16TcsJTgq1kAG8opmVZyVnAKBwLKu6ZPMwzxNc',
        '7LWanZteUKtvFjv4MHYgKXXdAuCQYFPJysL9pxxdRQGn',
        '5Tu84fKBpe9vfXeotjvfvWdWbAjy3hqsExvuHgFqFxA1',
        'Em46fxxwgY2RRoUbBMSbEjJwY62x3ESMNdhnsGpEKewm',
        'CCz3SGVziFeLYk2xfEstkiqJfYkjaSWb2GCABYsVcjo2',
        'BTRR3sj1Bn2ZjuemgbeQ6SCtf84iXS81CS7UDTSxUCaK'
    )
    AND to_owner IS NOT NULL
    AND amount > 0
),

-- Aptos: partition filter
aptos AS (
  SELECT DISTINCT 'BUIDL' AS fund, JSON_EXTRACT_SCALAR(data, '$.to') AS w
  FROM aptos.events
  WHERE block_date >= DATE '2024-01-01'
    AND block_time >= TIMESTAMP '2024-01-01'
    AND event_type = '0x4de5876d8a8e2be7af6af9f3ca94d9e4fafb24b5f4a5848078d8eb08f08e808a::ds_token::Transfer'
    AND JSON_EXTRACT_SCALAR(data, '$.to') IS NOT NULL
),

all_w AS (
  SELECT fund, w FROM evm
  UNION SELECT fund, w FROM stark
  UNION SELECT fund, w FROM stellar
  UNION SELECT fund, w FROM stellar_comp WHERE fund IS NOT NULL
  UNION SELECT fund, w FROM sol          WHERE fund IS NOT NULL
  UNION SELECT fund, w FROM aptos
)

SELECT
  COUNT(*) FILTER (WHERE fund = 'Spiko') AS "Spiko",
  COUNT(*) FILTER (WHERE fund = 'BUIDL') AS "BUIDL",
  COUNT(*) FILTER (WHERE fund = 'BENJI') AS "BENJI",
  COUNT(*) FILTER (WHERE fund = 'OUSG')  AS "OUSG",
  COUNT(*) FILTER (WHERE fund = 'USYC')  AS "USYC",
  COUNT(*) FILTER (WHERE fund = 'USTB')  AS "USTB",
  COUNT(*) FILTER (WHERE fund = 'USCC')  AS "USCC",
  COUNT(*) FILTER (WHERE fund = 'WTGXX') AS "WTGXX",
  COUNT(*) FILTER (WHERE fund = 'JTRSY') AS "JTRSY",
  COUNT(*) FILTER (WHERE fund = 'FDIT')  AS "FDIT"
FROM all_w