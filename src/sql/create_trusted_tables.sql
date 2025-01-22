CREATE OR REPLACE TABLE `ps-eng-dados-ds3x.sergio_maxpayne.icc_trusted` AS
SELECT
    ano_mes,
    indice,
    variacao_mes,
    variacao_ano,
    load_timestamp
FROM `ps-eng-dados-ds3x.sergio_maxpayne.icc_raw`
WHERE indice IS NOT NULL       -- Exclui índices nulos
GROUP BY ano_mes, indice, variacao_mes, variacao_ano, load_timestamp;

CREATE OR REPLACE TABLE `ps-eng-dados-ds3x.sergio_maxpayne.icf_trusted` AS
SELECT
    ano_mes,
    indice,
    variacao_mes,
    variacao_ano,
    load_timestamp
FROM `ps-eng-dados-ds3x.sergio_maxpayne.icf_raw`
WHERE indice IS NOT NULL       -- Exclui índices nulos
GROUP BY ano_mes, indice, variacao_mes, variacao_ano, load_timestamp;
