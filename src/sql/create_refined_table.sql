CREATE OR REPLACE TABLE `ps-eng-dados-ds3x.sergio_maxpayne.icf_icc_refined` AS
SELECT
    icc.ano_mes,
    icc.indice AS icc_indice,
    icc.variacao_mes AS icc_variacao_mes,
    icc.variacao_ano AS icc_variacao_ano,
    icf.indice AS icf_indice,
    icf.variacao_mes AS icf_variacao_mes,
    icf.variacao_ano AS icf_variacao_ano,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM `ps-eng-dados-ds3x.sergio_maxpayne.icc_trusted` icc
JOIN `ps-eng-dados-ds3x.sergio_maxpayne.icf_trusted` icf
    ON icc.ano_mes = icf.ano_mes;