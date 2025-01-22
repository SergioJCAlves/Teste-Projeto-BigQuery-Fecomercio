# Fecomércio Data Pipeline

Este projeto implementa um pipeline de dados para coletar e processar índices econômicos da Fecomércio (ICC e ICF).

## Estrutura do Projeto

```
project/
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── scraper.py
│   ├── loader.py
│   └── sql/
│       ├── create_trusted_tables.sql
│       └── create_refined_table.sql
├── requirements.txt
├── SA-maxpayne.json
└── README.md
```

## Configuração

1. Instale as dependências:

```bash
pip install -r requirements.txt
```

2. Configure as credenciais:

- Coloque o arquivo `SA-maxpayne.json` na raiz do projeto

### Local

```bash
python src/main.py
```

### Tabelas Geradas

### Raw Tables

- `icc_raw`: Dados brutos do ICC
- `icf_raw`: Dados brutos do ICF

### Trusted Tables

- `icc_trusted`: Dados limpos e organizados do ICC
- `icf_trusted`: Dados limpos e organizados do ICF

### Refined Table

- `icf_icc_refined`: Dados combinados e processados

## Regras de Transformação

### Trusted Tables

- Remoção de duplicatas
- Padronização de datas
- Organização de métricas

### Refined Table

- Join entre ICC e ICF por ano/mês
- Cálculo de métricas consolidadas
- Adição de timestamps de processamento

## Decisões Técnicas

1. **Web Scraping**:

   - Uso do Selenium em modo headless
   - Tratamento robusto de erros
   - Sistema de retry para downloads
2. **BigQuery**:

   - Uso de credenciais via service account
   - Implementação de logging detalhado
   - Controle de duplicatas via ROW_NUMBER()

## Contribuindo

1. Faça um fork do projeto
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Push para a branch
5. Abra um Pull Request
