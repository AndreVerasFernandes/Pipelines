# Chicago DMV ETL Pipeline

Pipeline ETL em Python para processar dados de acidentes de transito de Chicago a partir de arquivos CSV locais e carregar os resultados em tabelas PostgreSQL (Neon).

## Visao Geral

O pipeline executa 3 etapas:

1. Extract: leitura dos CSVs com padronizacao dos nomes de colunas para caixa alta.
2. Transform: limpeza de dados (remocao de duplicados, tratamento de nulos e conversoes de tipos quando aplicavel).
3. Load: insercao em massa nas tabelas de destino no schema `chicago_dmv`.

Arquivo principal de execucao: `pipeline.py`.

## Estrutura

```text
chicago_dmv/
  config.yaml
  pipeline.py
  requirements.txt
  .env
  data/
    traffic_crashes.csv
    traffic_crash_vehicle.csv
    traffic_crash_people.csv
  etl/
    extract.py
    transform.py
    load.py
```

## Requisitos

- Python 3.10+
- Acesso a um banco PostgreSQL (Neon ou compativel)

Dependencias Python:

- pandas
- PyYAML
- psycopg[binary]

## Configuracao

1. Crie e ative um ambiente virtual.
2. Instale as dependencias:

```bash
pip install -r requirements.txt
```

3. Configure a variavel `NEON_DATABASE_URL`.

Opcao A: exportar no shell

```bash
export NEON_DATABASE_URL="postgresql://USER:PASSWORD@HOST/DB?sslmode=require"
```

Opcao B: manter no arquivo `.env` na raiz de `chicago_dmv/`.

O carregamento no modulo `etl/load.py` tenta ler da variavel de ambiente e, se nao encontrar, faz fallback para `.env`.

## Configuracao dos Arquivos e Tabelas

As entradas e tabelas de destino estao em `config.yaml`:

- `crash_filepath`: caminho do CSV de acidentes
- `vehicle_filepath`: caminho do CSV de veiculos
- `people_filepath`: caminho do CSV de pessoas
- `crash_table_PSQL`: tabela de destino para acidentes
- `vehicle_table_PSQL`: tabela de destino para veiculos
- `person_table_PSQL`: tabela de destino para pessoas

Por padrao:

- `data/traffic_crashes.csv` -> `chicago_dmv.crash`
- `data/traffic_crash_vehicle.csv` -> `chicago_dmv.vehicle`
- `data/traffic_crash_people.csv` -> `chicago_dmv.person`

## Como Executar

Na pasta `chicago_dmv/`, rode:

```bash
python pipeline.py
```

Se tudo estiver correto, o script exibira mensagens de sucesso com quantidade de registros inseridos por tabela.

## Mapeamento de Colunas no Load

O modulo `etl/load.py` alinha as colunas do DataFrame para o schema esperado no banco.

Tabela `crash` espera:

- CRASH_UNIT_ID
- CRASH_ID
- PERSON_ID
- VEHICLE_ID
- NUM_UNITS
- TOTAL_INJURIES

Tabela `vehicle` espera:

- CRASH_UNIT_ID
- CRASH_ID
- CRASH_DATE
- VEHICLE_ID
- VEHICLE_MAKE
- VEHICLE_MODEL
- VEHICLE_YEAR
- VEHICLE_TYPE

Tabela `person` espera:

- PERSON_ID
- CRASH_ID
- CRASH_DATE
- PERSON_TYPE
- VEHICLE_ID
- PERSON_SEX
- PERSON_AGE

Aliases de origem suportados automaticamente:

- `CRASH_RECORD_ID` -> `CRASH_ID`
- `INJURIES_TOTAL` -> `TOTAL_INJURIES`
- `MAKE` -> `VEHICLE_MAKE`
- `MODEL` -> `VEHICLE_MODEL`
- `UNIT_TYPE` -> `VEHICLE_TYPE`
- `AGE` -> `PERSON_AGE`

## Solucao de Problemas

- Erro `NEON_DATABASE_URL is not set`:
  - Verifique se a variavel foi exportada no shell atual ou se o `.env` esta presente em `chicago_dmv/.env`.
- Erro de tabela invalida:
  - Confirme os nomes em `config.yaml` no formato `schema.tabela`.
- Erro de arquivo nao encontrado:
  - Valide os caminhos dos CSVs em `config.yaml` e execute o comando a partir da pasta `chicago_dmv/`.

## Observacoes

- O pipeline nao cria tabelas automaticamente; elas devem existir previamente no banco.
- Registros duplicados sao removidos na etapa de transformacao.
- Valores nulos sao preenchidos com media (numericos) e moda (categoricos), quando possivel.
