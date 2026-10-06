# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop RH — instalação da base de dados
# MAGIC
# MAGIC Cria no Unity Catalog a base **100% sintética/fictícia** de People Analytics usada no workshop
# MAGIC (Genie One, Agents e Dashboards via Genie Code):
# MAGIC
# MAGIC - **13 tabelas Delta** com descrições em português, chaves primárias e estrangeiras
# MAGIC - **Volume `politicas_rh`** com 8 políticas de RH fictícias em PDF
# MAGIC
# MAGIC **Como usar:** escolha o catálogo e o schema nos campos acima e clique em **Run all**.
# MAGIC Pode rodar em serverless ou em qualquer cluster com Unity Catalog. É idempotente: rodar de novo recria tudo.
# MAGIC
# MAGIC **Permissões necessárias:** `USE CATALOG` + `CREATE SCHEMA` no catálogo (ou `CREATE TABLE` + `CREATE VOLUME` num schema existente).

# COMMAND ----------

dbutils.widgets.text("catalogo", "workshop", "Catálogo")
dbutils.widgets.text("schema", "embracon_rh", "Schema")
CAT = dbutils.widgets.get("catalogo").strip()
SCH = dbutils.widgets.get("schema").strip()
S = f"`{CAT}`.`{SCH}`"
print(f"Destino: {CAT}.{SCH}")

# COMMAND ----------

import json
import os
import shutil

# pasta deste notebook (Git folder ou pasta importada no workspace)
caminho_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
BASE = "/Workspace" + os.path.dirname(caminho_nb)
DADOS, POLITICAS = f"{BASE}/dados", f"{BASE}/politicas_rh"
assert os.path.exists(f"{DADOS}/metadados.json"), f"Não encontrei {DADOS}/metadados.json — o notebook precisa estar na mesma pasta das pastas 'dados' e 'politicas_rh'."
META = json.load(open(f"{DADOS}/metadados.json"))
print("Origem:", BASE)

# COMMAND ----------

# MAGIC %md ## 1. Schema e volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {S} COMMENT 'Workshop RH (Genie One, Agents e Dashboards) — dados 100% sintéticos/fictícios'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {S}.raw COMMENT 'Parquets de carga do workshop'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {S}.politicas_rh COMMENT 'Políticas de RH fictícias (PDF) para Genie Agent mode / Knowledge Assistant'")

VOL_RAW = f"/Volumes/{CAT}/{SCH}/raw"
VOL_POL = f"/Volumes/{CAT}/{SCH}/politicas_rh"
for f in os.listdir(DADOS):
    if f.endswith(".parquet"):
        shutil.copy(f"{DADOS}/{f}", f"{VOL_RAW}/{f}")
for f in os.listdir(POLITICAS):
    if f.endswith(".pdf"):
        shutil.copy(f"{POLITICAS}/{f}", f"{VOL_POL}/{f}")
print("Parquets:", sorted(os.listdir(VOL_RAW)))
print("PDFs:", sorted(os.listdir(VOL_POL)))

# COMMAND ----------

# MAGIC %md ## 2. Tabelas, comentários e chaves

# COMMAND ----------

existentes = {r.tableName for r in spark.sql(f"SHOW TABLES IN {S}").collect()}

# recarga idempotente: remove chaves antigas antes de recriar
for t, c, _, _ in META["fk"]:
    if t in existentes:
        spark.sql(f"ALTER TABLE {S}.{t} DROP CONSTRAINT IF EXISTS fk_{t}_{c}")
for t in list(META["pk"]) + ["avaliacoes_desempenho"]:
    if t in existentes:
        spark.sql(f"ALTER TABLE {S}.{t} DROP CONSTRAINT IF EXISTS pk_{t} CASCADE")


def lit(s):
    return json.dumps(s, ensure_ascii=False)


for t, m in META["tabelas"].items():
    spark.sql(f"CREATE OR REPLACE TABLE {S}.{t} AS SELECT * FROM read_files('{VOL_RAW}/{t}.parquet', format => 'parquet')")
    spark.sql(f"COMMENT ON TABLE {S}.{t} IS {lit(m['comentario'])}")
    for c, com in m["colunas"].items():
        spark.sql(f"ALTER TABLE {S}.{t} ALTER COLUMN {c} COMMENT {lit(com)}")
    print("ok", t)

# COMMAND ----------

for t, c in META["pk"].items():
    spark.sql(f"ALTER TABLE {S}.{t} ALTER COLUMN {c} SET NOT NULL")
    spark.sql(f"ALTER TABLE {S}.{t} ADD CONSTRAINT pk_{t} PRIMARY KEY ({c})")
spark.sql(f"ALTER TABLE {S}.avaliacoes_desempenho ALTER COLUMN ciclo SET NOT NULL")
spark.sql(f"ALTER TABLE {S}.avaliacoes_desempenho ALTER COLUMN colaborador_id SET NOT NULL")
spark.sql(f"ALTER TABLE {S}.avaliacoes_desempenho ADD CONSTRAINT pk_avaliacoes_desempenho PRIMARY KEY (ciclo, colaborador_id)")
for t, c, rt, rc in META["fk"]:
    spark.sql(f"ALTER TABLE {S}.{t} ADD CONSTRAINT fk_{t}_{c} FOREIGN KEY ({c}) REFERENCES {S}.{rt}({rc})")
print("ok chaves primárias e estrangeiras")

# COMMAND ----------

# MAGIC %md ## 3. Tabela gold `headcount_mensal`

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {S}.headcount_mensal
COMMENT 'Gold: headcount (fotografia no último dia do mês), admissões e desligamentos por mês, área e unidade. Base para turnover mensal = desligamentos / headcount médio.'
AS WITH meses AS (
  SELECT explode(sequence(DATE'2024-01-01', DATE'2026-09-01', INTERVAL 1 MONTH)) AS mes
), base AS (
  SELECT m.mes, last_day(m.mes) AS fim_mes, c.*
  FROM meses m CROSS JOIN {S}.colaboradores c
)
SELECT mes, area_id, unidade_id,
  COUNT_IF(data_admissao <= fim_mes AND (data_desligamento IS NULL OR data_desligamento > fim_mes)) AS headcount_fim_mes,
  COUNT_IF(data_admissao BETWEEN mes AND fim_mes) AS admissoes,
  COUNT_IF(data_desligamento BETWEEN mes AND fim_mes) AS desligamentos
FROM base GROUP BY ALL""")
for c, com in {"mes": "Mês de referência (primeiro dia).", "area_id": "Área (FK dim_areas).", "unidade_id": "Unidade (FK dim_unidades).",
               "headcount_fim_mes": "Colaboradores ativos no último dia do mês.", "admissoes": "Admissões no mês.",
               "desligamentos": "Desligamentos no mês."}.items():
    spark.sql(f"ALTER TABLE {S}.headcount_mensal ALTER COLUMN {c} COMMENT {lit(com)}")
spark.sql(f"ALTER TABLE {S}.headcount_mensal ADD CONSTRAINT fk_headcount_mensal_area_id FOREIGN KEY (area_id) REFERENCES {S}.dim_areas(area_id)")
spark.sql(f"ALTER TABLE {S}.headcount_mensal ADD CONSTRAINT fk_headcount_mensal_unidade_id FOREIGN KEY (unidade_id) REFERENCES {S}.dim_unidades(unidade_id)")
print("ok headcount_mensal")

# COMMAND ----------

# MAGIC %md ## 4. Conferência
# MAGIC Contagens esperadas: colaboradores 4.299 (2.388 ativos) · folha_pagamento 74.800 · desligamentos 1.911 · vagas 2.333 ·
# MAGIC absenteismo 9.465 · treinamentos 22.142 · avaliacoes_desempenho 4.087 · pesquisa_clima 6.848 · headcount_mensal 1.617

# COMMAND ----------

esperado = {"dim_unidades": 12, "dim_areas": 16, "dim_cargos": 57, "dim_cursos": 15, "colaboradores": 4299,
            "desligamentos": 1911, "vagas": 2333, "folha_pagamento": 74800, "absenteismo": 9465,
            "treinamentos": 22142, "avaliacoes_desempenho": 4087, "pesquisa_clima": 6848, "headcount_mensal": 1617}
linhas = []
for t, n in esperado.items():
    real = spark.table(f"{S}.{t}").count()
    linhas.append((t, n, real, "OK" if real == n else "DIVERGENTE"))
display(spark.createDataFrame(linhas, "tabela string, esperado long, encontrado long, status string"))
assert all(l[3] == "OK" for l in linhas), "Alguma tabela ficou com contagem diferente da esperada"
print(f"Base instalada em {CAT}.{SCH} + PDFs em {VOL_POL}")
