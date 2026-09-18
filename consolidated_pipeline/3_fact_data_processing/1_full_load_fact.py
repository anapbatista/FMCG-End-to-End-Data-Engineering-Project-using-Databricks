# Databricks notebook source
# MAGIC %md
# MAGIC **Importar Bibliotecas Necessárias**

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC **Carregar Utilitários do Projeto e Inicializar Widgets do Notebook**

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-anapdeabreu/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"
print("Base Path: ", base_path)
print("Landing Path: ", landing_path)
print("Processed Path: ", processed_path)


# Definir as tabelas
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

df = spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}/*.csv").withColumn("read_timestamp", F.current_timestamp()).select("*", "_metadata.file_name", "_metadata.file_size")

print("Total Rows: ", df.count())
df.show(5)

# COMMAND ----------

# DBTITLE 1,Write Delta Table
df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("append") \
 .saveAsTable(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Movendo arquivos do diretório origem para o diretório processado

# COMMAND ----------

files = dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

df_orders = spark.sql(f"SELECT * FROM {bronze_table}")
df_orders.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformações**

# COMMAND ----------

# 1. Manter apenas linhas onde order_qty está presente
df_orders = df_orders.filter(F.col("order_qty").isNotNull())


# 2. Limpar customer_id → manter numérico, caso contrário definir como 999999
df_orders = df_orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
     .otherwise("999999")
     .cast("string")
)

# 3. Remover o nome do dia da semana do texto da data
#    "Tuesday, July 01, 2025" → "July 01, 2025"
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# 4. Converter order_placement_date usando múltiplos formatos possíveis
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "dd/MM/yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)

# 5. Remover duplicados
df_orders = df_orders.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

# 6. Converter product_id para string
df_orders = df_orders.withColumn('product_id', F.col('product_id').cast('string'))

# COMMAND ----------

# Verificar a data máxima e mínima
df_orders.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Join com a tabela de produtos**

# COMMAND ----------

df_products = spark.table("fmcg.silver.products")
df_joined = df_orders.join(df_products, on="product_id", how="inner").select(df_orders["*"], df_products["product_code"])

df_joined.show(5)

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(df_joined.alias("bronze"), "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {silver_table};")

df_gold.show(2)

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(df_gold.alias("gold"), "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mesclando com a Empresa Principal

# COMMAND ----------

# MAGIC %md
# MAGIC - Nota: Queremos dados em nível mensal, mas os dados da filial estão em nível diário

# COMMAND ----------

# MAGIC %md
# MAGIC **Carga Completa**

# COMMAND ----------

df_child = spark.sql(f"SELECT date, product_code, customer_code, sold_quantity FROM {gold_table}")
df_child.show(10)

# COMMAND ----------

df_child.count()

# COMMAND ----------

df_monthly = (
    df_child
    # 1. Obter o início do mês (ex: 2025-11-30 → 2025-11-01)
    .withColumn("month_start", F.trunc("date", "MM"))   # ou F.date_trunc("month", "date").cast("date")

    # 2. Agrupar em granularidade mensal por month_start + product_code + customer_code
    .groupBy("month_start", "product_code", "customer_code")
    .agg(
        F.sum("sold_quantity").alias("sold_quantity")
    )

    # 3. Renomear month_start de volta para `date` para corresponder ao schema alvo
    .withColumnRenamed("month_start", "date")
)

df_monthly.show(5, truncate=False)

# COMMAND ----------

df_monthly.count()

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_monthly.alias("child_gold"), "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# DBTITLE 1,Resumo do Processamento
# MAGIC %md
# MAGIC ## Resumo do Processamento de Dados de Pedidos (Fact)
# MAGIC
# MAGIC Este notebook realiza o processamento completo dos dados de pedidos (`orders`) através das camadas **Bronze**, **Silver** e **Gold**, seguindo a arquitetura medallion, incluindo a agregação mensal e a mesclagem com a tabela fato principal da empresa.
# MAGIC
# MAGIC ### Camada Bronze
# MAGIC
# MAGIC * **Importação de bibliotecas**: `pyspark.sql.functions` e `delta.tables.DeltaTable`
# MAGIC * **Carregamento de utilitários do projeto**: execução do notebook `/consolidated_pipeline/1_setup/utilities` para obter esquemas (`bronze`, `silver`, `gold`)
# MAGIC * **Inicialização de widgets**: configuração dos parâmetros `catalog` (fmcg) e `data_source` (orders); definição dos caminhos S3 (`landing_path` e `processed_path`) e nomes das tabelas Bronze, Silver e Gold
# MAGIC * **Leitura do CSV**: carregamento dos dados de `s3://sportsbar-final/orders/landing/*.csv` com inferência de schema, adição de `read_timestamp` e metadados de arquivo (`file_name`, `file_size`)
# MAGIC * **Escrita na camada Bronze**: gravação como tabela Delta `fmcg.bronze.orders` com Change Data Feed habilitado (modo `append`)
# MAGIC * **Movimentação de arquivos**: após a carga, os arquivos CSV são movidos do diretório `landing` para o diretório `processed` usando `dbutils.fs.mv()`
# MAGIC
# MAGIC ### Camada Silver
# MAGIC
# MAGIC * **Leitura dos dados Bronze**: carregamento da tabela `fmcg.bronze.orders`
# MAGIC * **Transformações aplicadas**:
# MAGIC   1. **Filtrar linhas sem `order_qty`**: manter apenas linhas onde `order_qty` está presente
# MAGIC   2. **Limpar `customer_id`**: manter apenas valores numéricos; caso contrário, definir como `999999`
# MAGIC   3. **Remover dia da semana da data**: remoção do nome do dia da semana do texto da data (ex: `"Tuesday, July 01, 2025"` → `"July 01, 2025"`) usando `regexp_replace`
# MAGIC   4. **Converter `order_placement_date`**: aplicação de `F.coalesce()` com `F.try_to_date()` para múltiplos formatos (`yyyy/MM/dd`, `dd-MM-yyyy`, `dd/MM/yyyy`, `MMMM dd, yyyy`)
# MAGIC   5. **Remover duplicados**: eliminação de registros duplicados com base em `order_id`, `order_placement_date`, `customer_id`, `product_id`, `order_qty`
# MAGIC   6. **Converter `product_id` para string**: cast da coluna `product_id` para `string`
# MAGIC * **Join com a tabela de produtos**: inner join com `fmcg.silver.products` para obter o `product_code` correspondente a cada `product_id`
# MAGIC * **Escrita na camada Silver**: gravação como tabela Delta `fmcg.silver.orders` com Change Data Feed e `mergeSchema` habilitados; se a tabela já existir, é realizado um `MERGE` (upsert) usando `DeltaTable.merge()` com chaves de `order_placement_date`, `order_id`, `product_code` e `customer_id`
# MAGIC
# MAGIC ### Camada Gold
# MAGIC
# MAGIC * **Leitura dos dados Silver**: seleção e renomeação de colunas (`order_placement_date` → `date`, `customer_id` → `customer_code`, `order_qty` → `sold_quantity`)
# MAGIC * **Escrita na camada Gold**: gravação como tabela Delta `fmcg.gold.sb_fact_orders` com Change Data Feed habilitado; se a tabela já existir, é realizado um `MERGE` (upsert) usando `DeltaTable.merge()` com chaves de `date`, `order_id`, `product_code` e `customer_code`
# MAGIC
# MAGIC ### Mesclagem com a Empresa Principal
# MAGIC
# MAGIC * **Nota**: Os dados da filial estão em nível diário, mas a tabela fato principal requer dados em nível mensal
# MAGIC * **Carga Completa**:
# MAGIC   * Leitura dos dados Gold (`fmcg.gold.sb_fact_orders`)
# MAGIC   * **Agregação mensal** (40.811 registros diários → 3.060 registros mensais):
# MAGIC     1. Obtenção do início do mês usando `F.trunc("date", "MM")`
# MAGIC     2. Agrupamento por `month_start`, `product_code` e `customer_code` com soma de `sold_quantity`
# MAGIC     3. Renomeação de `month_start` de volta para `date`
# MAGIC   * **Mesclagem (MERGE)**: dados mensais agregados são mesclados na tabela fato principal `fmcg.gold.fact_orders` usando `DeltaTable.merge()`
# MAGIC     * **Chave de mesclagem**: `date`, `product_code`, `customer_code`
# MAGIC     * **Quando correspondido (whenMatchedUpdateAll)**: atualização de todas as colunas
# MAGIC     * **Quando não correspondido (whenNotMatchedInsertAll)**: inserção de novos registros