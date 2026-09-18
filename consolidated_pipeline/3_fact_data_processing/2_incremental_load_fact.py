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
# MAGIC ### Tabela staging para processar apenas os dados incrementais recebidos

# COMMAND ----------

# DBTITLE 1,Write Delta Table
df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze_schema}.staging_{data_source}")

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

df_orders = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.staging_{data_source};")
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
# MAGIC ### Tabela staging para processar apenas os dados incrementais recebidos

# COMMAND ----------

# Staging para dados incrementais

df_joined.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.staging_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {catalog}.{silver_schema}.staging_{data_source};")

df_gold.show(2)

# COMMAND ----------

df_gold.count()

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    print("Criando nova tabela")
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
# MAGIC **Carga Incremental**

# COMMAND ----------

# df_child = suas linhas diárias incrementais

df_child =  spark.sql(f"SELECT order_placement_date as date FROM {catalog}.{silver_schema}.staging_{data_source}")

incremental_month_df = df_child.select(
    F.trunc("date", "MM").alias("start_month")
).distinct()

incremental_month_df.show()

incremental_month_df.createOrReplaceTempView("incremental_months")

# COMMAND ----------

monthly_table = spark.sql(f"""
    SELECT date, product_code, customer_code, sold_quantity
    FROM {catalog}.{gold_schema}.sb_fact_orders sbf
    INNER JOIN incremental_months m
        ON trunc(sbf.date, 'MM') = m.start_month
""")

print("Total Rows: ", monthly_table.count())
monthly_table.show(10)

# COMMAND ----------

monthly_table.select('date').distinct().orderBy('date').show()

# COMMAND ----------

df_monthly_recalc = (
    monthly_table
    .withColumn("month_start", F.trunc("date", "MM"))
    .groupBy("month_start", "product_code", "customer_code")
    .agg(F.sum("sold_quantity").alias("sold_quantity"))
    .withColumnRenamed("month_start", "date")   # month_start → date = primeiro dia do mês
)

df_monthly_recalc.show(10, truncate=False)

# COMMAND ----------

df_monthly_recalc.count()

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_monthly_recalc.alias("child_gold"), "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE fmcg.bronze.staging_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE fmcg.silver.staging_orders;

# COMMAND ----------

# DBTITLE 1,Resumo do Processamento
# MAGIC %md
# MAGIC ## Resumo do Processamento Incremental de Pedidos (Fact)
# MAGIC
# MAGIC Este notebook realiza o processamento incremental dos dados de pedidos (`orders`) através das camadas **Bronze**, **Silver** e **Gold**, seguindo a arquitetura medallion. Diferente do notebook de carga completa, este processa apenas os novos dados recebidos (incrementais) e recalcula apenas os meses afetados.
# MAGIC
# MAGIC ### Camada Bronze
# MAGIC
# MAGIC * **Importação de bibliotecas**: `pyspark.sql.functions` e `delta.tables.DeltaTable`
# MAGIC * **Carregamento de utilitários do projeto**: execução do notebook `/consolidated_pipeline/1_setup/utilities` para obter esquemas (`bronze`, `silver`, `gold`)
# MAGIC * **Inicialização de widgets**: configuração dos parâmetros `catalog` (fmcg) e `data_source` (orders); definição dos caminhos S3 (`landing_path` e `processed_path`) e nomes das tabelas Bronze, Silver e Gold
# MAGIC * **Leitura do CSV**: carregamento dos dados incrementais de `s3://sportsbar-final/orders/landing/*.csv` com inferência de schema, adição de `read_timestamp` e metadados de arquivo (`file_name`, `file_size`)
# MAGIC * **Escrita na camada Bronze**: gravação como tabela Delta `fmcg.bronze.orders` com Change Data Feed habilitado (modo `append`)
# MAGIC * **Tabela staging**: gravação dos dados incrementais em uma tabela staging `fmcg.bronze.staging_orders` (modo `overwrite`) para processamento isolado
# MAGIC * **Movimentação de arquivos**: após a carga, os arquivos CSV são movidos do diretório `landing` para o diretório `processed` usando `dbutils.fs.mv()`
# MAGIC
# MAGIC ### Camada Silver
# MAGIC
# MAGIC * **Leitura dos dados staging**: carregamento da tabela `fmcg.bronze.staging_orders`
# MAGIC * **Transformações aplicadas**:
# MAGIC   1. **Filtrar linhas sem `order_qty`**: manter apenas linhas onde `order_qty` está presente
# MAGIC   2. **Limpar `customer_id`**: manter apenas valores numéricos; caso contrário, definir como `999999`
# MAGIC   3. **Remover dia da semana da data**: remoção do nome do dia da semana do texto da data (ex: `"Tuesday, July 01, 2025"` → `"July 01, 2025"`) usando `regexp_replace`
# MAGIC   4. **Converter `order_placement_date`**: aplicação de `F.coalesce()` com `F.try_to_date()` para múltiplos formatos (`yyyy/MM/dd`, `dd-MM-yyyy`, `dd/MM/yyyy`, `MMMM dd, yyyy`)
# MAGIC   5. **Remover duplicados**: eliminação de registros duplicados com base em `order_id`, `order_placement_date`, `customer_id`, `product_id`, `order_qty`
# MAGIC   6. **Converter `product_id` para string**: cast da coluna `product_id` para `string`
# MAGIC * **Join com a tabela de produtos**: inner join com `fmcg.silver.products` para obter o `product_code` correspondente a cada `product_id`
# MAGIC * **Escrita na camada Silver**: se a tabela não existir, gravação como tabela Delta `fmcg.silver.orders` com Change Data Feed e `mergeSchema` habilitados; se já existir, é realizado um `MERGE` (upsert) usando `DeltaTable.merge()` com chaves de `order_placement_date`, `order_id`, `product_code` e `customer_id`
# MAGIC * **Tabela staging Silver**: gravação dos dados incrementais processados em `fmcg.silver.staging_orders` (modo `overwrite`) para uso na camada Gold
# MAGIC
# MAGIC ### Camada Gold
# MAGIC
# MAGIC * **Leitura dos dados staging Silver**: seleção e renomeação de colunas (`order_placement_date` → `date`, `customer_id` → `customer_code`, `order_qty` → `sold_quantity`)
# MAGIC * **Escrita na camada Gold**: se a tabela não existir, gravação como tabela Delta `fmcg.gold.sb_fact_orders` com Change Data Feed habilitado; se já existir, é realizado um `MERGE` (upsert) usando `DeltaTable.merge()` com chaves de `date`, `order_id`, `product_code` e `customer_code`
# MAGIC
# MAGIC ### Mesclagem com a Empresa Principal
# MAGIC
# MAGIC * **Nota**: Os dados da filial estão em nível diário, mas a tabela fato principal requer dados em nível mensal
# MAGIC * **Carga Incremental**:
# MAGIC   * Identificação dos meses afetados pelos dados incrementais usando `F.trunc("date", "MM")` a partir da tabela staging Silver
# MAGIC   * Consulta dos registros diários da tabela Gold `fmcg.gold.sb_fact_orders` apenas para os meses afetados (join com a view temporária `incremental_months`)
# MAGIC   * **Recálculo mensal**: agrupamento por `month_start`, `product_code` e `customer_code` com soma de `sold_quantity`; renomeação de `month_start` para `date` (primeiro dia do mês)
# MAGIC   * **Mesclagem (MERGE)**: dados mensais recalculados são mesclados na tabela fato principal `fmcg.gold.fact_orders` usando `DeltaTable.merge()`
# MAGIC     * **Chave de mesclagem**: `date`, `product_code`, `customer_code`
# MAGIC     * **Quando correspondido (whenMatchedUpdateAll)**: atualização de todas as colunas
# MAGIC     * **Quando não correspondido (whenNotMatchedInsertAll)**: inserção de novos registros
# MAGIC
# MAGIC ### Limpeza
# MAGIC
# MAGIC * **Remoção das tabelas staging**: exclusão das tabelas `fmcg.bronze.staging_orders` e `fmcg.silver.staging_orders` usando `DROP TABLE`