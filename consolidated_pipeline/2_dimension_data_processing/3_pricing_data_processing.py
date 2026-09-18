# Databricks notebook source
# MAGIC %md
# MAGIC **Importar Bibliotecas Necessárias**

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC **Carregar Utilitários do Projeto e Inicializar Widgets do Notebook**

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "gross_price", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-anapdeabreu/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

df = (
    spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(base_path)
        .withColumn("read_timestamp", F.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)

# COMMAND ----------

# Verificar o tipo de dados
df.printSchema()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformações**

# COMMAND ----------

# MAGIC %md
# MAGIC - 1: Normalizar o campo `month`

# COMMAND ----------

df_bronze.select('month').distinct().show()

# COMMAND ----------


# 1️. Converter `month` de múltiplos formatos possíveis
date_formats = ["yyyy/MM/dd", "dd/MM/yyyy", "yyyy-MM-dd", "dd-MM-yyyy"]

df_silver = df_bronze.withColumn(
    "month",
    F.coalesce(
        F.try_to_date(F.col("month"), "yyyy/MM/dd"),
        F.try_to_date(F.col("month"), "dd/MM/yyyy"),
        F.try_to_date(F.col("month"), "yyyy-MM-dd"),
        F.try_to_date(F.col("month"), "dd-MM-yyyy")
    )
)

# COMMAND ----------

df_silver.select('month').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - 2: Tratamento de `gross_price`

# COMMAND ----------

df_silver.show(10)

# COMMAND ----------

# Estamos validando a coluna gross_price, convertendo apenas valores numéricos válidos para double, corrigindo preços negativos tornando-os positivos, e substituindo todos os valores não numéricos por 0


df_silver = df_silver.withColumn(
    "gross_price",
    F.when(F.col("gross_price").rlike(r'^-?\d+(\.\d+)?$'), 
           F.when(F.col("gross_price").cast("double") < 0, -1 * F.col("gross_price").cast("double"))
            .otherwise(F.col("gross_price").cast("double")))
    .otherwise(0)
)

# COMMAND ----------

df_silver.show(10)

# COMMAND ----------

# Enriquecemos o dataset silver realizando um inner join com a tabela de produtos para obter o product_code correto para cada product_id.

df_products = spark.table("fmcg.silver.products") 
df_joined = df_silver.join(df_products.select("product_id", "product_code"), on="product_id", how="inner")
df_joined = df_joined.select("product_id", "product_code", "month", "gross_price", "read_timestamp", "file_name", "file_size")

df_joined.show(5)

# COMMAND ----------

df_joined.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true")\
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

# COMMAND ----------

# Selecionar apenas colunas necessárias
df_gold = df_silver.select("product_code", "month", "gross_price")
df_gold.show(5)

# COMMAND ----------

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mesclando Fonte de Dados com a Tabela Principal

# COMMAND ----------

df_gold_price = spark.table("fmcg.gold.sb_dim_gross_price")
df_gold_price.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC - Obter o preço para cada product_code (agregado por ano)

# COMMAND ----------

df_gold_price = (
    df_gold_price
    .withColumn("year", F.year("month"))
    # 0 = preço não-zero, 1 = preço zero ➜ não-zero vem primeiro
    .withColumn("is_zero", F.when(F.col("gross_price") == 0, 1).otherwise(0))
)

w = (
    Window
    .partitionBy("product_code", "year")
    .orderBy(F.col("is_zero"), F.col("month").desc())
)


df_gold_latest_price = (
    df_gold_price
      .withColumn("rnk", F.row_number().over(w))
      .filter(F.col("rnk") == 1)
)


# COMMAND ----------

display(df_gold_latest_price)

# COMMAND ----------

## Selecionar colunas necessárias

df_gold_latest_price = df_gold_latest_price.select("product_code", "year", "gross_price").withColumnRenamed("gross_price", "price_inr").select("product_code", "price_inr", "year")

# Converter year para string
df_gold_latest_price = df_gold_latest_price.withColumn("year", F.col("year").cast("string"))

df_gold_latest_price.show(5)

# COMMAND ----------

df_gold_latest_price.printSchema()

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_gross_price")


delta_table.alias("target").merge(
    source=df_gold_latest_price.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).execute()

# COMMAND ----------

# DBTITLE 1,Resumo do Processamento
# MAGIC %md
# MAGIC ## Resumo do Processamento de Dados de Preços
# MAGIC
# MAGIC Este notebook realiza o processamento completo dos dados de preços brutos (`gross_price`) através das camadas **Bronze**, **Silver** e **Gold**, seguindo a arquitetura medallion.
# MAGIC
# MAGIC ### Camada Bronze
# MAGIC
# MAGIC * **Importação de bibliotecas**: `pyspark.sql.functions`, `delta.tables.DeltaTable` e `pyspark.sql.window.Window`
# MAGIC * **Carregamento de utilitários do projeto**: execução do notebook `/consolidated_pipeline/1_setup/utilities` para obter esquemas (`bronze`, `silver`, `gold`)
# MAGIC * **Inicialização de widgets**: configuração dos parâmetros `catalog` (fmcg) e `data_source` (gross_price)
# MAGIC * **Leitura do CSV**: carregamento dos dados de `s3://sportsbar-final/gross_price/*.csv` com inferência de schema, adição de `read_timestamp` e metadados de arquivo (`file_name`, `file_size`)
# MAGIC * **Escrita na camada Bronze**: gravação como tabela Delta `fmcg.bronze.gross_price` com Change Data Feed habilitado
# MAGIC
# MAGIC ### Camada Silver
# MAGIC
# MAGIC * **Leitura dos dados Bronze**: carregamento da tabela `fmcg.bronze.gross_price`
# MAGIC * **Transformações aplicadas**:
# MAGIC   1. **Normalização do campo `month`**: conversão de múltiplos formatos de data (`yyyy/MM/dd`, `dd/MM/yyyy`, `yyyy-MM-dd`, `dd-MM-yyyy`) para um formato padronizado usando `F.coalesce()` com `F.try_to_date()`
# MAGIC   2. **Tratamento de `gross_price`**:
# MAGIC      * Validação da coluna `gross_price`, convertendo apenas valores numéricos válidos para `double`
# MAGIC      * Correção de preços negativos tornando-os positivos (valor absoluto)
# MAGIC      * Substituição de todos os valores não numéricos por `0`
# MAGIC   3. **Enriquecimento com `product_code`**: inner join com a tabela `fmcg.silver.products` para obter o `product_code` correto para cada `product_id`
# MAGIC * **Escrita na camada Silver**: gravação como tabela Delta `fmcg.silver.gross_price` com Change Data Feed e `mergeSchema` habilitados
# MAGIC
# MAGIC ### Camada Gold
# MAGIC
# MAGIC * **Leitura dos dados Silver**: carregamento da tabela `fmcg.silver.gross_price`
# MAGIC * **Seleção de colunas**: `product_code`, `month`, `gross_price`
# MAGIC * **Escrita na camada Gold**: gravação como tabela Delta `fmcg.gold.sb_dim_gross_price` com Change Data Feed habilitado
# MAGIC
# MAGIC ### Mesclagem com a Tabela Principal
# MAGIC
# MAGIC * **Obtenção do preço por product_code (agregado por ano)**:
# MAGIC   * Adição da coluna `year` extraída de `month`
# MAGIC   * Criação de flag `is_zero` (0 = preço não-zero, 1 = preço zero) para priorizar preços válidos
# MAGIC   * Uso de window function (`row_number()`) particionada por `product_code` e `year`, ordenando para obter o preço mais recente não-zero
# MAGIC   * Seleção das colunas finais: `product_code`, `price_inr` (renomeado de `gross_price`), `year` (convertido para string)
# MAGIC * **Mesclagem (MERGE)**: dados agregados são mesclados na tabela dimensional `fmcg.gold.dim_gross_price` usando `DeltaTable.merge()`
# MAGIC   * **Chave de mesclagem**: `product_code`
# MAGIC   * **Quando correspondido (whenMatched)**: atualização das colunas `price_inr` e `year`
# MAGIC   * **Quando não correspondido (whenNotMatched)**: inserção de novos registros com `product_code`, `price_inr` e `year`