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
dbutils.widgets.text("data_source", "products", "Data Source")

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
# MAGIC

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformações**

# COMMAND ----------

# MAGIC %md
# MAGIC - 1: Remover Duplicados

# COMMAND ----------

print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['product_id'])
print('Rows after duplicates dropped: ', df_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC - 2: Correção de Caixa (Title Case)
# MAGIC
# MAGIC (energy bars ---> Energy Bars, protien bars ---> Protein Bars etc)

# COMMAND ----------

df_silver.select('category').distinct().show()

# COMMAND ----------

# Correção de caixa (title case)
df_silver = df_silver.withColumn(
    "category",
    F.when(F.col("category").isNull(), None)
     .otherwise(F.initcap("category"))
)

# COMMAND ----------

df_silver.select('category').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - 3: Corrigir Erro de Ortografia de `Protien`

# COMMAND ----------

# Substituir 'protien' → 'protein' em product_name e category
df_silver = (
    df_silver
    .withColumn(
        "product_name",
        F.regexp_replace(F.col("product_name"), "(?i)Protien", "Protein")
    )
    .withColumn(
        "category",
        F.regexp_replace(F.col("category"), "(?i)Protien", "Protein")
    )
)


# COMMAND ----------

display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Padronização de Atributos para Correspondência com o Modelo de Dados da Empresa Principal

# COMMAND ----------

### 1: Adicionar coluna division
df_silver = (
    df_silver
    .withColumn(
        "division",
        F.when(F.col("category") == "Energy Bars",        "Nutrition Bars")
         .when(F.col("category") == "Protein Bars",       "Nutrition Bars")
         .when(F.col("category") == "Granola & Cereals",  "Breakfast Foods")
         .when(F.col("category") == "Recovery Dairy",     "Dairy & Recovery")
         .when(F.col("category") == "Healthy Snacks",     "Healthy Snacks")
         .when(F.col("category") == "Electrolyte Mix",    "Hydration & Electrolytes")
         .otherwise("Other")
    )
)


### 2: Coluna variant
df_silver = df_silver.withColumn(
    "variant",
    F.regexp_extract(F.col("product_name"), r"\((.*?)\)", 1)
)


### 3: Criar nova coluna: product_code  

# product_ids inválidos são substituídos por um valor padrão para evitar perda de registros e garantir joins consistentes

df_silver = (
    df_silver
    # 1. Gerar product_code determinístico a partir de product_name
    .withColumn(
        "product_code",
        F.sha2(F.col("product_name").cast("string"), 256)
    )
    # 2. Limpar product_id: manter apenas IDs numéricos, caso contrário definir como 999999
    .withColumn(
        "product_id",
        F.when(
            F.col("product_id").cast("string").rlike("^[0-9]+$"),
            F.col("product_id").cast("string")
        ).otherwise(F.lit(999999).cast("string"))
    )
    # 3. Renomear product_name → product
    .withColumnRenamed("product_name", "product")
)

# COMMAND ----------

df_silver = df_silver.select("product_code", "division", "category", "product", "variant", "product_id", "read_timestamp", "file_name", "file_size")

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")
df_gold = df_silver.select("product_code", "product_id", "division", "category", "product", "variant")
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

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_products")
df_child_products = spark.sql(f"SELECT product_code, division, category, product, variant FROM fmcg.gold.sb_dim_products;")
df_child_products.show(5)

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_products.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).execute()

# COMMAND ----------

# DBTITLE 1,Resumo do Processamento
# MAGIC %md
# MAGIC ## Resumo do Processamento de Dados de Produtos
# MAGIC
# MAGIC Este notebook realiza o processamento completo dos dados de produtos através das camadas **Bronze**, **Silver** e **Gold**, seguindo a arquitetura medallion.
# MAGIC
# MAGIC ### Camada Bronze
# MAGIC
# MAGIC * **Importação de bibliotecas**: `pyspark.sql.functions` e `delta.tables.DeltaTable`
# MAGIC * **Carregamento de utilitários do projeto**: execução do notebook `/consolidated_pipeline/1_setup/utilities` para obter esquemas (`bronze`, `silver`, `gold`)
# MAGIC * **Inicialização de widgets**: configuração dos parâmetros `catalog` (fmcg) e `data_source` (products)
# MAGIC * **Leitura do CSV**: carregamento dos dados de `s3://sportsbar-anapdeabreu/products/*.csv` com inferência de schema, adição de `read_timestamp` e metadados de arquivo (`file_name`, `file_size`)
# MAGIC * **Escrita na camada Bronze**: gravação como tabela Delta `fmcg.bronze.products` com Change Data Feed habilitado
# MAGIC
# MAGIC ### Camada Silver
# MAGIC
# MAGIC * **Leitura dos dados Bronze**: carregamento da tabela `fmcg.bronze.products`
# MAGIC * **Transformações aplicadas**:
# MAGIC   1. **Remoção de duplicados**: eliminação de registros duplicados com base em `product_id` (20 → 18 registros)
# MAGIC   2. **Correção de caixa (Title Case)**: aplicação de `F.initcap()` na coluna `category` para padronizar nomes (ex: `energy bars` → `Energy Bars`)
# MAGIC   3. **Correção de ortografia**: substituição de `Protien` por `Protein` nas colunas `product_name` e `category` usando `regexp_replace` com regex case-insensitive
# MAGIC * **Padronização de atributos para correspondência com o modelo de dados da empresa principal**:
# MAGIC   1. **Coluna `division`**: mapeamento de categorias para divisões (ex: Energy Bars → Nutrition Bars, Granola & Cereals → Breakfast Foods)
# MAGIC   2. **Coluna `variant`**: extração do conteúdo entre parênteses em `product_name` (ex: `60g`, `40g`)
# MAGIC   3. **Coluna `product_code`**: geração de hash SHA-256 determinístico a partir de `product_name`
# MAGIC   4. **Limpeza de `product_id`**: manutenção apenas de IDs numéricos; valores inválidos substituídos por `999999`
# MAGIC   5. **Renomeação**: `product_name` → `product`
# MAGIC * **Escrita na camada Silver**: gravação como tabela Delta `fmcg.silver.products` com Change Data Feed e `mergeSchema` habilitados
# MAGIC
# MAGIC ### Camada Gold
# MAGIC
# MAGIC * **Leitura dos dados Silver**: carregamento da tabela `fmcg.silver.products`
# MAGIC * **Seleção de colunas**: `product_code`, `product_id`, `division`, `category`, `product`, `variant`
# MAGIC * **Escrita na camada Gold**: gravação como tabela Delta `fmcg.gold.sb_dim_products` com Change Data Feed habilitado
# MAGIC
# MAGIC ### Mesclagem com a Tabela Principal
# MAGIC
# MAGIC * **Mesclagem (MERGE)**: dados de `fmcg.gold.sb_dim_products` são mesclados na tabela dimensional `fmcg.gold.dim_products` usando `DeltaTable.merge()`
# MAGIC   * **Chave de mesclagem**: `product_code`
# MAGIC   * **Quando correspondido (whenMatched)**: atualização das colunas `division`, `category`, `product`, `variant`
# MAGIC   * **Quando não correspondido (whenNotMatched)**: inserção de novos registros