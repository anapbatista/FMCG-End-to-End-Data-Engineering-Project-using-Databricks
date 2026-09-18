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
dbutils.widgets.text("data_source", "customers", "Data Source")

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

df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformações**

# COMMAND ----------

# MAGIC %md
# MAGIC - 1: Remover Duplicados

# COMMAND ----------



df_duplicates = df_bronze.groupBy("customer_id").count().filter(F.col("count") > 1)
display(df_duplicates)

# COMMAND ----------

print('Rows before duplicates dropped: ', df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print('Rows after duplicates dropped: ', df_silver.count())

# COMMAND ----------

# MAGIC %md
# MAGIC - 2: Remover espaços no nome do cliente

# COMMAND ----------

# Verificar esses valores
display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

## Remover os espaços encontrados

df_silver = df_silver.withColumn(
    "customer_name",
    F.trim(F.col("customer_name"))
)

# COMMAND ----------

# # Verificação de Sanidade

# # Verificar esses valores
display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name"))))


# COMMAND ----------

# MAGIC %md
# MAGIC - 3: Correção de Qualidade de Dados: Corrigindo Erros de Ortografia da Cidade

# COMMAND ----------

df_silver.select('city').distinct().show()

# COMMAND ----------

# # Dicionário de erros de ortografia
# city_typos = {
#     'Bengaluru': ['Bengaluruu', 'Bengaluruu', 'Bengalore'],
#     'Hyderabad': ['Hyderabadd', 'Hyderbad'],
#     'New Delhi': ['NewDelhi', 'NewDheli', 'NewDelhee']
# }

# Erros → nomes corretos
city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',

    'Hyderabadd': 'Hyderabad',
    'Hyderbad': 'Hyderabad',

    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi'
}


allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

df_silver = (
    df_silver
    .replace(city_mapping, subset=["city"])
    .withColumn(
        "city",
        F.when(F.col("city").isNull(), None)
         .when(F.col("city").isin(allowed), F.col("city"))
         .otherwise(None)
    )
)

# COMMAND ----------

# Verificação de sanidade
df_silver.select('city').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - 4: Corrigir Problema de Caixa (Title Case)

# COMMAND ----------

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

# Correção de caixa (title case)
df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(), None)
     .otherwise(F.initcap("customer_name"))
)

# COMMAND ----------

# Verificação de sanidade

df_silver.select('customer_name').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - 5: Tratamento de Cidades Ausentes

# COMMAND ----------

df_silver.filter(F.col("city").isNull()).show(truncate=False)


# COMMAND ----------

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------


# Nota de Confirmação de Negócio: Correções de cidade confirmadas pela equipe de negócio
customer_city_fix = {
    # Sprintx Nutrition
    789403: "New Delhi",

    # Zenathlete Foods
    789420: "Bengaluru",

    # Primefuel Nutrition
    789521: "Hyderabad",

    # Recovery Lane
    789603: "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

display(df_fix)

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")   # Substituir nulo pela cidade correta
    )
    .drop("fixed_city")
)

# COMMAND ----------

# Verificações de Sanidade

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC - 6: Converter customer_id para string

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))
print(df_silver.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Padronização de Atributos para Correspondência com o Modelo de Dados da Empresa Principal

# COMMAND ----------

df_silver = (
    df_silver
    # Construir coluna final customer: "NomeCliente-Cidade" ou "NomeCliente-Unknown"
    .withColumn(
        "customer",
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
    )
    
    # Atributos estáticos alinhados com o modelo de dados da empresa principal
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)

# COMMAND ----------

display(df_silver.limit(5))

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


# Selecionar apenas colunas necessárias
# "customer_id, customer_name, city, read_timestamp, file_name, file_size, customer, market, platform, channel"
df_gold = df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

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

from delta.tables import DeltaTable
from pyspark.sql import functions as F

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# DBTITLE 1,Resumo do Processamento
# MAGIC %md
# MAGIC ## Resumo do Processamento de Dados de Clientes
# MAGIC
# MAGIC Este notebook realiza o processamento completo dos dados de clientes através das camadas **Bronze**, **Silver** e **Gold**, seguindo a arquitetura medallion.
# MAGIC
# MAGIC ### Camada Bronze
# MAGIC
# MAGIC * **Importação de bibliotecas**: `pyspark.sql.functions` e `delta.tables.DeltaTable`
# MAGIC * **Carregamento de utilitários do projeto**: execução do notebook `/consolidated_pipeline/1_setup/utilities` para obter esquemas (`bronze`, `silver`, `gold`)
# MAGIC * **Inicialização de widgets**: configuração dos parâmetros `catalog` (fmcg) e `data_source` (customers)
# MAGIC * **Leitura do CSV**: carregamento dos dados de `s3://sportsbar-anapdeabreu/customers/*.csv` com inferência de schema, adição de `read_timestamp` e metadados de arquivo (`file_name`, `file_size`)
# MAGIC * **Escrita na camada Bronze**: gravação como tabela Delta `fmcg.bronze.customers` com Change Data Feed habilitado
# MAGIC
# MAGIC ### Camada Silver
# MAGIC
# MAGIC * **Leitura dos dados Bronze**: carregamento da tabela `fmcg.bronze.customers`
# MAGIC * **Transformações aplicadas**:
# MAGIC   1. **Remoção de duplicados**: eliminação de registros duplicados com base em `customer_id` (39 → 35 registros)
# MAGIC   2. **Remoção de espaços**: aplicação de `F.trim()` na coluna `customer_name` para remover espaços em branco no início e fim
# MAGIC   3. **Correção de erros de ortografia da cidade**: mapeamento de nomes de cidades incorretos (ex: `Bengaluruu` → `Bengaluru`, `Hyderbad` → `Hyderabad`, `NewDheli` → `New Delhi`) e definição de valores nulo para cidades não reconhecidas
# MAGIC   4. **Correção de caixa (Title Case)**: aplicação de `F.initcap()` na coluna `customer_name` para padronizar capitalização (ex: `MacroBite superfoods` → `Macrobite Superfoods`)
# MAGIC   5. **Tratamento de cidades ausentes**: identificação de clientes com cidade nula e preenchimento com valores confirmados pela equipe de negócio (Sprintx Nutrition → New Delhi, Zenathlete Foods → Bengaluru, Primefuel Nutrition → Hyderabad, Recovery Lane → Hyderabad)
# MAGIC   6. **Conversão de customer_id para string**: cast da coluna `customer_id` de `integer` para `string`
# MAGIC * **Padronização de atributos para correspondência com o modelo de dados da empresa principal**:
# MAGIC   * **Coluna `customer`**: concatenação de `customer_name` e `city` no formato `"NomeCliente-Cidade"` (ou `"NomeCliente-Unknown"` se cidade for nula)
# MAGIC   * **Atributos estáticos**: `market` = "India", `platform` = "Sports Bar", `channel` = "Acquisition"
# MAGIC * **Escrita na camada Silver**: gravação como tabela Delta `fmcg.silver.customers` com Change Data Feed e `mergeSchema` habilitados
# MAGIC
# MAGIC ### Camada Gold
# MAGIC
# MAGIC * **Leitura dos dados Silver**: carregamento da tabela `fmcg.silver.customers`
# MAGIC * **Seleção de colunas**: `customer_id`, `customer_name`, `city`, `customer`, `market`, `platform`, `channel`
# MAGIC * **Escrita na camada Gold**: gravação como tabela Delta `fmcg.gold.sb_dim_customers` com Change Data Feed habilitado
# MAGIC
# MAGIC ### Mesclagem com a Tabela Principal
# MAGIC
# MAGIC * **Mesclagem (MERGE)**: dados de `fmcg.gold.sb_dim_customers` são mesclados na tabela dimensional `fmcg.gold.dim_customers` usando `DeltaTable.merge()`
# MAGIC   * **Chave de mesclagem**: `customer_code`
# MAGIC   * **Quando correspondido (whenMatchedUpdateAll)**: atualização de todas as colunas
# MAGIC   * **Quando não correspondido (whenNotMatchedInsertAll)**: inserção de novos registros