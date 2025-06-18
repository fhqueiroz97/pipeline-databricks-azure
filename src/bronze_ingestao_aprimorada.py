# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ingestão de Dados Brutos
# MAGIC
# MAGIC **Objetivo**: Ingerir dados brutos no formato original com metadados de controle
# MAGIC
# MAGIC **Características da Bronze Layer**:
# MAGIC - Dados no formato original (raw data)
# MAGIC - Sem transformações complexas
# MAGIC - Adição de metadados de controle
# MAGIC - Formato Delta para melhor performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configurações Iniciais

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import uuid

# Configurações do ambiente
workspace_path = "/Workspace/Repos/senaipr31@fiap.com.br/pipeline-databricks-azure/src"
bronze_path = f"{workspace_path}/data/bronze"

print(f"Workspace: {workspace_path}")
print(f"Bronze Layer Path: {bronze_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criação de Dados Simulados (Para fins didáticos)

# COMMAND ----------

# Simulando dados de vendas que chegaram via API/File
dados_vendas = [
    (1, "João Silva", "Produto A", 150.50, "2024-06-15", "SP"),
    (2, "Maria Santos", "Produto B", 89.90, "2024-06-15", "RJ"),
    (3, "Pedro Costa", "Produto A", 150.50, "2024-06-16", "MG"),
    (4, "Ana Lima", "Produto C", 299.99, "2024-06-16", "SP"),
    (5, "Carlos Souza", "Produto B", 89.90, "2024-06-17", "RS")
]

schema_vendas = StructType([
    StructField("id_venda", IntegerType(), True),
    StructField("cliente", StringType(), True),
    StructField("produto", StringType(), True),
    StructField("valor", DoubleType(), True),
    StructField("data_venda", StringType(), True),
    StructField("estado", StringType(), True)
])

df_vendas = spark.createDataFrame(dados_vendas, schema_vendas)
display(df_vendas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aplicação de Metadados de Controle (Bronze Pattern)

# COMMAND ----------

def adicionar_metadados_bronze(df, fonte_sistema):
    """
    Adiciona metadados de controle para a camada Bronze
    """
    df_bronze = df.withColumn("bronze_load_timestamp", current_timestamp()) \
                  .withColumn("bronze_load_id", lit(str(uuid.uuid4()))) \
                  .withColumn("bronze_source_system", lit(fonte_sistema)) \
                  .withColumn("bronze_file_name", lit("api_vendas_daily")) \
                  .withColumn("bronze_is_deleted", lit(False))
    
    return df_bronze

# Aplicando metadados
df_bronze = adicionar_metadados_bronze(df_vendas, "sistema_vendas")
display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Salvamento na Bronze Layer

# COMMAND ----------

# Configurações para otimização
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Salvando em formato Delta (melhor para lakehouse)
try:
    df_bronze.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("data_venda") \
        .save(f"{bronze_path}/vendas")
    
    print("✅ Dados salvos com sucesso na Bronze Layer!")
    
except Exception as e:
    print(f"❌ Erro ao salvar na Bronze Layer: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação dos Dados Salvos

# COMMAND ----------

# Lendo dados da Bronze para validação
try:
    df_validacao = spark.read.format("delta").load(f"{bronze_path}/vendas")
    
    print(f"Total de registros na Bronze: {df_validacao.count()}")
    print("\nEsquema dos dados:")
    df_validacao.printSchema()
    
    print("\nÚltimos registros inseridos:")
    display(df_validacao.orderBy(col("bronze_load_timestamp").desc()).limit(5))
    
except Exception as e:
    print(f"Tabela ainda não existe: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Função de Monitoramento

# COMMAND ----------

def monitorar_bronze_layer():
    """
    Função para monitorar a qualidade e volume da Bronze Layer
    """
    try:
        df = spark.read.format("delta").load(f"{bronze_path}/vendas")
        
        # Métricas básicas
        total_registros = df.count()
        registros_hoje = df.filter(col("bronze_load_timestamp").cast("date") == current_date()).count()
        
        print("=" * 50)
        print("📊 MONITORAMENTO BRONZE LAYER")
        print("=" * 50)
        print(f"Total de registros: {total_registros}")
        print(f"Registros carregados hoje: {registros_hoje}")
        print(f"Última atualização: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)
        
        # Verificação de qualidade
        nulos = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
        print("\n🔍 Verificação de valores nulos:")
        display(nulos)
        
    except Exception as e:
        print(f"Erro no monitoramento: {str(e)}")

# Executando monitoramento
monitorar_bronze_layer()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumo e Próximos Passos
# MAGIC
# MAGIC ### ✅ O que foi feito:
# MAGIC - Ingestão de dados brutos com metadados de controle
# MAGIC - Salvamento em formato Delta particionado
# MAGIC - Configurações de otimização do Spark
# MAGIC - Monitoramento básico de qualidade
# MAGIC
# MAGIC ### 🚀 Próximos passos:
# MAGIC - Executar notebook **02_Silver_Layer_Transformation**
# MAGIC - Aplicar transformações e limpeza dos dados
# MAGIC - Criar estruturas dimensionais
# MAGIC
# MAGIC ### 📋 Boas Práticas Aplicadas:
# MAGIC - **Formato Delta**: Melhor performance e ACID transactions
# MAGIC - **Particionamento**: Por data para otimizar queries
# MAGIC - **Metadados**: Rastreabilidade e auditoria
# MAGIC - **Tratamento de erros**: Try/catch em operações críticas
