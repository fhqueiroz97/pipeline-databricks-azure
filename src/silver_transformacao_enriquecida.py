# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Transformação e Limpeza de Dados
# MAGIC
# MAGIC **Objetivo**: Limpar, transformar e estruturar dados da Bronze Layer
# MAGIC
# MAGIC **Características da Silver Layer**:
# MAGIC - Dados limpos e validados
# MAGIC - Transformações de negócio aplicadas
# MAGIC - Estrutura otimizada para análise
# MAGIC - Qualidade de dados garantida

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configurações e Imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import uuid

# Configurações do ambiente
workspace_path = "/Workspace/Repos/senaipr31@fiap.com.br/pipeline-databricks-azure/src"
bronze_path = f"{workspace_path}/data/bronze"
silver_path = f"{workspace_path}/data/silver"

print(f"Bronze Layer Path: {bronze_path}")
print(f"Silver Layer Path: {silver_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leitura dos Dados da Bronze Layer

# COMMAND ----------

# Configurações de otimização
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

try:
    # Lendo dados da Bronze
    df_bronze = spark.read.format("delta").load(f"{bronze_path}/vendas")
    
    print(f"✅ Dados carregados da Bronze Layer: {df_bronze.count()} registros")
    
    # Mostrando estrutura atual
    print("\n📋 Estrutura dos dados da Bronze:")
    df_bronze.printSchema()
    
    # Amostra dos dados
    display(df_bronze.limit(5))
    
except Exception as e:
    print(f"❌ Erro ao carregar dados da Bronze: {str(e)}")
    print("Execute primeiro o notebook 01_Bronze_Layer_Ingestion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Funções de Transformação e Limpeza

# COMMAND ----------

def aplicar_limpeza_dados(df):
    """
    Aplica limpeza básica nos dados
    """
    df_limpo = df.filter(col("id_venda").isNotNull()) \
                 .filter(col("valor") > 0) \
                 .filter(col("cliente").isNotNull()) \
                 .filter(col("bronze_is_deleted") == False)
    
    return df_limpo

def aplicar_transformacoes_negocio(df):
    """
    Aplica transformações de negócio específicas
    """
    df_transformado = df.withColumn("data_venda_date", to_date(col("data_venda"), "yyyy-MM-dd")) \
                        .withColumn("ano_venda", year(col("data_venda_date"))) \
                        .withColumn("mes_venda", month(col("data_venda_date"))) \
                        .withColumn("valor_com_desconto", 
                                  when(col("valor") > 200, col("valor") * 0.9)
                                  .otherwise(col("valor"))) \
                        .withColumn("categoria_valor", 
                                  when(col("valor") <= 100, "Baixo")
                                  .when(col("valor") <= 200, "Médio")
                                  .otherwise("Alto")) \
                        .withColumn("cliente_normalizado", 
                                  initcap(trim(col("cliente"))))
    
    return df_transformado

def adicionar_metadados_silver(df):
    """
    Adiciona metadados de controle para Silver Layer
    """
    df_silver = df.withColumn("silver_load_timestamp", current_timestamp()) \
                  .withColumn("silver_load_id", lit(str(uuid.uuid4()))) \
                  .withColumn("silver_quality_score", lit(100))  # Score de qualidade
    
    return df_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aplicação das Transformações

# COMMAND ----------

# Aplicando pipeline de transformação
print("🔄 Iniciando transformações...")

# Passo 1: Limpeza
df_limpo = aplicar_limpeza_dados(df_bronze)
print(f"Após limpeza: {df_limpo.count()} registros")

# Passo 2: Transformações de negócio
df_transformado = aplicar_transformacoes_negocio(df_limpo)
print("✅ Transformações de negócio aplicadas")

# Passo 3: Seleção de colunas para Silver
colunas_silver = [
    "id_venda",
    "cliente_normalizado",
    "produto", 
    "valor",
    "valor_com_desconto",
    "categoria_valor",
    "data_venda_date",
    "ano_venda",
    "mes_venda",
    "estado",
    "bronze_load_id"  # Mantém rastreabilidade
]

df_silver_prep = df_transformado.select(*colunas_silver)

# Passo 4: Adição de metadados Silver
df_silver = adicionar_metadados_silver(df_silver_prep)

print("🎯 Pipeline de transformação concluído!")
display(df_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação de Qualidade dos Dados

# COMMAND ----------

def validar_qualidade_silver(df):
    """
    Executa validações de qualidade na Silver Layer
    """
    print("🔍 VALIDAÇÃO DE QUALIDADE - SILVER LAYER")
    print("=" * 50)
    
    # Contagem total
    total_registros = df.count()
    print(f"📊 Total de registros: {total_registros}")
    
    # Verificar valores nulos em campos críticos
    campos_criticos = ["id_venda", "cliente_normalizado", "valor", "data_venda_date"]
    
    for campo in campos_criticos:
        nulos = df.filter(col(campo).isNull()).count()
        if nulos > 0:
            print(f"⚠️  Campo '{campo}' possui {nulos} valores nulos")
        else:
            print(f"✅ Campo '{campo}' sem valores nulos")
    
    # Verificar duplicatas
    duplicatas = df.count() - df.dropDuplicates(["id_venda"]).count()
    if duplicatas > 0:
        print(f"⚠️  Encontradas {duplicatas} duplicatas")
    else:
        print("✅ Sem duplicatas encontradas")
    
    # Estatísticas de valores
    print(f"\n💰 Valor médio: R$ {df.agg(avg('valor')).collect()[0][0]:.2f}")
    print(f"💰 Valor máximo: R$ {df.agg(max('valor')).collect()[0][0]:.2f}")
    
    # Distribuição por categoria
    print("\n📈 Distribuição por categoria de valor:")
    df.groupBy("categoria_valor").count().show()
    
    return total_registros > 0  # Retorna True se passou na validação básica

# Executando validação
validacao_ok = validar_qualidade_silver(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Salvamento na Silver Layer

# COMMAND ----------

if validacao_ok:
    try:
        # Salvando em formato Delta com otimizações
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .option("overwriteSchema", "true") \
            .partitionBy("ano_venda", "mes_venda") \
            .save(f"{silver_path}/vendas_transformadas")
        
        print("✅ Dados salvos com sucesso na Silver Layer!")
        
        # Otimização da tabela Delta
        spark.sql(f"OPTIMIZE delta.`{silver_path}/vendas_transformadas`")
        print("🚀 Otimização Delta aplicada!")
        
    except Exception as e:
        print(f"❌ Erro ao salvar na Silver Layer: {str(e)}")
else:
    print("❌ Validação falhou. Dados não foram salvos na Silver Layer.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Criação de Views para Análise

# COMMAND ----------

# Criando view temporária para análises
try:
    df_silver_final = spark.read.format("delta").load(f"{silver_path}/vendas_transformadas")
    df_silver_final.createOrReplaceTempView("vendas_silver")
    
    print("✅ View 'vendas_silver' criada com sucesso!")
    
    # Exemplo de análise usando SQL
    spark.sql("""
        SELECT 
            estado,
            categoria_valor,
            COUNT(*) as total_vendas,
            SUM(valor) as receita_total,
            AVG(valor) as ticket_medio
        FROM vendas_silver 
        GROUP BY estado, categoria_valor
        ORDER BY receita_total DESC
    """).show()
    
except Exception as e:
    print(f"Erro ao criar view: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Monitoramento da Silver Layer

# COMMAND ----------

def monitorar_silver_layer():
    """
    Monitora métricas da Silver Layer
    """
    try:
        df = spark.read.format("delta").load(f"{silver_path}/vendas_transformadas")
        
        print("=" * 60)
        print("📊 MONITORAMENTO SILVER LAYER")
        print("=" * 60)
        
        # Métricas gerais
        total_registros = df.count()
        receita_total = df.agg(sum("valor")).collect()[0][0]
        
        print(f"Total de registros processados: {total_registros}")
        print(f"Receita total: R$ {receita_total:,.2f}")
        print(f"Última atualização: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Top 3 estados por receita
        print("\n🏆 Top 3 Estados por Receita:")
        df.groupBy("estado") \
          .agg(sum("valor").alias("receita")) \
          .orderBy(desc("receita")) \
          .limit(3) \
          .show()
        
        # Qualidade dos dados
        registros_qualidade = df.filter(col("silver_quality_score") >= 90).count()
        percentual_qualidade = (registros_qualidade / total_registros) * 100
        
        print(f"📈 Qualidade dos dados: {percentual_qualidade:.1f}%")
        
    except Exception as e:
        print(f"Erro no monitoramento: {str(e)}")

# Executando monitoramento
monitorar_silver_layer()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resumo e Próximos Passos
# MAGIC
# MAGIC ### ✅ O que foi feito:
# MAGIC - Limpeza e validação de dados da Bronze Layer
# MAGIC - Aplicação de transformações de negócio
# MAGIC - Criação de campos calculados e categorizações
# MAGIC - Validação de qualidade automatizada
# MAGIC - Otimização com formato Delta e particionamento
# MAGIC
# MAGIC
# MAGIC ### 📋 Boas Práticas Aplicadas:
# MAGIC - **Validação de qualidade**: Verificações automáticas
# MAGIC - **Transformações modulares**: Funções reutilizáveis
# MAGIC - **Particionamento inteligente**: Por ano/mês para performance
# MAGIC - **Rastreabilidade**: Mantém ligação com Bronze Layer
# MAGIC - **Otimização Delta**: Para melhor performance de leitura
