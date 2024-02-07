from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as max_

# Inicialize a sessão do Spark
spark = SparkSession.builder \
    .appName("DadoDaOrigem") \
    .getOrCreate()

# Defina os valores para o DataFrame
data = [
    ("2023-12-01", 3),
    ("2024-01-01", 3),
    ("2024-02-01", 3),
    ("2024-01-01", 3),
    ("2024-01-01", 3),
    ("2023-12-01", 4),
    ("2023-12-01", 4),
    ("2023-12-01", 5)
]

# Crie o DataFrame
df = spark.createDataFrame(data, ["CargaDaBase", "FlagValor"])

# Verifica se na data mais recente  há valores distintos para FlagValor
def verificar_atualizacao(df):
    max_date = df.select(max_("CargaDaBase")).collect()[0][0]
    recent_values = df.filter(col("CargaDaBase") == max_date).select("FlagValor").distinct().count()
    if recent_values == 1:
        # Se não houver ,checa a data anterior
        previous_date = df.filter(col("CargaDaBase") < max_date).select(max_("CargaDaBase")).collect()[0][0]
        previous_values = df.filter(col("CargaDaBase") == previous_date).select("FlagValor").distinct().count()
        if previous_values > 1:
            # Se houver valores distintos para a data anterior, crie um novo data frame
            df_ja_atualizado = df.filter(col("CargaDaBase") == previous_date)
            df_ja_atualizado.show()
            return df_ja_atualizado
    return None

# envoke
df_atualizado = verificar_atualizacao(df)
if df_atualizado is None:
    print("Não há necessidade de atualização.")