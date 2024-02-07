from pyspark.sql.functions import col, countDistinct

def verificar_valores_distintos_por_data(df, coluna_data, coluna_informacao):
    # Agrupa os dados pelo coluna_data e conta os valores distintos na coluna_informacao
    df_agrupado = df.groupBy(coluna_data).agg(countDistinct(coluna_informacao).alias('num_valores_distintos'))
    
    # Verifica se há pelo menos um valor distinto para cada data
    resultados = df_agrupado.filter(col('num_valores_distintos') > 1)
    
    # Retorna True se houver pelo menos uma data com mais de um valor distinto
    return resultados.count() > 0

# Exemplo de uso:
# Suponha que 'df' seja o DataFrame Spark e 'data' seja o nome da coluna de datas, e 'informacao' seja o nome da coluna de informações
# Verifica se há pelo menos um valor distinto por data
resultado = verificar_valores_distintos_por_data(df, 'data', 'informacao')

if resultado:
    print("Existem datas com valores distintos de informação.")
else:
    print("Todos os valores de informação para cada data são iguais.")