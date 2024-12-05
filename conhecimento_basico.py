# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import max,count,col,asc,desc


# Create my_spark
spark = SparkSession.builder.master('local').appName('sparkestudo').getOrCreate()

#print(f'Criado com sucesso {spark}')


df = spark.read.csv('IMDB movies.csv', header=True, inferSchema = True)
#print(df.printSchema())

#print(df.show())

#selecionar colunas
df_movie = df.select('Title', 'Year', 'Director', 'Votes','Genre')
#print(df_movie.show())

#muda tipo das colunas
df_votes = df_movie.withColumn('votos', df_movie['Votes'].cast('int')).drop('votes')


#filtros
df_votes.filter(df_votes.votos > 500)
#print(df_votes.show())

#pegar o max de votos
df_maximo = df_votes.agg(max('votos').alias('max_votos'))
#print(df_maximo.show())

df_maximo_votos = df_votes.filter(df_votes.votos == 1791916)
#print(df_maximo_votos.show())

#Generos dos filmes
df_soma = df_votes.groupBy('Genre').count()


df_soma.orderBy(col('count').desc())
#print(df_soma.show(truncate=False))

#Aplicando In/ 
df_filtrado = df_votes.select('Title','Genre','votos').filter(df_votes.Genre.isin('Thriller'))
#print(df_filtrado.show(truncate=False))

#SQL
df_votes.createOrReplaceTempView('movies')

df_query = spark.sql('select Genre, count(*) as qtd from movies GROUP BY Genre ORDER BY qtd desc')
#print(df_query.show(truncate=False))
