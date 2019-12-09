from pyspark.sql import HiveContext
from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.functions import count,col

h = HiveContext(sc)
df = h.sql("select * from trabalho.resultado")
df.show()

# Pergunta a
df.count()

# Pergunta b
dfmandantes = df.groupBy("equipe_mandante").count()
dfmandantes.count()

#Pergunta c
dfMandantesVitoriosos = df.filter(df["gol_equipe_mandante"] > df["gol_equipe_visitante"])
dfMandantesVitoriosos.count()

#Pergunta d
dfVisitantesVitoriosos = df.filter(df["gol_equipe_mandante"] < df["gol_equipe_visitante"])
dfVisitantesVitoriosos.count()

#Pergunta e
dfEmpates = df.filter(df["gol_equipe_mandante"] == df["gol_equipe_visitante"])
dfEmpates.count()

#Pergunta f
partidaPais = df.groupBy("pais").count()
partidaPais.write.mode("overwrite").format("parquet").option("compression", "zlib").saveAsTable("trabalho.partida_pais")