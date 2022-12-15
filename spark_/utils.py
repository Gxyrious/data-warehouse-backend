def get_spark_session():
    from pyspark.sql import SparkSession
    spark_session = (SparkSession
                     .builder
                     .master("local")
                     # .master("spark://175.24.152.204:7077")
                     .appName('spark_hive')
                     .config("hive.metastore.uris", "thrift://localhost:9083")
                     # .config("hive.metastore.uris", "thrift://175.24.152.204:9083")
                     .enableHiveSupport()
                     .getOrCreate()
                     )
    spark_session.sql('use dw_movie')
    spark_session.sql('show tables').show()
    return spark_session