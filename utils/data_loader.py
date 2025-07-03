def load_csv(spark, path, schema=None, header=True, inferSchema=False):
    return spark.read.csv(path, schema=schema, header=header, inferSchema=inferSchema)


def read_json(spark, path, multiline=True, mode="PERMISSIVE", allow_comments=False, 
              allow_single_quotes=False, schema=None):
    
    reader = spark.read.option("multiLine", str(multiline)) \
                       .option("mode", mode) \
                       .option("allowComments", str(allow_comments)) \
                       .option("allowSingleQuotes", str(allow_single_quotes))
    if schema:
        reader = reader.schema(schema)
    return reader.json(path)

def load_delta_table(spark, table_name):
    return spark.read.table(table_name)