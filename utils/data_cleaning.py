from pyspark.sql.functions import trim, length, col, when

def get_null_or_empty_rows(): 

    check = ["NULL", "NaN", "", [], {}]

    null_counts = df.select(
    [count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in df.columns]
    )
    null_counts.show()




def encode_column(df, column, mapping):
    expr = None
    for rating, encoding in mapping.items():
        condition = (col(column) == rating)
        expr = when(condition, encoding) if expr is None else expr.when(condition, encoding)
    expr = expr.otherwise(None)
    return df.withColumn(column, expr)








