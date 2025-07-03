from functools import reduce
from pyspark.sql.functions import col, trim, size, array_contains, when, filter


def checkNullRows(df): 
    null_rows = df.filter(
    reduce(lambda a, b: a | b, [col(c).isNull() for c in df.columns])
    )

    print("Rows with at least one null:", null_rows.count())
    null_rows.show()



def checkEmptyRows(df):
    empty_rows = df.filter(
    reduce(lambda a, b: a & b, [col(c).isNull() for c in df.columns])
    )

    print("Rows with all null:", empty_rows.count())
    empty_rows.show()





def checkSuspiciousStrings(df):
    suspicious = {"", "N/A", "n/a", "null", "unknown", "?"}

    def is_suspicious_col(c):
        dtype = dict(df.dtypes)[c]
        
        if dtype.startswith("array"):
            return reduce(
                lambda a, b: a | b,
                [array_contains(col(c), val) for val in suspicious]
            )
        elif dtype == "string":
            return trim(col(c)).isin([s.strip() for s in suspicious])
        else:
            return None  # Skip non-string, non-array columns

    conditions = [cond for cond in [is_suspicious_col(c) for c in df.columns] if cond is not None]

    if not conditions:
        print("No string or array<string> columns to check.")
        return df.limit(0)

    bad_rows = df.filter(reduce(lambda a, b: a | b, conditions))

    bad_rows.show()
    print("Rows with suspicious strings only:", bad_rows.count())

    return bad_rows



def checkDuplicates(df): 
    total_rows = df.count()
    distinct_rows = df.distinct().count()

    print(f"Total Rows: {total_rows}")
    print(f"Distinct Rows: {distinct_rows}")
    print(f"Duplicate Rows: {total_rows - distinct_rows}")

    duplicates_df = df.groupBy(df.columns).count().filter("count > 1")
    duplicates_df.show()
    return duplicates_df



def check_duplicates_by(df, cols, label):
    dupes = df.groupBy(cols).count().filter("count > 1")
    print(f"Duplicates in {label} by {cols}: {dupes.count()}")
    dupes.show(truncate=False)