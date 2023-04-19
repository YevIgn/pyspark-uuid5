import time
import uuid
from pathlib import Path
from typing import Iterable

import pandas as pd
import pyarrow
from pyspark.sql import SparkSession, functions as f, DataFrame
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import StringType, StructField, StructType

JSON_PATH = str((Path.cwd() / "string_data" / "100000_rows_with_strings.json").as_posix())
spark = SparkSession.builder.getOrCreate()


class Timer:
    def __enter__(self):
        self.start = time.time_ns()
        return self

    def __exit__(self, *args):
        self.end = time.time_ns()
        self.interval = (self.end - self.start) / 1000 / 1000


def uuid5_namespace(ns: str | uuid.UUID | None) -> uuid.UUID:
    """Helper function used to provide a UUID5 hashed namespace based on the passed str

    Parameters
    ----------
    ns: str | uuid.UUID | None
        A str, an empty string (or None), or an existing UUID can be passed

    Returns
    -------
    uuid.UUID
    """

    # if we already have a UUID, we just return it
    if isinstance(ns, uuid.UUID):
        return ns

    # if ns is empty or none, we simply return the default NAMESPACE_DNS
    if not ns:
        ns = uuid.NAMESPACE_DNS
        return ns

    # else we hash the string against the NAMESPACE_DNS
    ns = uuid.uuid5(uuid.NAMESPACE_DNS, ns)
    return ns


def hash_uuid5(
    input_value: str,
    namespace: str | uuid.UUID | None = "",
    extra_string: str | None = "",
):
    """Pure python implementation of UUID5 with additional hashing over.

    See: https://docs.python.org/3/library/uuid.html#uuid.uuid5

    Parameters
    ----------
    input_value: str
        value that will be hashed
    namespace: str | uuid.UUID | None
    extra_string: str | None
        optional extra string that will be prepended to the input_value

    Returns
    -------
    str:
        uuid.UUID (uuid5) cast to string
    """

    if not isinstance(namespace, uuid.UUID):
        hashed_namespace = uuid5_namespace(namespace)
    else:
        hashed_namespace = namespace
    return str(uuid.uuid5(hashed_namespace, (extra_string + input_value)))


def uuid_hash_uuid5_pandas_udf(
    df: DataFrame,
    source_columns: list[str],
    target_column: str,
    delimiter: str = "|",
    namespace: str = "domain.com",
    extra_string: str = "",
) -> DataFrame:
    """
    This function allows you to generate a UUID.

    Prerequisites: this function has no side-effects. But be aware that in most cases,
    the expectation is that you dataset is clean (trimmed of leading and trailing spaces)

     Args:
        df:
            The Spark DataFrame to hash
        source_columns:
            Columns that will be hashed in alphabetical order, ascending.
        target_column:
            The newly added column name containing the result of the hashing
        delimiter:
            The delimiter used to.
        namespace:
            Namespace DNS.
        extra_string:
            In case of collisions one can pass an extra string to hash on.
            Only use when required and aligned on within your domain.

    Returns:
        DataFrame: The DataFrame with the hashed columns.
    """

    source_columns = sorted([f"`{s}`" for s in source_columns])
    ns = uuid.uuid5(uuid.NAMESPACE_DNS, namespace)

    @pandas_udf("string")
    def uuid5_hash_pandas_series(s: pd.Series) -> pd.Series:
        return s.transform(lambda g: hash_uuid5(g, namespace=ns, extra_string=extra_string))

    return df.withColumn(
        target_column,
        uuid5_hash_pandas_series(f.concat_ws(delimiter, *source_columns)),
    )


def uuid_hash_uuid5_arrow_udf(
    df: DataFrame,
    source_columns: list[str],
    target_column: str,
    delimiter: str = "|",
    namespace: str = "domain.com",
    extra_string: str = "",
) -> DataFrame:
    source_columns = sorted(source_columns)

    def add_target_column(batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
        schema = batch.schema
        col_indices = [schema.get_field_index(col) for col in source_columns]
        if any(index < 0 for index in col_indices):
            raise ValueError("One or more source columns not found in schema")
        col_arrays = [pyarrow.compute.cast(batch.column(index), pyarrow.string()) for index in col_indices]
        combined_array = pyarrow.compute.binary_join_element_wise(*(col_arrays + [delimiter]))
        schema = schema.append(pyarrow.field(target_column, pyarrow.string()))
        arrays = batch.columns + [combined_array]
        return pyarrow.RecordBatch.from_arrays(arrays, schema=schema)

    def add_uuid5_column(iterator: Iterable[pyarrow.RecordBatch]) -> Iterable[pyarrow.RecordBatch]:
        ns = uuid.uuid5(uuid.NAMESPACE_DNS, namespace)
        for batch in iterator:
            new_batch = add_target_column(batch)
            tgt_column_index = new_batch.schema.get_field_index(target_column)
            tgt_column = new_batch.column(tgt_column_index)
            uuid_array = pyarrow.array(
                hash_uuid5(value.as_py(), namespace=ns, extra_string=extra_string) for value in tgt_column
            )
            arrays = new_batch.columns[:-1] + [uuid_array]
            new_batch = pyarrow.RecordBatch.from_arrays(arrays, schema=new_batch.schema)
            yield new_batch

    output_fields = df.schema.fields.copy()
    output_fields.append(StructField(target_column, StringType(), True))
    output_schema = StructType(output_fields)
    return df.mapInArrow(add_uuid5_column, output_schema)


def uuid5_pyspark(
    df: DataFrame,
    source_columns: list[str],
    target_column: str,
    delimiter: str = "|",
    namespace: str = "domain.com",
    extra_string: str = "",
) -> DataFrame:
    ns = f.lit(uuid.uuid5(uuid.NAMESPACE_DNS, namespace).bytes)
    cols_to_hash = f.concat_ws(delimiter, *source_columns)
    cols_to_hash = f.concat(f.lit(extra_string), cols_to_hash)
    cols_to_hash = f.encode(cols_to_hash, "utf-8")
    cols_to_hash = f.concat(ns, cols_to_hash)
    source_columns_sha1 = f.sha1(cols_to_hash)
    target_col_uuid = f.concat_ws(
        "-",
        f.substring(source_columns_sha1, 1, 8),
        f.substring(source_columns_sha1, 9, 4),
        f.concat(f.lit("5"), f.substring(source_columns_sha1, 14, 3)),  # Set version.
        f.concat(f.substring(source_columns_sha1, 17, 4)),  # ToDo: Variant isn't set correctly.
        f.substring(source_columns_sha1, 21, 12),
    )
    return df.withColumn(target_column, target_col_uuid)


input_df = spark.read.json(JSON_PATH)
kwargs = dict(target_column="c4", source_columns=["c1", "c2", "c3"], namespace="scratch")


def func_spark_udf():
    ns = kwargs.get("namespace")
    target_column = kwargs.get("target_column")
    extra_string = kwargs.get("extra_string", "")
    delimiter = kwargs.get("delimiter", "|")
    source_columns = kwargs.get("source_columns")

    uuid5_udf = f.udf(
        lambda hash_string: str(uuid.uuid5(uuid5_namespace(ns), (extra_string + hash_string))),
        returnType=StringType(),
    )

    df = input_df.withColumn(
        target_column,
        uuid5_udf(f.concat_ws(delimiter, *source_columns)),
    )
    df.collect()  # forces an action


def func_pandas_udf():
    df = uuid_hash_uuid5_pandas_udf(df=input_df, **kwargs)
    df.collect()  # forces an action


def func_arrow_udf():
    df = uuid_hash_uuid5_arrow_udf(df=input_df, **kwargs)
    df.collect()  # forces an action


def func_pure_pyspark():
    df = uuid5_pyspark(df=input_df, **kwargs)
    df.collect()  # forces an action


from timeit import timeit

result_pure_pyspark = timeit(func_pure_pyspark, number=100)

result_spark_udf = timeit(func_spark_udf, number=100)

result_pandas_udf = timeit(func_pandas_udf, number=100)

result_arrow_udf = timeit(func_arrow_udf, number=100)


print(
    f"""
Ran four tests, each calling the respective function 100 times on 10,000 rows of data containing 3 columns of strings.
> note: lower numbers are better

0. Pure PySpark implementation as written with the help of GhatGPT (free version)
{result_pure_pyspark=}

1. Using spark Python UDF
{result_spark_udf=} 

2. Using Pandas UDF
{result_pandas_udf=}

3. Using Arrow UDF as written with the help of GhatGPT (free version)
{result_arrow_udf=}

"""
)
