from pyspark.sql import functions as sf
from pyspark.testing import assertDataFrameEqual
import os

from assessment.helpers import get_spark
from assessment.transform import compute

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    print(os.path.join(current_dir, "data", "edges.jsonl"))

    spark = spark = get_spark()

    edges = spark.read.json(
        os.path.join(current_dir, "data", "edges.jsonl"),
        """
        parent STRING,
        child STRING
        """,
    )
    
    output = compute(edges)

    output.write.json(os.path.join(current_dir, "data", "output.jsonl"))
    assertDataFrameEqual(
        output,
        spark.read.json(
            os.path.join(current_dir, "data", "solution.jsonl"),
            """
            node STRING,
            path STRING,
            depth LONG,
            descendants LONG,
            is_root BOOLEAN,
            is_leaf BOOLEAN
            """,
        ),
        ignoreNullable=True,
    )

    print(f"Success!")
