from pyspark.sql import DataFrame, functions as F

def compute(edges: DataFrame) -> DataFrame:

    # Build the initial roots DataFrame with path and depth
    roots = (edges.select("parent").distinct()
      .join(edges.select("child").distinct(),
            F.col("parent") == F.col("child"),
            "left_anti")
      .withColumnRenamed("parent", "node")
      .withColumn("path", F.col("node"))
      .withColumn("depth", F.lit(0).cast("long")))
    
    # finding the root nodes. Any nodes that doesnt have parent is root.
    roots_nodes = set(r["node"] for r in roots.collect())

    # finding the leaf nodes. Any node that doesnt have child is leaf.
    leaves = (
        edges.select("child").distinct()
        .join(edges.select("parent").distinct(),
                F.col("child") == F.col("parent"),
                "left_anti")
        .withColumnRenamed("child", "leaf")
    )
    leaf_nodes = set(r["leaf"] for r in leaves.collect())

    result = roots
    current_data = roots

    max_depth = 10  # As mentioned in the readme file, we can assume max depth is 10. If not we have to loop until no new nodes are found.
    for depth in range(max_depth):
        next_level = (
            current_data.join(edges, current_data.node == edges.parent, "inner")
                    .select(
                        edges.child.alias("node"),
                        F.concat_ws(".", current_data.path, edges.child).alias("path"),
                        (current_data.depth.cast("long") + F.lit(1)).alias("depth")
                    )
        )
        if next_level.count() == 0:
            break
        result = result.union(next_level)
        current_data = next_level
        

    # Finding descendants for each node and appending to the dataframe. 
    # Descendants are calculated by checking if the path of one node starts with the path of another node.
    # If it does, then the former is a descendant of the latter.
    descendants = (
        result.alias("a")
        .join(result.alias("b"), F.col("b.path").startswith(F.concat(F.col("a.path"), F.lit("."))), "left")
        .groupBy("a.node", "a.path", "a.depth")
        .agg(F.count("b.node").alias("descendants"))
    )

    # Adding is_root and is_leaf columns to the dataframe
    final_result = (
        descendants
        .withColumn("is_root", F.col("node").isin(roots_nodes))
        .withColumn("is_leaf", F.col("node").isin(leaf_nodes))
        .orderBy("depth")
    )
    return final_result.select("node", "path", "depth", "descendants", "is_root", "is_leaf")