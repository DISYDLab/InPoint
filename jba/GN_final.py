# Community Detection in Proximity Graph
import copy

from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as F
import time
import sys
from collections import defaultdict


from pyspark.sql import types as T
from pyspark.sql import functions as F
from pathlib import Path

import graphframes
from graphframes import *

def set_dict_value(dictionary, key, value):
    dictionary[key] = value


def bfs(root, original_graph):
    parent = defaultdict(list)
    depth = defaultdict(int)
    shortest_path_count = defaultdict(float)
    credit = defaultdict(float)
    queue = []
    bfs_queue = []

    set_dict_value(parent, root, None)
    set_dict_value(depth, root, 0)
    set_dict_value(shortest_path_count, root, 1)
    bfs_queue.append(root)

    for c in original_graph[root]:
        set_dict_value(parent, c, [root])
        set_dict_value(depth, c, 1)
        set_dict_value(shortest_path_count, c, 1)
        queue.append(c)
        bfs_queue.append(c)

    while queue:
        node = queue.pop(0)
        set_dict_value(credit, node, 1)
        paths = 0
        for p in parent[node]:
            paths = paths + shortest_path_count[p]
        set_dict_value(shortest_path_count, node, paths)
        for neighbour in original_graph[node]:
            if neighbour not in bfs_queue:
                set_dict_value(parent, neighbour, [node])
                set_dict_value(depth, neighbour, depth[node] + 1)
                queue.append(neighbour)
                bfs_queue.append(neighbour)
            else:
                # case when one node has multiple parents
                if depth[neighbour] == depth[node] + 1:
                    parent[neighbour].append(node)

    bfs_queue.reverse()
    for child in bfs_queue[:-1]:
        for parnt in parent[child]:
            score = credit[child] * (shortest_path_count[parnt] / shortest_path_count[child])
            credit[parnt] += score
            yield (tuple(sorted([child, parnt])), score)


def get_communities(vertices):
    communities = []
    queue = []
    visited_nodes = []
    for vertex in vertices:
        if vertex not in visited_nodes:
            visited = [vertex]
            queue.append(vertex)
            while queue:
                node = queue.pop(0)
                for neighbour in original_graph[node]:
                    if neighbour not in visited:
                        visited.append(neighbour)
                        queue.append(neighbour)
            visited.sort()
            visited_nodes.extend(visited)
            communities.append(visited)
    return communities


def get_modularity(communities):
    modularity = 0.0
    denominator = 2 * m
    for community in communities:
        for i in community:
            for j in community:
                actual = 1 if j in original_graph[i] else 0
                expected = (len(original_graph[i]) * len(original_graph[i])) / denominator
                modularity += (actual - expected)
    return modularity / denominator


def cut_graph(edges):
    for edge in edges:
        original_graph[edge[0]].remove(edge[1])
        original_graph[edge[1]].remove(edge[0])


# Create SparkSession Instance
from pyspark.sql import SparkSession
# appName('FunWithGraphs')

spark = SparkSession.builder.appName('Proximity_Graph_Communities_Detection').getOrCreate()
sc=spark.sparkContext

# Creating GraphFrames

# You can create GraphFrames from vertex and edge DataFrames.

# Vertex DataFrame: A vertex DataFrame should contain a special column named id which specifies unique IDs for each vertex in the graph.

# Edge DataFrame: An edge DataFrame should contain two special columns: src (source vertex ID of edge) and dst (destination vertex ID of edge).

# Both DataFrames can have arbitrary other columns. Those columns can represent vertex and edge attributes.`

# Reading from Neo4j
# ===================================================
# 1. Node

#     Remember to:
#     1. Start inpointTestDB Neo4j DataBase
#     2. Open Neo4j Browser

# Single label
node_labels = "User7" # define single label

# Multiple labels:
# labels = "Person:Customer:Confirmed" # Define multiple labels example

# You can read nodes by specifiying a single label, or multiple labels.
# Reading all the nodes of type labels from your Neo4j instance:

# Columns
# When reading data with this method, the Dataframe will contain all the fields 
# contained in the nodes, plus 2 additional columns.
nodes_DF = (spark.read.format("org.neo4j.spark.DataSource")
        .option("url", "bolt://localhost:7687")
        .option("authentication.type", "basic")
        .option("authentication.basic.username", "neo4j")
        .option("authentication.basic.password", "1234")
        .option('database', 'neo4j')
        .option("labels", node_labels)
        .load()
        )


nodes_DF.show()
nodes_DF_columns = nodes_DF.columns
nodes_DF_columns = [node for node in nodes_DF_columns if node != 'community_id' ]
print(nodes_DF_columns)
#  Rename Spark DataFrame Columns
# withColumnRenamed(existingName, newName)
nodes_DF = nodes_DF.select(nodes_DF_columns)
nodes_DF = nodes_DF.withColumnRenamed('<id>', 'id').withColumnRenamed('<labels>', 'labels').withColumnRenamed('SENTOUT', 'sentiment')


nodes_DF.show(2)

# Relationship

# To read a relationship you must specify the relationship name, the source node labels, and the target node labels.
edges_DF = (spark.read.format("org.neo4j.spark.DataSource")
    .option("url", "bolt://localhost:7687")
    .option("authentication.type", "basic")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", "1234")
    .option('database', 'neo4j')
    .option("relationship", "Followed_by")
    .option("relationship.source.labels", node_labels)
    .option("relationship.target.labels", node_labels)
    .load()
    )

edges_DF_columns = edges_DF.columns
edges_DF = edges_DF.withColumnRenamed('<source.id>', 'src').withColumnRenamed('<target.id>', 'dst').withColumnRenamed('<rel.type>', 'relationship')

edges_DF=edges_DF.select('src','dst','relationship')

# Undirected Graph Creation

# Newman's modularity works for undirected weighted networks.

# Loading the nodes is easy, but for the relationships we need to do a little preprocessing so that we can create each relationship twice


# # Nodes File
# nodes_filename='transport-nodes.csv'

# # Relationship File
# relationships_filename = 'transport-relationships.csv'

# # Return a new path pointing to the current working directory (as returned by os.getcwd()).
# directory_path = Path.cwd()

# #  Return the path as a 'file' URI.
# full_path_to_nodes_file = (directory_path / nodes_filename).as_uri()
# full_path_to_relationships_file = (directory_path / relationships_filename).as_uri()


# node_fields = [
#     T.StructField("id", T.StringType(), True),
#     T.StructField("latitude", T.FloatType(), True),
#     T.StructField("longitude", T.FloatType(), True),
#     T.StructField("population", T.IntegerType(), True)
# ]

def create_proximity_graph(nodes_DF, edges_DF):

#     print('nodes:')
#     print(nodes_DF.count())

#     print('edges_DF:')
#     print(edges_DF.count())

    reversed_edges_DF = (edges_DF.withColumn("newSrc", edges_DF.dst)
        .withColumn("newDst", edges_DF.src)
        .drop("dst", "src")
        .withColumnRenamed("newSrc", "src")
        .withColumnRenamed("newDst", "dst")
        .select("src", "dst", "relationship"))

#     print('reversed_edges_DF:')
#     print(reversed_edges_DF.count())

    undirected_edges_DF = edges_DF.union(reversed_edges_DF)
#     print('undirected_edges:')
#     print(undirected_edges_DF.count())

    return GraphFrame(nodes_DF, undirected_edges_DF.distinct())

u_graph = create_proximity_graph(nodes_DF,edges_DF)

u_graph.vertices.show()
u_graph.edges.show()

# Write to and Read from parquet files
# Because of a disfunction of Spatk neo4j connector as of this moment written this code, 
# we use a workaround - We write and read
# Dataframes read from Neo4j in order to be functional.

# u_graph.vertices write to multiple parquet files / Read from parquet files
u_graph.vertices.write.mode(saveMode='overwrite').parquet("spark_connector_examples/u_graph_vertices.parquet")

u_graph_vertices_parquet = spark.read.parquet('spark_connector_examples/u_graph_vertices.parquet')
u_graph_vertices_parquet.show(2)

# u_graph.edges write to multiple parquet files / Read from parquet files
u_graph.edges.write.mode(saveMode='overwrite').parquet("spark_connector_examples/u_graph_edges.parquet")
u_graph_edges_parquet = spark.read.parquet('spark_connector_examples/u_graph_edges.parquet')

# Create GraphFrame
# g=GraphFrame(vertices , edges)
u_graph_from_parquet = GraphFrame(u_graph_vertices_parquet, u_graph_edges_parquet)

u_graph_from_parquet.edges.sort('src', 'dst').show()

# total remaining edges
m = u_graph_from_parquet.edges.count()
print(f"m = {m}")

# Get arguments (graph_file, betweenness_output_file, community_output_file) from spark-submit
modular_community = []
best_modularity = -1

original_graph = u_graph_from_parquet.edges.rdd.flatMap(lambda x: [(x[0], [x[1]]), (x[1], [x[0]])]).reduceByKey(lambda x, y: x + y).collectAsMap()
# type(original_graph)

# Vertices
vertices = sorted(original_graph.keys())

# Finding Communities
rem_edges = m
modularity_list=[]
print('rem_edges: ', rem_edges)
while rem_edges > 0:
    communities = get_communities(vertices)
    modularity = get_modularity(communities)
    modularity_list.append(modularity)
    
    if modularity > best_modularity:
        best_modularity = modularity
        modular_community = copy.deepcopy(communities)
    min_cut = sc.parallelize(vertices).flatMap(lambda x: bfs(x, original_graph)).reduceByKey(lambda x, y: x + y) \
        .map(lambda x: (x[1]/2, [x[0]])).reduceByKey(lambda x, y: x+y).sortBy(lambda x: (-x[0])).map(lambda x: x[1]).first()
    cut_graph(min_cut)
    rem_edges -= len(min_cut)
    print('rem_edges: ', rem_edges)
# comunities = sc.parallelize(modular_community).sortBy(lambda x: (len(x), x)).collect()
# comunities = sc.parallelize(modular_community).sortBy(lambda x: (len(x), x)).toDF()

print(best_modularity)
# comunities_list2 = sc.parallelize(modular_community).collect()
comunities_list = sc.parallelize(modular_community).sortBy(lambda x: (len(x), x)).collect()
comunities_list
print(f"communities: {len(comunities_list)}")

col = ['community_id', 'id']
test = [ [community_id, comunities_list[community_id]] for community_id in range(len(comunities_list))]
test_DF = spark.createDataFrame(test, schema=col)

test_DF_exploded = test_DF.select(F.explode("id").alias("id"), test_DF.community_id).sort('id')

# test_DF_exploded.show()
# test_DF_exploded.count()
u_graph_from_parquet_vertices = u_graph_from_parquet.vertices.join(test_DF_exploded,["id"]).sort('id')
u_graph_from_parquet_vertices_columns = nodes_DF_columns + ['community_id']
# u_graph_from_parquet_vertices_columns
# u_graph_from_parquet_vertices.show()
u_graph_from_parquet_vertices = u_graph_from_parquet_vertices.toDF(*u_graph_from_parquet_vertices_columns)
# u_graph_from_parquet_vertices.show(truncate = False)

u_graph_from_parquet_vertices.write.mode(saveMode='overwrite').parquet("spark_connector_examples/u_graph_final_vertices.parquet")

# u_graph_from_parquet_vertices.printSchema()
# u_graph_from_parquet_vertices = u_graph_from_parquet_vertices.withColumn('<id>', F.col('<id>').cast('string'))
# u_graph_from_parquet_vertices.printSchema()
u_graph_vertices_parquet = spark.read.parquet('spark_connector_examples/u_graph_final_vertices.parquet')
u_graph_from_parquet_vertices = u_graph_from_parquet_vertices.drop('<id>', '<labels>')
# u_graph_from_parquet_vertices.printSchema()

# WRITE Node DataFrame to Neo4j
# Node

# In case you use the option labels the Spark Connector will persist the entire Dataset as nodes. Depending on the SaveMode it will CREATE or MERGE nodes (in the last case using the node.keys properties).

# The nodes will be sent to Neo4j in a batch of rows defined in the batch.size property and we will perform an UNWIND operation under the hood.

# For Node type:
#     SaveMode.ErrorIfExists: this will build a CREATE query
#     SaveMode.Overwrite: this will build a MERGE query

#     For SaveMode.Overwrite mode you need to have unique constrains on the keys.
# ATTENTION: If you are using Spark 3 the default Save Mode ErrorIfExists won’t work.

unique_constraints_node_keys = "myid"
(u_graph_from_parquet_vertices.write.format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("url", "bolt://localhost:7687")
    .option("authentication.type", "basic")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", "1234")
    .option('database', 'neo4j')
    .option("labels", "User7")
    .option("node.keys", unique_constraints_node_keys)
    .save()
  )