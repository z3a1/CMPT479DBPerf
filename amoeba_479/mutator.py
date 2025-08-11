from mutatation_rules import MUTATE_RULES
# from sqlglot import parse_one, exp
import jpype
import jpype.imports
from jpype.types import *
import os




java_lib_dir = "target/dependency"
dep_dir = "target/dependency"

# jars = [
#     "calcite-core-1.40.0.jar",
#     "calcite-linq4j-1.40.0.jar",
#     "guava-33.3.0-jre.jar",
#     "jackson-annotations-2.15.4.jar",
#     "jackson-core-2.15.4.jar",
#     "jackson-databind-2.15.4.jar",
#     "slf4j-api-1.7.25.jar",
#     "commons-codec-1.16.0.jar",
#     "commons-io-2.15.0.jar",
#     "avatica-core-1.26.0.jar",
#     "slf4j-simple-1.7.25.jar",
#     "jts-core-1.20.0.jar"
# ]
jars = [os.path.join(dep_dir, jar) for jar in os.listdir(dep_dir) if jar.endswith(".jar")]
# classpath = os.pathsep.join(os.path.join(java_lib_dir, jar) for jar in jars)
classpath = os.pathsep.join(jars)

jvm_path = jpype.getDefaultJVMPath()

jpype.startJVM(jvm_path,f"-Djava.class.path={classpath}")

from org.apache.calcite.sql.parser import SqlParser
from org.apache.calcite.plan.hep import HepPlanner, HepProgramBuilder
from org.apache.calcite.rel.rules import *
from org.apache.calcite.tools import Frameworks
from org.apache.calcite.rel.rel2sql import RelToSqlConverter
from org.apache.calcite.rel.rules import CoreRules
from org.apache.calcite.schema import SchemaPlus
from org.apache.calcite.jdbc import CalciteSchema
from org.apache.calcite.schema.impl import AbstractTable
from org.apache.calcite.rel.type import RelDataTypeFactory
from org.apache.calcite.sql.type import SqlTypeName






def rules_initialization():
    return [
        CoreRules.AGGREGATE_CASE_TO_FILTER,
        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
        CoreRules.AGGREGATE_FILTER_TRANSPOSE,
        CoreRules.AGGREGATE_JOIN_TRANSPOSE,
        CoreRules.AGGREGATE_REMOVE,
        CoreRules.FILTER_AGGREGATE_TRANSPOSE,
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.FILTER_MERGE,
        CoreRules.FILTER_PROJECT_TRANSPOSE,
        CoreRules.JOIN_PUSH_EXPRESSIONS,
        CoreRules.PROJECT_JOIN_TRANSPOSE,
        CoreRules.PROJECT_MERGE,
        CoreRules.PROJECT_REMOVE,
        CoreRules.SORT_REMOVE,
        CoreRules.SORT_PROJECT_TRANSPOSE,
        CoreRules.UNION_TO_DISTINCT
    ]


#preprocess base query and generates logical query plan tree r_origin (apply mutation rules on this)
def preprocess(base_query):

    pass
    

def apply_rule(target_expr, rule):
    pass


def translate_to_query(r_new):
    if r_new == None:
        return None
   
    

def update(transformed_trees, mutant_queries,  r_new, new_query):
    # MUTATOR translates Rnew into a well-formed
    # SQL query, new query, based on the target DBMS’s dialect and
    # appends it to mutant queries

    transformed_trees.append(r_new)
    mutant_queries.append(new_query)


#Transforms r_origin using mutate_rules
def mutate_tree(r_origin, mutate_rules):
    target_expr = r_origin
    for rule in mutate_rules:
        target_expr = apply_rule(target_expr, rule)
    if target_expr != r_origin:
        return target_expr
    


#base_query and meta data of target database
def mutate_query(base_query):
    number_of_attempts = 10
    mutant_queries = []
    transformed_trees= []
    r_origin = preprocess(base_query)

    for k in range(number_of_attempts):
        mutate_rules = rules_initialization()
        r_new = mutate_tree(r_origin, mutate_rules)
        if r_new not in transformed_trees:
            new_query = translate_to_query(r_new)
            if new_query is not None:
                update(transformed_trees, mutant_queries, r_new, new_query)

    return base_query, mutant_queries




def main():
    base_query = "SELECT col FROM (SELECT * FROM my_table) AS sub WHERE col > 5 GROUP BY col"
    mutant_queries = []
   
    base_query, mutant_queries = mutate_query(base_query)
    
    for query in mutant_queries:
        print(query)

    jpype.shutdownJVM()
if __name__ == "__main__":
    main()
    


