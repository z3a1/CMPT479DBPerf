import random
import jpype
import jpype.imports
from jpype.types import *
import os
from dotenv import load_dotenv


java_lib_dir = "target/dependency"
dep_dir = "target/dependency"

jars = [os.path.join(dep_dir, jar) for jar in os.listdir(dep_dir) if jar.endswith(".jar")]
classpath = os.pathsep.join(jars)
jvm_path = jpype.getDefaultJVMPath()

jpype.startJVM(jvm_path,f"-Djava.class.path={classpath}")

from org.apache.calcite.sql.parser import SqlParser
from org.apache.calcite.plan.hep import HepPlanner, HepProgramBuilder, HepProgram
from org.apache.calcite.rel.rules import *
from org.apache.calcite.tools import Frameworks
from org.apache.calcite.rel.rel2sql import RelToSqlConverter
from org.apache.calcite.rel.rules import CoreRules
from org.apache.calcite.sql.dialect import PostgresqlSqlDialect 
from org.apache.calcite.adapter.jdbc import JdbcSchema
from org.apache.calcite.sql import SqlDialect
from java.sql import DriverManager
from java.util import Properties


# Core_Rules = jpype.JClass("org.apache.calcite.rel.rules.CoreRules")
# print(dir(Core_Rules))
def rules_initialization():
    rules_list = [
        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
        CoreRules.AGGREGATE_FILTER_TRANSPOSE,
        CoreRules.AGGREGATE_JOIN_REMOVE,
        CoreRules.AGGREGATE_JOIN_TRANSPOSE,
        CoreRules.AGGREGATE_MERGE,
        CoreRules.AGGREGATE_PROJECT_MERGE,
        CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
        CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
        CoreRules.AGGREGATE_REMOVE,
        CoreRules.AGGREGATE_UNION_TRANSPOSE,
        CoreRules.AGGREGATE_VALUES,
        CoreRules.CALC_MERGE,
        CoreRules.CALC_REMOVE,
        CoreRules.FILTER_AGGREGATE_TRANSPOSE,
        CoreRules.FILTER_CALC_MERGE,
        DateRangeRules.FILTER_INSTANCE,
        CoreRules.FILTER_INTO_JOIN,
        CoreRules.JOIN_CONDITION_PUSH,
        CoreRules.FILTER_MERGE,
        CoreRules.FILTER_PROJECT_TRANSPOSE,
        CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM,
        CoreRules.FILTER_SET_OP_TRANSPOSE,
        CoreRules.FILTER_TO_CALC,
        #CoreRules.INTERSECT_REMOVE,
        CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER,
        CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER,
        CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE_INCLUDE_OUTER,
        CoreRules.JOIN_EXTRACT_FILTER,
        CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE,
        CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE,
        CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE,
        CoreRules.JOIN_PUSH_EXPRESSIONS,
        CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES,
        CoreRules.JOIN_LEFT_UNION_TRANSPOSE,
        CoreRules.JOIN_RIGHT_UNION_TRANSPOSE,
        #CoreRules.MINUS_REMOVE,
        CoreRules.PROJECT_CALC_MERGE,
        CoreRules.PROJECT_FILTER_TRANSPOSE,
        CoreRules.PROJECT_JOIN_REMOVE,
        CoreRules.PROJECT_JOIN_TRANSPOSE,
        CoreRules.PROJECT_MERGE,
        CoreRules.PROJECT_REMOVE,
        CoreRules.PROJECT_SET_OP_TRANSPOSE,
        CoreRules.PROJECT_TO_CALC,
        CoreRules.CALC_TO_WINDOW,
        CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
        CoreRules.PROJECT_WINDOW_TRANSPOSE,
        CoreRules.CALC_REDUCE_EXPRESSIONS,
        CoreRules.FILTER_REDUCE_EXPRESSIONS,
        CoreRules.PROJECT_REDUCE_EXPRESSIONS,
        CoreRules.JOIN_REDUCE_EXPRESSIONS,
        CoreRules.WINDOW_REDUCE_EXPRESSIONS,
        CoreRules.SORT_JOIN_COPY,
        CoreRules.SORT_JOIN_TRANSPOSE,
        CoreRules.SORT_PROJECT_TRANSPOSE,
        CoreRules.SORT_REMOVE_CONSTANT_KEYS,
        CoreRules.SORT_UNION_TRANSPOSE,
        CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
        CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
        CoreRules.JOIN_SUB_QUERY_TO_CORRELATE,
        CoreRules.UNION_REMOVE,
        CoreRules.UNION_TO_DISTINCT,
        CoreRules.FILTER_VALUES_MERGE,
        CoreRules.UNION_PULL_UP_CONSTANTS,
        CoreRules.PROJECT_VALUES_MERGE,
        CoreRules.AGGREGATE_CASE_TO_FILTER,
        CoreRules.AGGREGATE_UNION_AGGREGATE,
        CoreRules.PROJECT_CORRELATE_TRANSPOSE,
        CoreRules.AGGREGATE_JOIN_JOIN_REMOVE,
        CoreRules.PROJECT_JOIN_JOIN_REMOVE,
        CoreRules.UNION_MERGE,
    ]
    random.shuffle(rules_list)
    return rules_list


#preprocess base query and generates logical query plan tree r_origin (apply mutation rules on this)
def preprocess(base_query):
    load_dotenv()

    db_url = os.getenv("DATABASE_URL")
    
    user = os.getenv("DB_USER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    port = 5432
    db_name = os.getenv("DATABASE_NAME")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db_name}"

    ds = JdbcSchema.dataSource(
        jdbc_url,
        "org.postgresql.Driver",
        user,
        password
    )
    root_schema = Frameworks.createRootSchema(True)
   
    jdbc_schema = JdbcSchema.create(
        root_schema,
        None,
        ds,
        None,        
        None    
    )
    root_schema.add("public", jdbc_schema)

    parserConfig = SqlParser.config().withCaseSensitive(False)  
    config = Frameworks.newConfigBuilder().parserConfig(parserConfig).defaultSchema(root_schema.getSubSchema("public")).build()

    planner = Frameworks.getPlanner(config)

    sql_node = planner.parse(base_query)
    # print(sql_node)
    validate_node = planner.validate(sql_node)
    # print(validate_node)
    rel_root = planner.rel(validate_node) #has metadata and logical query plan

    # Use this for rule mutations
    return rel_root.rel


def apply_rule(target_expr, rule):
    program = HepProgram.builder().addRuleInstance(rule).build()
    hepPlanner = HepPlanner(program)
    hepPlanner.setRoot(target_expr)
    return hepPlanner.findBestExp()
    # print(hepPlanner)


def translate_to_query(r_new, dialect):
    if r_new == None:
        return None
    else:
        sql_node = RelToSqlConverter(dialect).visitRoot(r_new).asStatement()
        return sql_node.toString().replace("`", "")
    

def update(transformed_trees, mutant_queries,  r_new, new_query):
    transformed_trees.add(r_new)
    mutant_queries.add(new_query)
    


#Transforms r_origin using mutate_rules
def mutate_tree(r_origin, mutate_rules):
    target_expr = r_origin
    for rule in mutate_rules:
        # print(rule)
        target_expr = apply_rule(target_expr, rule)
    if target_expr != r_origin:
        return target_expr


#base_query and meta data of target database
def mutate_query(base_query):
    number_of_attempts = 10
    mutant_queries = set()
    transformed_trees= set()
    r_origin = preprocess(base_query)
    dialect =PostgresqlSqlDialect.DEFAULT

    for k in range(number_of_attempts):
        mutate_rules = rules_initialization()
    
        r_new = mutate_tree(r_origin, mutate_rules)
        if r_new not in transformed_trees:
            new_query = translate_to_query(r_new, dialect)
            if new_query is not None:
                update(transformed_trees, mutant_queries, r_new, new_query)

    return base_query, mutant_queries


def shutdown_JVM():
    jpype.shutdownJVM()


BASE_QUERIES = [
    "SELECT t0.move_id, t0.priority FROM pokemon_moves t0 WHERE t0.level > 0 GROUP BY t0.move_id, t0.priority",
    # "SELECT t1.damage_class_id FROM (pokemon t0 CROSS JOIN types t1) WHERE t0.species_id > 0 AND t1.identifier = 'ground' LIMIT 53",
    # "SELECT t0.species_id, t0.base_experience, t0.identifier FROM (pokemon t0 CROSS JOIN abilities t1) WHERE t1.id > 0 GROUP BY t0.species_id, t0.base_experience, t0.identifier LIMIT 62",
    # "SELECT t0.is_hidden FROM pokemon_abilities t0 WHERE t0.is_hidden IS TRUE GROUP BY t0.is_hidden",
    # "SELECT t0.identifier FROM types t0 WHERE t0.id > 0 GROUP BY t0.identifier LIMIT 61",
]


def main():
    base_query = "SELECT t0.move_id, t0.priority FROM pokemon_moves t0 WHERE t0.level > 0 GROUP BY t0.move_id, t0.priority"

    mutant_queries = []
   
    
    base_query, mutant_queries = mutate_query(base_query)
    
    for query in mutant_queries:
        print(query)
        print("==========================")

if __name__ == "__main__":
    main()
