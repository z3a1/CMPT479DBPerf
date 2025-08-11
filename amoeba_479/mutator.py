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
from org.apache.calcite.schema import SchemaPlus
from org.apache.calcite.sql.dialect import PostgresqlSqlDialect 
from org.apache.calcite.rel.type import RelDataTypeFactory
from org.apache.calcite.sql.type import SqlTypeName
from org.apache.calcite.adapter.jdbc import JdbcSchema
from org.apache.calcite.sql import SqlDialect
from java.sql import DriverManager
from java.util import Properties



def rules_initialization():
    return [
        CoreRules.AGGREGATE_CASE_TO_FILTER, #WORKS
        CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES, #WORKS
        CoreRules.AGGREGATE_FILTER_TRANSPOSE, #WORKS
        CoreRules.AGGREGATE_JOIN_TRANSPOSE, #WORKS
        CoreRules.AGGREGATE_REMOVE, #WORKS
        CoreRules.FILTER_AGGREGATE_TRANSPOSE, #WORKS
        CoreRules.FILTER_INTO_JOIN, #WORKS
        CoreRules.FILTER_MERGE, #WORKS
        CoreRules.FILTER_PROJECT_TRANSPOSE, #WORKS
        CoreRules.JOIN_PUSH_EXPRESSIONS, #WORKS
        CoreRules.PROJECT_JOIN_TRANSPOSE, #WORKS
        CoreRules.PROJECT_MERGE, #WORKS
        CoreRules.PROJECT_REMOVE, #WORKS
        CoreRules.SORT_REMOVE,
        CoreRules.SORT_PROJECT_TRANSPOSE,
        CoreRules.UNION_TO_DISTINCT
      
    ]



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
        "public",
        ds,
        None,        
        "public"     
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


def translate_to_query(r_new):
    if r_new == None:
        return None
    else:
        converter = RelToSqlConverter(None)
        sql_node = converter.visitRoot(r_new).asStatement()
        return sql_node.toString()
    

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
        print(rule)
        target_expr = apply_rule(target_expr, rule)
    if target_expr != r_origin:
        return target_expr
    


#base_query and meta data of target database
def mutate_query(base_query):
    number_of_attempts = 10
    mutant_queries = []
    transformed_trees= []
    r_origin = preprocess(base_query)
    print("R_ORIGIN =", r_origin)

    for k in range(number_of_attempts):
        mutate_rules = rules_initialization()
        r_new = mutate_tree(r_origin, mutate_rules)
        if r_new not in transformed_trees:
            new_query = translate_to_query(r_new)
            if new_query is not None:
                update(transformed_trees, mutant_queries, r_new, new_query)

    return base_query, mutant_queries






def main():
    # preprocess("SELECT * FROM pokemon INNER JOIN pokemon_types ON pokemon.id = pokemon_types.pokemon_id")  

    base_query = "SELECT * FROM pokemon INNER JOIN pokemon_types ON pokemon.id = pokemon_types.pokemon_id"
    mutant_queries = []
   
    base_query, mutant_queries = mutate_query(base_query)
    
    for query in mutant_queries:
        print(query)

    jpype.shutdownJVM()
if __name__ == "__main__":
    main()
    


