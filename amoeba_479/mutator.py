from mutatation_rules import MUTATE_RULES
from sqlglot import parse_one

#preprocess base query and generates logical query plan tree r_origin (apply mutation rules on this)
def preprocess(base_query):
    parsed_query = parse_one(base_query)
    return parsed_query

def apply_rule(target_expr, rule):
    

def translate_to_query(r_new):
    return r_new.sql()

def update(transformed_trees, mutant_queries,  r_new, new_query):
    # If so, MUTATOR translates Rnew into a well-formed
    # SQL query, new query, based on the target DBMS’s dialect and
    # appends it to mutant queries

    transformed_trees.append(r_new)
    mutant_queries.append(new_query)


#Transforms r_origin using mutate_rules
def mutate_tree(r_origin, mutate_rules, meta_data):
    target_expr = r_origin
    for rule in mutate_rules:
        target_expr = apply_rule(target_expr, rule)
    if target_expr != r_origin:
        return target_expr
    

#base_query and meta data of target database
def mutate_query(base_query):
    number_of_attempts = 5
    mutant_queries = []
    transformed_trees= []
    r_origin = preprocess(base_query)

    for k in range(number_of_attempts):
        mutate_rules = MUTATE_RULES
        r_new = mutate_tree(r_origin, mutate_rules)
        if r_new not in transformed_trees:
            new_query = translate_to_query(r_new)
            update(transformed_trees, mutant_queries, r_new, new_query)

    return base_query, mutant_queries



def main():
    base_query = "SELECT col FROM my_table WHERE col > 5 GROUP BY col"
    mutant_queries = []
   
    base_query, mutant_queries = mutate_query(base_query)
    
    for query in mutant_queries:
        print(query)

if __name__ == "__main__":
    main()
    



