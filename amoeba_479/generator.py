import os
import random
import asyncpg
import asyncio

class QueryGenerator:
    def __init__(self, metadata, prob_table=None):
        self.metadata = metadata
        self.prob_table = prob_table or self.default_prob_table()
        self.rule_activation_frequency = self.init_rule_activation_frequency_table()

    def init_rule_activation_frequency_table(self):
        # Initialize frequency table for mutation rules (example rule IDs) at 0
        return {
            0: 0,
            1: 0,
            2: 0,
            3: 0,
            # add more if there's more mutation rules
        }

    def record_rule_activation(self, rule_id):
        # Increment the activation count for a rule
        if rule_id in self.rule_activation_frequency:
            self.rule_activation_frequency[rule_id] += 1
        else:
            self.rule_activation_frequency[rule_id] = 1

    def default_prob_table(self):
        # Default probabilities for SQL clause usage and constructs
        return {
            "table_ref_types": {"tablesimple": 0.5, "tablejoined": 0.5},  # simple table or join
            "join_types": {"INNER": 0.34, "LEFT": 0.33, "CROSS": 0.33},  # join types probabilities
            "join_condition_types": {"boolexpr": 0.5, "true": 0.5},  # join condition type probabilities
            "group_by": 0.7,  # probability to include GROUP BY
            "limit": 0.5,     # probability to include LIMIT clause
            "where": 0.6,     # probability to include WHERE clause
            "select_column_count": {1: 0.5, 2: 0.3, 3: 0.2},  # how many columns to select
        }

    def weighted_choice(self, choices):
        # Randomly pick a key from dict based on weighted probabilities
        r = random.random()
        acc = 0.0
        for key, prob in choices.items():
            acc += prob
            if r < acc:
                return key
        return list(choices.keys())[0]  # fallback

    def choose_join_type(self):
        # Choose join type according to probabilities
        return self.weighted_choice(self.prob_table["join_types"])

    def choose_join_condition_type(self):
        # Choose join condition type according to probabilities
        return self.weighted_choice(self.prob_table["join_condition_types"])

    def build_where_clause(self, table_alias, table_name, columns):
        # Generate WHERE clause conditions for one table alias
        where_clauses = []
        if random.random() < self.prob_table["where"]:
            col = random.choice(columns)
            col_type = self.metadata["tables"][table_name]["types"].get(col, "text")

            # Numeric columns: simple comparison
            if col_type in ['integer', 'bigint', 'smallint', 'numeric', 'real', 'double precision']:
                clause = f"{table_alias}.{col} > 0"
            # String/text columns
            elif col_type in ['text', 'character varying', 'character', 'varchar', 'char']:
                # Check if we have real values cached for this column
                sample_values = self.metadata.get("sample_values", {}).get(table_name, {}).get(col)
                if sample_values:
                    val = random.choice(sample_values)
                    clause = f"{table_alias}.{col} = '{val}'"
                else:
                    clause = f"{table_alias}.{col} = 'example_value'"
            # Boolean columns: IS TRUE
            elif col_type == 'boolean':
                clause = f"{table_alias}.{col} IS TRUE"
            # Fallback: no filtering condition
            else:
                clause = "TRUE"

            where_clauses.append(clause)
        return where_clauses


    def build_table_reference(self):
        # Decide to pick simple table or build join
        table_ref_type = self.weighted_choice(self.prob_table["table_ref_types"])
        if table_ref_type == "tablesimple" or len(self.metadata["tables"]) < 2:
            # Single table with alias t0
            table = random.choice(list(self.metadata["tables"].keys()))
            alias = "t0"
            return f"{table} {alias}", {alias: table}
        else:
            # Build join between two tables with aliases t0 and t1
            tables = random.sample(list(self.metadata["tables"].keys()), 2)
            alias1, alias2 = "t0", "t1"
            join_type = self.choose_join_type()
            join_cond_type = self.choose_join_condition_type()

            if join_cond_type == "boolexpr":
                # Try to find common columns for join condition
                cols1 = set(self.metadata["tables"][tables[0]]["columns"])
                cols2 = set(self.metadata["tables"][tables[1]]["columns"])
                common_cols = list(cols1.intersection(cols2))
                if common_cols:
                    col = random.choice(common_cols)
                    join_condition = f"{alias1}.{col} = {alias2}.{col}"
                else:
                    join_condition = "TRUE"
            else:
                join_condition = "TRUE"

            if join_type == "CROSS":
                table_ref = f"({tables[0]} {alias1} CROSS JOIN {tables[1]} {alias2})"
            else:
                table_ref = f"({tables[0]} {alias1} {join_type} JOIN {tables[1]} {alias2} ON {join_condition})"

            alias_map = {alias1: tables[0], alias2: tables[1]}
            return table_ref, alias_map

    def build_query_spec(self):
        # Build FROM clause and alias-to-table map
        from_clause, alias_map = self.build_table_reference()

        # Collect all available columns from selected tables (with aliases)
        total_columns = []
        for alias, table in alias_map.items():
            cols = self.metadata["tables"][table]["columns"]
            total_columns.extend([f"{alias}.{col}" for col in cols])

        # Pick how many columns to select
        num_cols = int(self.weighted_choice(self.prob_table["select_column_count"]))
        selected_columns = random.sample(total_columns, min(len(total_columns), num_cols))

        # Decide whether to include GROUP BY clause on selected columns
        include_group_by = random.random() < self.prob_table["group_by"]
        group_by_cols = selected_columns if include_group_by else []

        # Build WHERE clauses for each table alias involved
        where_clauses = []
        for alias, table in alias_map.items():
            cols = self.metadata["tables"][table]["columns"]
            where_clauses.extend(self.build_where_clause(alias, table, cols))

        # Randomly decide to add a LIMIT clause (with random limit)
        limit = random.randint(1, 100) if random.random() < self.prob_table["limit"] else None

        # Return query specification dict
        spec = {
            "from_clause": from_clause,
            "columns": selected_columns,
            "group_by": group_by_cols,
            "where": where_clauses,
            "limit": limit,
        }
        return spec

    def spec_to_query(self, spec):
        # Convert the query specification dict to SQL query string
        select_cols = ", ".join(spec["columns"])
        query = f"SELECT {select_cols} FROM {spec['from_clause']}"

        if spec["where"]:
            query += " WHERE " + " AND ".join(spec["where"])

        if spec["group_by"]:
            group_by_cols = ", ".join(spec["group_by"])
            query += f" GROUP BY {group_by_cols}"

        if spec["limit"]:
            query += f" LIMIT {spec['limit']}"

        return query + ";"

    def update_prob_table_with_feedback(self, base_query, fired_rules, triggered_bug):
        # Placeholder method for updating probabilities based on feedback
        pass

    async def generate_queries(self, n=10):
        # Generate a list of n SQL queries using the generator
        queries = []
        for _ in range(n):
            spec = self.build_query_spec()
            sql = self.spec_to_query(spec)
            queries.append(sql)
        return queries


async def retrieve_metadata(pool=None):
    # Retrieve DB metadata: tables, columns, and types from information_schema
    created_pool = False
    if pool is None:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise Exception("DATABASE_URL environment variable not set")
        pool = await asyncpg.create_pool(dsn=database_url)
        created_pool = True

    metadata = {"tables": {}, "sample_values": {}}
    async with pool.acquire() as conn:
        tables = await conn.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
        )
        for row in tables:
            table_name = row["table_name"]
            cols = await conn.fetch(
                """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name=$1;
                """,
                table_name,
            )
            metadata["tables"][table_name] = {
                "columns": [col["column_name"] for col in cols],
                "types": {col["column_name"]: col["data_type"] for col in cols},
            }

            # Initialize sample_values dict for this table
            metadata["sample_values"][table_name] = {}

            # For each text-like column, fetch up to N distinct sample values
            for col in cols:
                col_name = col["column_name"]
                col_type = col["data_type"]
                if col_type in ['text', 'character varying', 'character', 'varchar', 'char']:
                    samples = await conn.fetch(
                        f"SELECT DISTINCT {col_name} FROM {table_name} WHERE {col_name} IS NOT NULL LIMIT 10;"
                    )
                    # Extract values to list of strings
                    values = [r[col_name] for r in samples if r[col_name] is not None]
                    metadata["sample_values"][table_name][col_name] = values

    if created_pool:
        await pool.close()
    return metadata


# Usage example
async def main():
    # Retrieve metadata, instantiate generator, and print 5 generated queries
    metadata = await retrieve_metadata()
    gen = QueryGenerator(metadata)
    queries = await gen.generate_queries(5)
    for q in queries:
        print(q)


if __name__ == "__main__":
    asyncio.run(main())
