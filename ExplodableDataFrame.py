from itertools import combinations

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import openpyxl # used as an engine in pandas, import not actually needed

# TODO test cases

class ExplodableDataFrame(pd.DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


    # If the object tries to create a copy of itself, use the subclass instead
    # https://pandas.pydata.org/pandas-docs/stable/development/extending.html
    @property
    def _constructor(self):
        return ExplodableDataFrame

    
    @staticmethod
    def from_excel(filename, sheetname):
        return ExplodableDataFrame(pd.read_excel(filename, engine='openpyxl', sheet_name=sheetname))

    
    def add_composite_column(self, columns, name=None, joiner="▲"):
        """Adds a new column by contactenating multiple columns and casting values to strings"""
        if name is None:
            name = joiner.join(columns)
        while name in self.columns: name += joiner # unlikely, but guarantees uniqueness
        self[name] = self.apply(lambda row: joiner.join([str(row[c]) for c in columns]), axis=1)
        return name


    def break_into_subtables(self, max_composite_depth=1, joiner="▲", key_flag="†"):
        
        # TODO add argument for abstraction depth to pull common columns from separate tables

        # TODO track key/fk relationships and only change names at the end
        # TODO track table-key and key-table mappings
        working_table = self.copy()
        tables = [working_table]
        schema = [[]]
        constants = []
        uniques = []
        foreign_keys = []
        composite_depth = 1

        def extract_columns_to_new_table(column_structure, composites):
            for key_column, dependent_columns in column_structure.items():
                # identify keys
                if key_column in composites:
                    keys = composites[key_column]
                else:
                    keys = [key_column]
                for k in keys:
                    foreign_keys.append(k)
                column_headers = keys + dependent_columns
                # create new table
                new_table = pd.concat([working_table[c] for c in column_headers], axis=1)
                new_table.drop_duplicates(inplace=True)
                new_table.rename({c:f'{c}{key_flag}'for c in keys}, axis=1, inplace=True)
                tables.append(new_table)
                schema.append(keys)
            
            # remove dependent columns
            for key_column, dependent_columns in column_structure.items():
                for col in dependent_columns:
                    if col in working_table.columns:
                        working_table.drop(col, axis=1, inplace=True)
        
        def mark_constants_and_uniques(relationships):
            for col in relationships['Constant']:
                if col not in constants: constants.append(col)
            for col in relationships['Unique']:
                if col not in uniques: uniques.append(col)

        def remove_composite_redundancy(relationships, composites):
            if composite_depth > 1:
                i = 0
                while i < len(relationships["Identifies"]):
                    pair = relationships["Identifies"][i]
                    x, y = pair
                    if (x in composites and y in composites[x]) or y in composites:
                        relationships["Identifies"].remove(pair)
                    else:
                        i += 1

        while composite_depth <= max_composite_depth:
            # add composite columns
            composites = {}
            if composite_depth > 1:
                working_columns = [c for c in working_table.columns if c not in constants and c not in uniques]
                if len(working_columns) == composite_depth: break
                for combo in list(combinations(working_columns, composite_depth)):
                    composite_name = working_table.add_composite_column(combo, joiner=joiner)
                    composites[composite_name] = list(combo)

# TODO add identification relationships to bijective tables if created?
# TODO process bijectives and identifications at the same time

# TODO pull out tables for constants and uniques first

            # pull out bijective relationships
            relationships = working_table.detect_column_relationships()
            mark_constants_and_uniques(relationships)
            remove_composite_redundancy(relationships, composites)
            column_structure = {}
            for g in relationships["Bijective"]:
                key, *others = g # take first value as key
                column_structure[key] = others
            extract_columns_to_new_table(column_structure, composites)

            # pull out identification relationships
            relationships = working_table.detect_column_relationships()
            mark_constants_and_uniques(relationships)
            remove_composite_redundancy(relationships, composites)
            column_structure = {}
            dependent_cols = []
            for col_x, col_y in relationships["Identifies"]:
                if col_x not in dependent_cols:
                    if col_x not in column_structure:
                        column_structure[col_x] = []
                    column_structure[col_x].append(col_y)
                    dependent_cols.append(col_y)
            extract_columns_to_new_table(column_structure, composites)

            # remove composite columns
            for col in composites:
                working_table.drop(col, axis=1, inplace=True)

            composite_depth += 1
        
        # identify keys in final table
        working_table.rename({c:f'{c}{key_flag}'for c in foreign_keys}, axis=1, inplace=True)
        schema[0] = foreign_keys

        return tables, schema
        

    def detect_column_relationships(self):
        # TODO add ignore_columns option to relationships to save time
        # TODO make find_relationships find bijectives in one step instead of two, again
        types = {
            "Independent": [],
            "Constant": [],
            "Unique": [],
            "Identifies": [],
            "Bijective": [],
        }

        # to start, assume every column is independent
        types["Independent"] = list(self.columns)

        # check if every value in column is unique
        for col in self.columns:
            unique_count = len(self[col].drop_duplicates())
            if len(self[col]) == unique_count:
                types["Unique"].append(col)
                types["Independent"].remove(col)
            elif unique_count == 1:
                types["Constant"].append(col)
                types["Independent"].remove(col)

        # compare each column to each other column
        remaining_columns = [c for c in types['Independent']]
        for i in range(len(remaining_columns)):
            col_x = remaining_columns[i]
            for j in range(len(remaining_columns)):
                col_y = remaining_columns[j]
                if col_x == col_y: continue
                
                relationship = self.characterize_column_relationship(col_x, col_y)
                if relationship == "Independent":
                    pass
                elif relationship == "Identifies":
                    if (col_x, col_y) not in types["Identifies"]:
                        types["Identifies"].append((col_x, col_y))
                        if col_y in types["Independent"]:
                            types["Independent"].remove(col_y)
                elif relationship == "Injective":
                    if (col_y, col_x) not in types["Identifies"]:
                        types["Identifies"].append((col_y, col_x))
                        if col_x in types["Independent"]:
                            types["Independent"].remove(col_x)

        # identify bijective associations between columns
        i = 0
        while i < len(types["Identifies"]):
            (col_x, col_y) = types["Identifies"][i]
            if (col_y, col_x) in types["Identifies"]:
                types["Identifies"].remove((col_x, col_y))
                types["Identifies"].remove((col_y, col_x))
                types["Bijective"].append((col_x, col_y))
            else:
                i += 1

        # group bijective relationships
        groups = []
        for x, y in types["Bijective"]:
            unique_columns = True
            for g in groups:
                if x in g or y in g:
                    unique_columns = False
                    if x not in g: g.append(x)
                    if y not in g: g.append(y)
                    break
            if unique_columns:
                groups.append([x, y])
        types["Bijective"] = groups

        # remove duplicate identifications due to bijective relationships
        i = 0
        while i < len(types["Identifies"]):
            pair = types["Identifies"][i]
            x, y = pair
            for g in types["Bijective"]:
                if y in g and g[0] != y:
                    types["Identifies"].remove(pair)
            i += 1

        return types


    def characterize_column_relationships(self, col):
        col_x = col
        relationships = {}
        for col_y in self.columns:
            if col_x == col_y: continue
            relationship = self.characterize_column_relationship(col_x, col_y)
            relationships[col_y] = relationship
        return relationships


    def characterize_column_relationship(self, col_x, col_y):
        two_columns = self.drop(self.columns.difference([col_x, col_y]), axis=1)
        unique_two_columns = two_columns.drop_duplicates()
        unique_pairs = len(unique_two_columns)
        if len(two_columns) == unique_pairs:
            return "Independent"
        unique_x_values = len(two_columns.drop_duplicates(subset=[col_x], keep="first"))
        unique_y_values = len(two_columns.drop_duplicates(subset=[col_y], keep="first"))
        if unique_pairs == unique_x_values:
            return "Identifies"
        elif unique_pairs == unique_y_values:
            return "Injective"
        else:
            return "Independent"


    def get_unique_column_pairs(self, col_x, col_y, as_dict=False):
        if as_dict:
            return {x: y for x, y in set(zip(self[col_x], self[col_y]))}
        else:
            two_columns = pd.concat([self[col_x], self[col_y]], axis=1)
            unique_two_columns = two_columns.drop_duplicates()
            return unique_two_columns


    def visualize_relationships(self):
        rel = self.detect_column_relationships()
        G = nx.DiGraph()
        G.add_nodes_from(rel["Independent"])
        G.add_nodes_from(rel["Constant"])
        G.add_nodes_from(rel["Unique"])
        G.add_edges_from(rel["Identifies"])
        options = {
            "font_size": 20,
            "width": 3,
            "arrowsize": 20,
            "node_color": "gray",
            "edge_color": "red",
        }
        nx.draw_networkx(G, pos=nx.spring_layout(G), **options)
        ax = plt.gca()
        ax.margins(0.20)
        plt.show()