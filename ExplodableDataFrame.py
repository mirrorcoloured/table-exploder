from itertools import combinations

import pandas as pd
import networkx as nx
import plotly.graph_objects as go
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


    def break_into_subtables(self, max_composite_depth=1, joiner="▲", key_flag="†", fkey_flag="‡"):
        
        # TODO add argument for abstraction depth to pull common columns from separate tables

        working_table = self.copy()
        tables = [working_table]
        table_primary_keys = [[]]
        table_foreign_keys = [{}]
        all_keys = []
        skip_columns = []
        composite_depth = 1

        def extract_columns_to_new_table(column_structure, composites, tables, table_primary_keys, table_foreign_keys):
            for key_column, dependent_columns in column_structure.items():
                # identify keys
                if key_column in composites:
                    keys = composites[key_column]
                else:
                    keys = [key_column]
                for k in keys:
                    if k not in table_foreign_keys[0]:
                        table_foreign_keys[0][k] = []
                    table_foreign_keys[0][k].append(len(tables))
                column_headers = keys + dependent_columns

                # create new table
                new_table = pd.concat([working_table[c] for c in column_headers], axis=1)
                new_table.drop_duplicates(inplace=True)
                tables.append(new_table)
                table_primary_keys.append(keys)
                for key in keys:
                    if key not in all_keys: all_keys.append(key)
            
            # remove dependent columns
            for key_column, dependent_columns in column_structure.items():
                for col in dependent_columns:
                    if col in working_table.columns:
                        working_table.drop(col, axis=1, inplace=True)
        
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
                working_columns = [c for c in working_table.columns if c not in skip_columns]
                # if len(working_columns) == composite_depth: break
# PUT ME BACK
                for combo in list(combinations(working_columns, composite_depth)):
                    composite_name = working_table.add_composite_column(combo, joiner=joiner)
                    composites[composite_name] = list(combo)

# TODO pull out tables for constants and uniques first

            relationships = working_table.detect_column_relationships()
            if composite_depth == 1:
                for col in relationships['Constant']:
                    if col not in skip_columns: skip_columns.append(col)
                for col in relationships['Unique']:
                    if col not in skip_columns: skip_columns.append(col)
            remove_composite_redundancy(relationships, composites)

            column_structure = {}
            # pull out bijective relationships
            for g in relationships["Bijective"]:
                key, *others = g # take first value as key
                column_structure[key] = others
            # pull out identification relationships
            dependent_cols = []
            for col_x, col_y in relationships["Identifies"]:
                if col_x not in dependent_cols:
                    if col_x not in column_structure:
                        column_structure[col_x] = []
                    column_structure[col_x].append(col_y)
                    dependent_cols.append(col_y)
            extract_columns_to_new_table(column_structure, composites, tables, table_primary_keys, table_foreign_keys)

            # remove composite columns
            for col in composites:
                working_table.drop(col, axis=1, inplace=True)

            composite_depth += 1
        
        # condense into schema
        schema = []
        for table in tables:
            schema.append(list(table.columns))

        # rename columns to identify keys in final tables
        for i, table in enumerate(tables):
            column_renames = {}
            for k in all_keys:
                column_renames[k] = f'{k}{key_flag if k in table_primary_keys[i] else fkey_flag}'
            table.rename(column_renames, axis=1, inplace=True)

        return tables, schema
        

    def detect_column_relationships(self):
        # TODO add ignore_columns option to relationships to save time
        types = {
            "Independent": [],
            "Constant": [],
            "Unique": [],
            "Identifies": [],
            "Bijective": [],
        }

        # to start, assume every column is independent
        types["Independent"] = list(self.columns)

        # identify columns where all values are unique or constant
        for col in self.columns:
            unique_count = len(self[col].drop_duplicates())
            if len(self[col]) == unique_count:
                types["Unique"].append(col)
                types["Independent"].remove(col)
            elif unique_count == 1:
                types["Constant"].append(col)
                types["Independent"].remove(col)
        
        # compare each column to each other column
        column_pairs = combinations(types['Independent'], 2)
        bijective_map = {}
        for col_x, col_y in column_pairs:

            # skip dependent columns
            if col_x not in types["Independent"]: continue
            # skip redundant columns
            if col_x in bijective_map and bijective_map[col_x][0] != col_x: continue

            relationship = self.characterize_column_relationship(col_x, col_y)
            if relationship != "Independent": # some relationship exists
                inv_relationship = self.characterize_column_relationship(col_y, col_x)
                if relationship == inv_relationship == "Identifies": # 1-1 relationship

                    if col_x not in bijective_map and col_y not in bijective_map:
                        types["Bijective"].append([col_x, col_y])
                        bijective_map[col_x] = types["Bijective"][-1]
                        bijective_map[col_y] = types["Bijective"][-1]
                        remove_if_present(types["Independent"], col_y)
                    else:
                        if col_x not in bijective_map:
                            group = bijective_map[col_y]
                            group.append(col_x)
                            remove_if_present(types["Independent"], col_x)
                        if col_y not in bijective_map:
                            group = bijective_map[col_x]
                            group.append(col_y)
                            remove_if_present(types["Independent"], col_y)
                        
                else: # 1-many or many-1 relationship
                    if relationship == "Identifies":
                        types["Identifies"].append((col_x, col_y))
                        remove_if_present(types["Independent"], col_y)
                    elif relationship == "Injective":
                        types["Identifies"].append((col_y, col_x))
                        remove_if_present(types["Independent"], col_x)

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


    def relationships_to_graph(self):
        rel = self.detect_column_relationships()
        G = nx.DiGraph()
        G.add_nodes_from(rel["Independent"])
        G.add_nodes_from(rel["Constant"])
        G.add_nodes_from(rel["Unique"])
        G.add_edges_from(rel["Identifies"])
        for group in rel["Bijective"]:
            G.add_edges_from(combinations(group, 2)) # TODO bidirectional arrows
        return G
    
    def visualize_relationships(self):
        G = self.relationships_to_graph()
        # options = {
        #     "font_size": 20,
        #     "width": 3,
        #     "arrowsize": 20,
        #     "node_color": "gray",
        #     "edge_color": "red",
        # }
        # nx.draw_networkx(G, pos=nx.spring_layout(G), **options)
        # ax = plt.gca()
        # ax.margins(0.20)
        # plt.show()

        pos=nx.spring_layout(G)
        
        # https://plotly.com/python/network-graphs/
        # G = nx.random_geometric_graph(200, 0.125)
        
        edge_x = []
        edge_y = []
        for edge in G.edges():
            x0, y0 = G.nodes[edge[0]]['pos']
            x1, y1 = G.nodes[edge[1]]['pos']
            edge_x.append(x0)
            edge_x.append(x1)
            edge_x.append(None)
            edge_y.append(y0)
            edge_y.append(y1)
            edge_y.append(None)

        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(width=0.5, color='#888'),
            hoverinfo='none',
            mode='lines')

        node_x = []
        node_y = []
        for node in G.nodes():
            x, y = G.nodes[node]['pos']
            node_x.append(x)
            node_y.append(y)

        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode='markers',
            hoverinfo='text',
            marker=dict(
                showscale=True,
                # colorscale options
                #'Greys' | 'YlGnBu' | 'Greens' | 'YlOrRd' | 'Bluered' | 'RdBu' |
                #'Reds' | 'Blues' | 'Picnic' | 'Rainbow' | 'Portland' | 'Jet' |
                #'Hot' | 'Blackbody' | 'Earth' | 'Electric' | 'Viridis' |
                colorscale='YlGnBu',
                reversescale=True,
                color=[],
                size=10,
                colorbar=dict(
                    thickness=15,
                    title='Node Connections',
                    xanchor='left',
                    titleside='right'
                ),
                line_width=2))
        
        node_adjacencies = []
        node_text = []
        for node, adjacencies in enumerate(G.adjacency()):
            node_adjacencies.append(len(adjacencies[1]))
            node_text.append('# of connections: '+str(len(adjacencies[1])))

        node_trace.marker.color = node_adjacencies
        node_trace.text = node_text

        fig = go.Figure(data=[edge_trace, node_trace],
             layout=go.Layout(
                title='<br>Network graph made with Python',
                titlefont_size=16,
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20,l=5,r=5,t=40),
                # annotations=[ dict(
                #     text="Python code: <a href='https://plotly.com/ipython-notebooks/network-graphs/'> https://plotly.com/ipython-notebooks/network-graphs/</a>",
                #     showarrow=False,
                #     xref="paper", yref="paper",
                #     x=0.005, y=-0.002 ) ],
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                )
        fig.show()


def remove_if_present(list_obj, value):
    if value in list_obj:
        return list_obj.remove(value)
    return None