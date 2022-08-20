#%%
import json

from ExplodableDataFrame import ExplodableDataFrame

#%%
def obj_equals(a, b):
    return json.dumps(a) == json.dumps(b)


def get_example_df():
    # TODO improve examples to eliminate spurious relationships
    df = ExplodableDataFrame(
        columns=
        ["state", "state-code", "city", "city-code", "animal", "legs", "name", "fee", "id", "state_id", "alive", "date"],
        data=[
        ["CO", 0, "denver", 0, "cat", 4, "Boots", 150, 0, "C1", True, "1/1/2020"],
        ["CO", 0, "denver", 0, "dog", 4, "Fluffy", 300, 1, "C2", True, "1/2/2020"],
        ["CO", 0, "boulder", 1, "fish", 0, "Swimbo", 30, 2, "C3", True, "1/3/2020"],
        ["PA", 1, "york", 2, "cat", 4, "Fluffy", 200, 3, "P1", True, "1/4/2020"],
        ["PA", 1, "york", 2, "cat", 4, "Charles", 200, 4, "P2", True, "1/5/2020"],
        ["PA", 1, "york", 2, "fish", 0, "Fluffy", 40, 5, "P3", True, "1/6/2020"],
        ["PA", 1, "dover", 3, "lizard", 4, "Waldo", 30, 6, "P4", True, "1/6/2020"],
        ["FL", 2, "miami", 4, "lizard", 4, "Dizzy", 35, 7, "F1", True, "1/6/2020"],
        ["FL", 2, "miami", 4, "fish", 0, "Blub", 40, 8, "F2", True, "1/7/2020"],
        ["FL", 2, "miami", 4, "crab", 6, "Pynch", 40, 9, "F3", True, "1/8/2020"],
        ["PA", 1, "york", 2, "cat", 4, "Elmo", 200, 10, "P5", True, "1/8/2020"],
        ["CO", 0, "boulder", 1, "dog", 4, "Scraps", 300, 11, "C4", True, "1/8/2020"],
    ])
    return df

df = get_example_df()

#%%
expected_rels = {"Independent": ["city", "animal", "name", "fee", "date"],
                 "Constant": ["alive"],
                 "Unique": ["id", "state_id"],
                 "Identifies": [("city", "state"), ("animal", "legs")],
                 "Bijective": [["state", "state-code"], ["city", "city-code"]]}
                 
rels = df.detect_column_relationships()

assert obj_equals(rels, expected_rels), "Test failed: Relationships"
#%%
tbls, schema = df.break_into_subtables(10)
tbls
schema

#%%
# TODO develop method to check relationships (of a sample) against data for validity
assert df.characterize_column_relationship("city", "state") == "Identifies"
assert df.characterize_column_relationship("state", "city") == "Injective"
assert df.characterize_column_relationship("animal", "legs") == "Identifies"
assert df.characterize_column_relationship("legs", "animal") == "Injective"
assert df.characterize_column_relationship("animal", "state") == "Independent"
assert df.characterize_column_relationship("state", "name") == "Independent"
assert df.characterize_column_relationship("state", "id") == "Independent"
assert df.characterize_column_relationship("state_id", "id") == "Independent"
assert df.characterize_column_relationship("animal", "fee") == "Independent"
assert df.characterize_column_relationship("state", "fee") == "Independent"
comp = df.add_composite_column(["state", "animal"])
assert df.characterize_column_relationship(comp, "fee") == "Identifies"

#%%