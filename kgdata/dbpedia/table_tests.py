from .dbpediamodels import Table

"""This module contains complex tests for wikipedia html tables that can't be done in one line
"""


def is_relational_table(tbl: Table) -> bool:
    if len(tbl.rows) == 0:
        return False

    if not all(c.is_header for c in tbl.rows[0].cells):
        return False

    if not all(not c.is_header for r in tbl.rows[1:] for c in r.cells):
        return False

    return True
