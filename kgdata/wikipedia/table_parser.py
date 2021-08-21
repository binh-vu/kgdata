from parsimonious import Grammar, NodeVisitor
from parsimonious.expressions import (
    ZeroOrMore,
    OneOf,
    OneOrMore,
    Optional as PEGOptional,
    Sequence as PEGSequence,
)

from kgdata.wikipedia.wikimodels import Table, Row, Cell

"""
(DEPRECATED) Module for parsing tables from wikipedia markup.

You should use existing wikiparser to get the result as users see in the page as wikiparser are very lenient to user errors.
"""


class TableVisitor(NodeVisitor):
    grammar = Grammar(
        r"""
        # https://en.wikipedia.org/wiki/Help:Table
        # define the table, attributes of each mark must be on the same line as the mark, except the "|}" does not have any attrs
        tbl = _n "{|" attrs? _e rows "|}" _n
        
        # rows
        rows = first_row? row*
        first_row = (row_cells)+
        row = "|-" _ attrs? _e (row_cells)*
        row_cells = header_cells / data_cells
        # the header row is determined by the first ! character, even if the rest is || it still recognized as header! that's why we should use the cell delimiter instead of just !!
        header_cells = !"|-" "!" cell (same_row_cell)* _n
        data_cells = !"|-" !"|}" "|" cell (same_row_cell)* _n
        
        # cells. Note that there is one exception in the wiki table parser that || after the value of previous multiline cell (e.g., `cell 2 \n continue || cell 3`) is still belong
        # to the previous cell, which against its documentation that || is equal to `\n|` so we decide to stick to the doc as it's easier for us
        cell = (attrs "|")? cell_value
        same_row_cell = _cell_delimiter cell
        cell_value = tbl / ~r"((?!(!!|\|\||\|-|\|}|(\n *\|)|(\n *!))).)*"s
        _cell_delimiter = "!!" / "||"
        
        # html attributes
        attrs = (_ ident _ '=' _ attr_value _)+

        # basic expr
        ident = ~r"[a-z0-9]*"i
        # sometimes they don't use quoted for attribute values as they should be
        attr_value = ~'"[^\"]+"' / ~"'[^\']+'" / ~"[a-z0-9%]+"i
        
        # define space only, new line only, and space with new line
        # space
        _ = ~r"[ \t]*"
        # space and new line
        _n = ~r"[ \n]*"
        # end row
        _e = _ "\n" _n
    """
    )

    def visit_tbl(self, node, children):
        (_n, otag, attrs, _e, rows, ctag, _n) = children
        return Table(rows=rows, attrs=attrs or {}, caption=None)

    def visit_attrs(self, node, children):
        # extract html attributes
        attr_seq = children
        attrs = {}
        for attr in attr_seq:
            _, ident, _, _eq, _, attr_value, _ = attr
            attrs[ident.text] = attr_value
        return attrs

    def visit_attr_value(self, node, children):
        if node.text.startswith("'") or node.text.startswith('"'):
            return node.text[1:-1]
        return node.text

    def visit_rows(self, node, children):
        op_first_row, rep_rows = children
        rows = []
        if op_first_row is not None:
            rows.append(op_first_row)
        rows += rep_rows
        return rows

    def visit_first_row(self, node, children):
        # an array of cells (2D) so have to flatten
        row = Row(cells=[c for cs in children for c in cs])
        return row

    def visit_row(self, node, children):
        _l, _, attrs, _e, cells = children
        # an array of cells (2D) so have to flatten
        return Row(cells=[c for cs in cells for c in cs], attrs=attrs or {})

    def visit_header_cells(self, _node, children):
        _l, _l, cell_with_attrs, rep_cells, _n = children
        data = [cell_with_attrs]
        data[0].is_header = True
        for cell in rep_cells:
            if cell.is_literal():
                cell.is_header = True
            data.append(cell)
        return data

    def visit_data_cells(self, _node, children):
        _l, _l, _l, cell_with_attrs, rep_cells, _n = children
        data = [cell_with_attrs] + rep_cells
        return data

    def visit_cell(self, _node, children):
        opt_attr, cell_value = children
        if opt_attr is not None:
            attrs, _bar = opt_attr
        else:
            attrs = {}
        return Cell(value=cell_value, attrs=attrs)

    def visit_same_row_cell(self, node, children):
        return children[1]

    def visit_cell_value(self, _node, children):
        tbl_or_data = children[0]
        if isinstance(tbl_or_data, Table):
            return tbl_or_data
        return tbl_or_data.text.strip()

    def generic_visit(self, node, children):
        if isinstance(node.expr, (ZeroOrMore, OneOrMore, PEGSequence)):
            return children
        if isinstance(node.expr, OneOf):
            return children[0]
        if isinstance(node.expr, PEGOptional):
            if len(children) == 0:
                return None
            return children[0]
        return node
