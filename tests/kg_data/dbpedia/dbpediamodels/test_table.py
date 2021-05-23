from kgdata.dbpedia import Table as T, Row as R, Cell as C, HTMLTableExtractor, RawHTMLTable


def test_span():
    tbl = T(id="", pageURI="", rows=[
        R(cells=[
            C(value='A1', html='A1', rowspan=2, colspan=1, is_header=True),
            C(value='A2', html='A2', rowspan=1, colspan=1, is_header=True),
            C(value='A3', html='A3', rowspan=1, colspan=1, is_header=True),
        ]),
        R(cells=[
            C(value='B1', html='B1', rowspan=1, colspan=1),
            C(value='B2', html='B2', rowspan=1, colspan=2),
        ]),
        R(cells=[
            C(value='C1', html='C1', rowspan=1, colspan=1),
            C(value='C2', html='C2', rowspan=1, colspan=2),
        ]),
    ])
    assert tbl.span().to_list() == [['A1', 'A2', 'A3'],
                                    ['A1', 'B1', 'B2'],
                                    ['C1', 'C2', 'C2']]


def test_span2():
    tbl = T(id="", pageURI="", rows=[
        R(cells=[
            C(value="R0C0", html="R0C0", rowspan=1, colspan=1, is_header=True),
            C(value="R0C1", html="R0C1", rowspan=1, colspan=1, is_header=True),
            C(value="R0C2", html="R0C2", rowspan=1, colspan=2, is_header=True),
        ]),
        R(cells=[
            C(value="R1C0", html="R1C0", rowspan=1, colspan=1),
            C(value="R1C1", html="R1C1", rowspan=2, colspan=1),
            C(value="R1C2", html="R1C2", rowspan=1, colspan=2),
        ]),
        R(cells=[
            C(value="R2C0", html="R2C0", rowspan=1, colspan=1),
            C(value="R2C1", html="R2C1", rowspan=2, colspan=2),
        ]),
        R(cells=[
            C(value="R3C0", html="R3C0", rowspan=1, colspan=1),
            C(value="R3C1", html="R3C1", rowspan=3, colspan=1),
        ]),
        R(cells=[
            C(value="R4C0", html="R4C0", rowspan=1, colspan=1),
            C(value="R4C1", html="R4C1", rowspan=1, colspan=2),
        ]),
        R(cells=[
            C(value="R5C0", html="R5C0", rowspan=1, colspan=1),
            C(value="R5C1", html="R5C1", rowspan=2, colspan=2),
        ]),
        R(cells=[
            C(value="R6C0", html="R6C0", rowspan=1, colspan=1),
            C(value="R6C1", html="R6C1", rowspan=2, colspan=1),
        ]),
        R(cells=[
            C(value="R7C0", html="R7C0", rowspan=1, colspan=1),
            C(value="R7C1", html="R7C1", rowspan=1, colspan=2),
        ]),
        R(cells=[
            C(value="R8C0", html="R8C0", rowspan=1, colspan=1),
            C(value="R8C1", html="R8C1", rowspan=4, colspan=1),
            C(value="R8C2", html="R8C2", rowspan=1, colspan=2),
        ]),
        R(cells=[
            C(value="R9C0", html="R9C0", rowspan=1, colspan=1),
            C(value="R9C1", html="R9C1", rowspan=1, colspan=2),
        ]),
        R(cells=[
            C(value="R10C0", html="R10C0", rowspan=1, colspan=1),
            C(value="R10C1", html="R10C1", rowspan=1, colspan=2),
        ]),
        R(cells=[
            C(value="R11C0", html="R11C0", rowspan=1, colspan=1),
            C(value="R11C1", html="R11C1", rowspan=2, colspan=1),
            C(value="R11C2", html="R11C2", rowspan=1, colspan=1),
        ]),
        R(cells=[
            C(value="R12C0", html="R12C0", rowspan=1, colspan=1),
            C(value="R12C1", html="R12C1", rowspan=7, colspan=1),
            C(value="R12C2", html="R12C2", rowspan=1, colspan=1),
        ]),
        R(cells=[
            C(value="R13C0", html="R13C0", rowspan=1, colspan=1),
            C(value="R13C1", html="R13C1", rowspan=1, colspan=1),
            C(value="R13C2", html="R13C2", rowspan=1, colspan=1),
        ]),
        R(cells=[
            C(value="R14C0", html="R14C0", rowspan=1, colspan=1),
            C(value="R14C1", html="R14C1", rowspan=1, colspan=1),
            C(value="R14C2", html="R14C2", rowspan=2, colspan=1),
        ]),
        R(cells=[
            C(value="R15C0", html="R15C0", rowspan=1, colspan=1),
            C(value="R15C1", html="R15C1", rowspan=3, colspan=1),
        ]),
        R(cells=[
            C(value="R16C0", html="R16C0", rowspan=1, colspan=1),
            C(value="R16C1", html="R16C1", rowspan=3, colspan=1),
        ]),
        R(cells=[
            C(value="R17C0", html="R17C0", rowspan=1, colspan=1),
        ]),
        R(cells=[
            C(value="R18C0", html="R18C0", rowspan=1, colspan=1),
            C(value="R18C1", html="R18C1", rowspan=1, colspan=1),
        ]),
    ])

    assert tbl.span().to_list() == [
        ['R0C0', 'R0C1', 'R0C2', 'R0C2'],
        ['R1C0', 'R1C1', 'R1C2', 'R1C2'],
        ['R2C0', 'R1C1', 'R2C1', 'R2C1'],
        ['R3C0', 'R3C1', 'R2C1', 'R2C1'],
        ['R4C0', 'R3C1', 'R4C1', 'R4C1'],
        ['R5C0', 'R3C1', 'R5C1', 'R5C1'],
        ['R6C0', 'R6C1', 'R5C1', 'R5C1'],
        ['R7C0', 'R6C1', 'R7C1', 'R7C1'],
        ['R8C0', 'R8C1', 'R8C2', 'R8C2'],
        ['R9C0', 'R8C1', 'R9C1', 'R9C1'],
        ['R10C0', 'R8C1', 'R10C1', 'R10C1'],
        ['R11C0', 'R8C1', 'R11C1', 'R11C2'],
        ['R12C0', 'R12C1', 'R11C1', 'R12C2'],
        ['R13C0', 'R12C1', 'R13C1', 'R13C2'],
        ['R14C0', 'R12C1', 'R14C1', 'R14C2'],
        ['R15C0', 'R12C1', 'R15C1', 'R14C2'],
        ['R16C0', 'R12C1', 'R15C1', 'R16C1'],
        ['R17C0', 'R12C1', 'R15C1', 'R16C1'],
        ['R18C0', 'R12C1', 'R18C1', 'R16C1'],
    ]
    

def test_span3():
    tbl = T(id="", pageURI="", rows=[
        R(cells=[
            C(value="A", html="A", rowspan=1, colspan=1, is_header=True),
            C(value="B", html="B", rowspan=1, colspan=1, is_header=True),
            C(value="C", html="C", rowspan=1, colspan=2, is_header=True),
        ]),
        R(cells=[
            C(value="A", html="A", rowspan=1, colspan=100),
        ]),
    ])
    assert tbl.span().to_list() == [
        ['A', 'B', 'C'],
        ['A', 'A', 'A'],
    ]


def simplify_tbl(html):
    """Run this code to print a simplify version of the table so that we can debug easier"""
    tbls = HTMLTableExtractor().extract(RawHTMLTable("", set(), html, ""))
    assert len(tbls) == 1
    tbl = tbls[0]

    print("T(rows=[")
    for i, r in enumerate(tbl.rows):
        print("\tR(cells=[")
        for j, c in enumerate(r.cells):
            if c.is_header:
                is_header = ", is_header=True"
            else:
                is_header = ""

            val = f"R{i}C{j}"
            print(
                f'\t\tC(value="{val}", html="{val}", rowspan={c.rowspan}, colspan={c.colspan}{is_header}),'
            )
        print("\t]),")
    print("])")
