from kgdata.wikipedia.prelude import Table as T, Row as R, Cell as C, TableVisitor
import pytest


def test_parse_canonical_table():
    tbl = TableVisitor().parse("""
{| class="wikitable"
|-
! Name and special position!! Appointment date
|-
| '''[[Arifin Zakaria]]'''<br />[[Chief Justice of Malaysia|Chief Justice]] || 12 September 2011
|-
| Hassan Lah || 10 August 2011
|}
    """)
    assert tbl == T(
        attrs={"class": "wikitable"},
        rows=[
            R(cells=[
                C(value='Name and special position', is_header=True),
                C(value='Appointment date', is_header=True)
            ]),
            R(cells=[
                C(value=
                  r"'''[[Arifin Zakaria]]'''<br />[[Chief Justice of Malaysia|Chief Justice]]"
                  ),
                C(value='12 September 2011')
            ]),
            R(cells=[C(value="Hassan Lah"),
                     C(value="10 August 2011")])
        ])


def test_parse_with_cell_properties():
    tbl = TableVisitor().parse("""
    {| class="wikitable"
    |-
    ! Column 1 !! Column 2 !! Column 3
    |-
    | rowspan="2" | A || colspan="2" style="text-align: center;" | B
    |-
    | C <!-- column 1 occupied by cell A -->
    | D
    |-
    | E
    | rowspan="2" colspan="2" style="text-align: center;" |F
    |-
    | G <!-- column 2+3 occupied by cell F -->
    |-
    | colspan="3" style="text-align: center;" | H
    |}
    """)
    assert tbl == T(
        attrs={"class": "wikitable"},
        rows=[
            R(cells=[
                C(value='Column 1', is_header=True),
                C(value='Column 2', is_header=True),
                C(value='Column 3', is_header=True)
            ]),
            R(cells=[
                C(value='A', attrs={"rowspan": "2"}),
                C(value='B',
                  attrs={
                      "colspan": "2",
                      "style": "text-align: center;"
                  })
            ]),
            R(cells=[
                C(value="C <!-- column 1 occupied by cell A -->"),
                C(value="D")
            ]),
            R(cells=[
                C(value="E"),
                C(value="F",
                  attrs={
                      "rowspan": "2",
                      "colspan": "2",
                      "style": "text-align: center;"
                  })
            ]),
            R(cells=[C(value="G <!-- column 2+3 occupied by cell F -->")]),
            R(cells=[
                C(value="H",
                  attrs={
                      "colspan": "3",
                      "style": "text-align: center;"
                  })
            ])
        ])


def test_parse_multiline_cells():
    result = TableVisitor().parse("""
    {|class="wikitable" style="float: right; margin:1em;"
    |-
    | Col 1 content
    continue
    | Col 2
    continue | still col 2
    | Col 3
    continue || should not be col 3
    |}
    """)
    assert result == T(attrs={
        "class": "wikitable",
        "style": "float: right; margin:1em;"
    },
                       rows=[
                           R(cells=[
                               C(value="Col 1 content\n    continue"),
                               C(value="Col 2\n    continue | still col 2"),
                               C(value="Col 3\n    continue"),
                               C(value="should not be col 3")
                           ])
                       ])



def test_parse_optional_first_row_delimiter():
    result = TableVisitor().parse("""
    {| class=wikitable style="text-align:center;"
    ! H1
    ! H2
    |-
    | A || B
    |}""")
    assert result == T(attrs={"class": "wikitable", "style": "text-align:center;"},
                       rows=[
                           R(cells=[
                               C(value="H1", is_header=True),
                               C(value="H2", is_header=True)
                           ]),
                           R(cells=[
                               C(value="A", is_header=False),
                               C(value="B", is_header=False)
                           ]),
                       ])


def test_parse_empty_row():
    result = TableVisitor().parse("""
    {| class="wikitable sortable"
    !align="left" width="220"|Charts (2007)
    !align="center"|Peak<br>position
    |-
    |align="left"|Australian [[ARIA Charts|ARIA]] Top 20 Compilations Chart
    |align="center"|1
    |-
    |}""")
    assert result == T(
        attrs={"class": "wikitable sortable"},
        rows=[
            R(cells=[
                C(value="Charts (2007)",
                  attrs={
                      "align": "left",
                      "width": "220"
                  },
                  is_header=True),
                C(value="Peak<br>position",
                  attrs={"align": "center"},
                  is_header=True)
            ]),
            R(cells=[
                C(value=
                  "Australian [[ARIA Charts|ARIA]] Top 20 Compilations Chart",
                  attrs={"align": "left"},
                  is_header=False),
                C(value="1", attrs={"align": "center"}, is_header=False)
            ]),
            R(cells=[])
        ])


def test_parse_multi_empty_cells():
    result = TableVisitor().parse("""
    {| class="wikitable"
    |-
    | Rank ||||
    |}
    """)
    assert result == T(
        attrs={'class': 'wikitable'},
        rows=[
            R(cells=[
                C(value="Rank"),
                C(value=""),
                C(value=""),
            ])
        ]
    )
    result = TableVisitor().parse("""
    {| class="wikitable"
    |-
    ! Rank !! Name !! CD1 !! CD2 !! OD !! FD
    |-
    ! WD
    | [[Renée Roca]] / [[James Yorke (figure skater)|James Yorke]] ||||||||
    |}
    """)
    assert result == T(
        attrs={'class': 'wikitable'},
        rows=[
            R(cells=[
                C(value='Rank', is_header=True),
                C(value='Name', is_header=True),
                C(value='CD1', is_header=True),
                C(value='CD2', is_header=True),
                C(value='OD', is_header=True),
                C(value='FD', is_header=True)
            ]),
            R(cells=[
                C(value='WD', is_header=True),
                C(value=
                  '[[Renée Roca]] / [[James Yorke (figure skater)|James Yorke]]',
                  is_header=False),
                C(value='', is_header=False),
                C(value='', is_header=False),
                C(value='', is_header=False),
                C(value='', is_header=False)
            ])
        ])


@pytest.mark.skip
def test_unknown_space():
    result = TableVisitor().parse("""
{| class="wikitable"
|-
! Name !! Position !! Year
|-
| Choc Sanders || [[Guard (American football)|Guard]] || 1928
|-
| Marion Hammon || [[Tackle (American football)|Tackle]] || 1929
|-
| Speedy Mason || [[halfback (American football)|Halfback]] || 1931
|-
| Clyde Carter || [[Tackle (American football)|Tackle]] || 1934
|-
| Harry Shuford<br />[[Bob Wilson (American football)|Bobby Wilson]] || [[Fullback (American football)|Fullback]]<br />[[halfback (American football)|Halfback]]|| 1934
|-
| Harry Shuford<br />[[Bob Wilson (American football)|Bobby Wilson]]<br />[[Truman Spain|Truman "Big Dog" Spain]]<br />[[J. C. Wetsel|J.C. "Iron Man" Wetsel]]|| [[Fullback (American football)|Fullback]]<br />[[halfback (American football)|Halfback]]<br />[[Tackle (American football)|Tackle]]<br />[[Guard (American football)|Guard]] || 1935
|-
| Kelly Simpson || [[End (American football)|End]] || 1941
|-
| Tom Dean || [[Tackle (American football)|Tackle]] || 1945
|-
| [[Doak Walker|Doak "The Doaker" Walker]] || [[halfback (American football)|Halfback]] || 1947
|-
| Doak Walker || [[halfback (American football)|Halfback]] || 1948
|-
| Doak Walker || [[halfback (American football)|Halfback]] || 1949
|-
| [[Kyle Rote|Kyle "The Mighty Mustang" Rote]] || [[halfback (American football)|Halfback]] || 1950
|-
| Dick Hightower || [[Center (American football)|Center]] || 1951
|- [[Forrest Gregg]] || [[Tackle (American football)|Tackle]] || 1955
|-
| [[Don Meredith|Don "Dandy Don" Meredith]] || [[Quarterback]] || 1958
|-
| Don Meredith || [[Quarterback]] || 1959
|-
| [[John LaGrone]] || [[Guard (American football)|Guard]] || 1966
|-
| [[Jerry LeVias]] || [[Wide Receiver]] || 1968
|-
| Robert Popelka || [[Defensive End]] || 1972
|-
| [[Louie Kelcher]]<br />[[Oscar Roan]] || [[Guard (American football)|Guard]]<br />[[Tight End]] || 1974
|-
| [[Emanuel Tolbert]] || [[Wide Receiver]] || 1978
|-
| [[John Simmons (American football)|John Simmons]] || [[Defensive back]] || 1980
|-
| [[Harvey Armstrong]] || [[Defensive Tackle]] || 1981
|-
| [[Eric Dickerson]] || [[Running Back]] || 1982
|-
| [[Russell Carter]] || [[Defensive back]] || 1983
|-
| [[Reggie Dupard]] || [[Running Back]] || 1985
|-
| John Stewert || [[Placekicker]] || 1993
|}
    """)