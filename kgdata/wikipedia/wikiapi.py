from kgdata.wikipedia.config import WikipediaConfig
import requests
from typing import List, Dict
from tqdm.auto import tqdm


def get_urls_from_page_ids(page_ids: List[str]):
    """Get Wikipedia URLs from their page IDs. Not very useful since we can use the curid parameter as in WikiPageArticle
    
    Parameters
    ----------
    page_ids : List[str]
        list of page ids
    
    Returns
    -------
    List[str]
        list of corresponding URLs
    """
    s_page_ids = "|".join(page_ids)
    resp = requests.get(f"{WikipediaConfig.WikiURL}/w/api.php",
                        params={
                            "action": "query",
                            "prop": "info",
                            "pageids": s_page_ids,
                            "inprop": "url",
                            "format": "json"
                        })
    data = resp.json()
    assert data['batchcomplete'] is True
    return [data['query']['pages'][page_id]['fullurl'] for page_id in page_ids]


def does_wikiurl_not_exist(url: str):
    assert url.startswith("{WikipediaConfig.WikiURL}/wiki/"), url
    resp = requests.get(url, params={"redirect": "no"})
    return resp.status_code == 404


def get_page_ids_from_titles(titles: List[str],
                             batch_size: int = 25) -> Dict[str, str]:
    """Get page IDs from titles.

    Parameters
    ----------
    titles : List[str]
        list of titles that we wish to retrieve the page ids

    Returns
    -------
    Dict[str, str]
        a mapping from title to page id, the order of each item is guarantee to be the same as the original list
    """
    assert all(
        title.find("|") == -1
        for title in titles), "All titles must not contain the delimiter `|`"
    assert len(set(titles)) == len(titles), "Should not have duplicated titles"

    # update the result to only find the one not in the cache
    set_titles = set(titles)

    if len(titles) > batch_size:
        results = {}
        for i in tqdm(range(0, len(titles), batch_size)):
            result = get_page_ids_from_titles(titles[i:i + batch_size],
                                              batch_size)
            for k, v in result.items():
                results[k] = v
        return results

    results = {}
    if len(titles) > 0:
        resp = requests.get(f"{WikipediaConfig.WikiURL}/w/api.php",
                        params={
                            "action": "query",
                            "titles": "|".join(titles),
                            "format": "json",
                            "formatversion": 2
                        })
        data = resp.json()
        assert data['batchcomplete'] is True

        titles_tokens = [set(title.lower().split(" ")) for title in titles]

        for i, page in enumerate(data['query']['pages']):
            if 'pageid' not in page:
                assert page.get('missing', False), str(page)
                assert page['title'] in set_titles
                results[page['title']] = None
            else:
                # now we need to find the title
                tokens = set(page['title'].lower().split(" "))
                title_with_score = [(title,
                                    len(tokens.intersection(tt)) / len(tokens))
                                    for title, tt in zip(titles, titles_tokens)]
                title_with_score.sort(reverse=True, key=lambda x: x[1])

                title = title_with_score[0][0]
                assert title not in results
                results[title] = {
                    "id": page['pageid'],
                    "wiki_title": page['title'],
                    "match_title": title,
                    "rank_titles": title_with_score
                }

        for k, v in results.items():
            results[k] = v

    return results


if __name__ == "__main__":
    titles = [
        'Murder of Alistair Wilson', 'Ivan Tikhonov', 'Emoia digul',
        '1980 Nevada Wolf Pack football team',
        "2020 World Single Distances Speed Skating Championships – Men's 500 metres",
        'Uwe Schröder', 'Francesca Corbett', 'Axel Krämer',
        'Black Mountain (Alaska)', 'SFIAC',
        '2010–11 UEFA Europa League qualifying phase',
        'Polaris (American band)', 'SS Theodore Foster',
        'Monkseaton metro station',
        '2015–16 UEFA Europa League qualifying phase', 'Mariusz Rytkowski',
        'Kalle Samooja', 'Nicole Dunki', 'Mewkledreamy', 'Agustín Vernice',
        'Bobby Edwards (soccer)', 'Li Un-hwa', 'Einar Ortiz',
        'Hernan Emanuel Urra', 'Sophie Molan', 'Karamdaata (1986 film)',
        'A. Veerappan (actor)', 'The Laureate'
    ]
    titles = [
        'Sarah Kelly (bowls)', 'Chung Sum Wai (Tai Hang)',
        'Engelbrekt Parish', 'Karapet Chalyan',
        "2018 Men's Asian Champions Trophy", 'Better Days (Pete Murray song)',
        'Oliver Colina', 'Days (film)', 'Enrique Claverol'
    ]
    title2id = get_page_ids_from_titles(titles, batch_size=10)
    print(title2id)