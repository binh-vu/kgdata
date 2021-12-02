from urllib.parse import unquote_plus, urlparse


def get_title_from_url(url: str) -> str:
    """This function converts a wikipedia page/article's URL to its title. The function is tested manually in `20200425-wikipedia-links` notebook in section 2.2.

    Args:
        url: A wikipedia page/article's URL.

    Returns:
        A wikipedia page/article's title.
    """
    path = urlparse(url).path
    if not path.startswith("/wiki/"):
        return ""

    assert path.startswith("/wiki/"), path
    path = path[6:]
    title = unquote_plus(path).replace("_", " ")
    return title.strip()
