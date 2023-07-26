from typing import Optional
from urllib.parse import unquote, urlparse


def get_title_from_url(url: str, prefix: str = "/wiki/") -> str:
    """This function converts a wikipedia page/article's URL to its title. The function is tested manually in `20200425-wikipedia-links` notebook in section 2.2.

    Args:
        url: A wikipedia page/article's URL.
        prefix: The prefix of the URL that is removed to get the title.

    Returns:
        A wikipedia page/article's title.
    """
    if ";" in url:
        # ; is old standard to split the parameter (similar to &) and is obsolete.
        # python 3.9, however, is still splitting it under this scheme http:// or https://
        # therefore, we need to address this special case by replacing the scheme.
        if url.startswith("http://") or url.startswith("https://"):
            url = "scheme" + url[4:]
        parsed_url = urlparse(url)
        path = parsed_url.path
        if ";" not in path:
            # if ";" not in path, it must be preserved in params, query or fragment
            n_semicolon = url.count(";")
            assert (
                parsed_url.params.count(";")
                + parsed_url.query.count(";")
                + parsed_url.fragment.count(";")
                == n_semicolon
            ), (url, path)
    else:
        path = urlparse(url).path

    if not path.startswith(prefix):
        return ""

    assert path.startswith(prefix), path
    path = path[len(prefix) :]
    title = unquote(path).replace("_", " ")
    return title.strip()


def is_wikipedia_url(url: str, lang: Optional[str] = None) -> bool:
    """This function checks if a URL is a wikipedia URL.

    Args:
        url: A URL.
        lang: A 2-char language code (e.g., en). If None, all languages are acceptable.
    """
    host = "wikipedia.org" if lang is None else f"{lang}.wikipedia.org"
    return urlparse(url).netloc.endswith(host)


def get_path(url: str) -> str:
    """This function returns the path of a URL.

    Args:
        url: A URL.
    """
    if ";" in url:
        # ; is old standard to split the parameter (similar to &) and is obsolete.
        # python 3.9, however, is still splitting it under this scheme http:// or https://
        # therefore, we need to address this special case by replacing the scheme.
        if url.startswith("http://") or url.startswith("https://"):
            url = "scheme" + url[4:]
        parsed_url = urlparse(url)
        path = parsed_url.path
        if ";" not in path:
            # if ";" not in path, it must be preserved in params, query or fragment
            n_semicolon = url.count(";")
            assert (
                parsed_url.params.count(";")
                + parsed_url.query.count(";")
                + parsed_url.fragment.count(";")
                == n_semicolon
            ), (url, path)
    else:
        path = urlparse(url).path
