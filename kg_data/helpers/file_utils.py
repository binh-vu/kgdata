import bz2, gzip


def get_open_fn(infile: str):
    """Get the correct open function for the input file based on its extension. Supported bzip2, gz
    
    Parameters
    ----------
    infile : str
        the file we wish to open
    
    Returns
    -------
    Callable
        the open function that can use to open the file
    
    Raises
    ------
    ValueError
        when encounter unknown extension
    """
    if infile.endswith(".bz2"):
        return bz2.open
    elif infile.endswith(".gz"):
        return gzip.open
    else:
        return open