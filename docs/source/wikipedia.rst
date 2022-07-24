Wikipedia
=========

Here is a list of dumps that you need to download depending on the
database/files you want to build:

1. `Static HTML
   Dumps <https://dumps.wikimedia.org/other/enterprise_html/>`__: they
   only dumps some namespaces. The namespace that you likely to use is 0
   (main articles). For example,
   enwiki-NS0-20220420-ENTERPRISE-HTML.json.tar.gz.

Then, execute the following steps:

1. Extract HTML Dumps:

   -  ``kgdata wikipedia -d <wikipedia_dir> enterprise_html_dumps``