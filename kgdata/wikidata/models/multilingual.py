from __future__ import annotations

from sm.misc.funcs import filter_duplication
from typing import List


class MultiLingualString(str):
    # two characters language code: en, th, de, fr, etc.
    lang: str
    lang2value: dict[str, str]

    def __new__(cls, lang2value: dict[str, str], lang):
        object = str.__new__(cls, lang2value[lang])
        object.lang = lang
        object.lang2value = lang2value
        return object

    def as_lang(self, lang: str) -> str:
        return self.lang2value[lang]

    def as_lang_default(self, lang: str, default: str) -> str:
        return self.lang2value.get(lang, default)

    def has_lang(self, lang: str) -> bool:
        return lang in self.lang2value

    @staticmethod
    def en(label: str):
        return MultiLingualString(lang2value={"en": label}, lang="en")

    def to_dict(self):
        return {"lang2value": self.lang2value, "lang": self.lang}

    def to_tuple(self):
        return self.lang2value, self.lang

    def __getnewargs__(self) -> tuple[dict[str, str], str]:
        return self.lang2value, self.lang


# important to use List[str] as python 3.8 does not support inherit from list[str]
class MultiLingualStringList(List[str]):
    def __init__(self, lang2values: dict[str, list[str]], lang):
        super().__init__(lang2values[lang])
        self.lang2values = lang2values
        self.lang = lang

    def to_dict(self):
        return {"lang2values": self.lang2values, "lang": self.lang}

    def to_tuple(self):
        return self.lang2values, self.lang

    def flatten(self) -> list[str]:
        """Flatten multilingual lists of strings into a single list of strings. This will lose the language information"""
        return filter_duplication(
            value for values in self.lang2values.values() for value in values
        )
