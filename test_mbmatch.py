"""The shelf rule must say exactly what the app's LibMerge.keep says (its unit tests, ported). Run: python3 -m unittest test_mbmatch"""
import unittest
from mbmatch import covered_by, dup_groups, key

LANG = {"en": "English", "hi": "Hindi", "te": "Telugu", "bn": "Bengali", None: ""}


def mb(title, year, lang, type_=1, has=True):
    return {"title": title, "releaseDate": f"{year}-01-01" if year else "", "language": LANG.get(lang, lang), "subjectType": type_, "hasResource": has}


def lib(title, year, lang, type_=1, aka=None):
    return {"title": title, "aka": aka, "year": int(year) if year else None, "language": lang, "type": type_}


class LibMergeParity(unittest.TestCase):
    def test_library_only_title_is_kept(self):
        self.assertIsNone(covered_by(lib("Nilar Baba", "2011", "Bengali"), [mb("Oppenheimer", "2023", "en")]))

    def test_same_work_in_a_covered_language_is_hidden(self):
        hits = [mb("The Man from Toronto", "2022", "en"), mb("The Man from Toronto [Hindi]", "2022", "hi")]
        self.assertTrue(covered_by(lib("The Man from Toronto", "2022", "Hindi-English"), hits))

    def test_bengali_dub_beside_the_original_is_kept(self):
        self.assertIsNone(covered_by(lib("The Man from Toronto", "2022", "Bengali"), [mb("The Man from Toronto", "2022", "en")]))

    def test_a_language_moviebox_lacks_is_kept(self):
        self.assertIsNone(covered_by(lib("Pushpa 2", "2024", "Hindi"), [mb("Pushpa 2", "2024", "te")]))

    def test_unlabelled_moviebox_copy_is_the_original(self):
        hits = [mb("Jawan", "2023", None)]
        self.assertTrue(covered_by(lib("Jawan", "2023", "Hindi"), hits))
        self.assertIsNone(covered_by(lib("Jawan", "2023", "Bangla Dubbed"), hits))

    def test_years_apart_or_another_kind(self):
        self.assertIsNone(covered_by(lib("Dune", "1984", "English"), [mb("Dune", "2021", "en")]))
        self.assertIsNone(covered_by(lib("Dune", "2021", "English", 2), [mb("Dune", "2021", "en")]))

    def test_coming_soon_never_hides(self):
        self.assertIsNone(covered_by(lib("Gopal Bhar", "2015", "Bengali", 2), [mb("Gopal bhar", "2015", "bn", 2, has=False)]))

    def test_no_language_is_treated_as_the_same(self):
        self.assertTrue(covered_by(lib("Dune", "2021", None), [mb("Dune", "2021", "en")]))

    def test_other_name_matches(self):
        # the Library's "Nilar Baba" is MovieBox's "Deiva Thirumagal [Bengali]" (checked live 2026-10-12)
        hits = [mb("Deiva Thirumagal", "2011", "Tamil"), mb("Deiva Thirumagal [Bengali]", "2011", "Tamil")]
        self.assertTrue(covered_by(lib("Nilar Baba", "2011", "Bengali Dubbed", aka="Deiva Thirumagal"), hits))

    def test_a_subtitle_on_either_side(self):
        hits = [mb("Khalifa", "2026", "Malayalam"), mb("Khalifa [Hindi]", "2026", "Malayalam")]
        self.assertTrue(covered_by(lib("Khalifa: The Ruler", "2026", "Hindi & Malayalam (Dual Audio)"), hits))
        self.assertIsNone(covered_by(lib("Khalifa: The Ruler", "2019", "Hindi"), hits), "the year still decides")
        self.assertIsNone(covered_by(lib("Up in the Air", "2009", "Hindi & English (Dual Audio)"), [mb("Up in the Air", "2009", "en")]),
                          "a Hindi dub MovieBox lacks stays")

    def test_multi_language_catalogue_value_names_nothing(self):
        # MbRegion.language("Tamil, Telugu") == "" -> the row counts as the original
        hits = [mb("Kadaram Kondan", "2019", "Tamil, Telugu")]
        self.assertTrue(covered_by(lib("Kadaram Kondan", "2019", "Hindi"), hits) is not None)


class Dups(unittest.TestCase):
    def test_direct_files_win_and_series_keep_their_seasons(self):
        t = [
            {"num": 1, "type": 1, "title": "Bidai Byomkesh", "year": 2018, "language": "Bangla", "playable": 2, "files_n": 3, "first_seen": 1, "adult": 0},
            {"num": 2, "type": 1, "title": "Bidai Byomkesh", "year": 2018, "language": "Bengali", "playable": 1, "files_n": 3, "first_seen": 2, "adult": 0},
            {"num": 3, "type": 2, "title": "Show", "year": 2020, "language": "Hindi", "playable": 1, "files_n": 9, "first_seen": 1, "adult": 0, "seasons": [1]},
            {"num": 4, "type": 2, "title": "Show", "year": 2020, "language": "Hindi", "playable": 1, "files_n": 3, "first_seen": 2, "adult": 0, "seasons": [2]},
            {"num": 5, "type": 2, "title": "Show", "year": 2020, "language": "Hindi", "playable": 1, "files_n": 2, "first_seen": 3, "adult": 0, "seasons": [1]},
        ]
        self.assertEqual(dup_groups(t), {1: 2, 5: 3})

    def test_key(self):
        self.assertEqual(key("Wednesday S2"), "wednesday")
        self.assertEqual(key("K.G.F: Chapter 2"), key("KGF: Chapter 2"))


if __name__ == "__main__":
    unittest.main()
