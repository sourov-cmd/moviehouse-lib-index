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

    def test_a_list_of_languages_is_the_original_in_its_first(self):
        # live MovieBox, 2026-10-12: both V for Vendetta copies list "English, French, Latin, Spanish"
        hits = [mb("V for Vendetta [Hindi]", "2006", "English, French, Latin, Spanish"), mb("V for Vendetta", "2006", "English, French, Latin, Spanish")]
        self.assertTrue(covered_by(lib("V for Vendetta", "2006", "Hindi & English (Dual Audio)"), hits))
        self.assertIsNone(covered_by(lib("V for Vendetta", "2006", "Bengali"), hits))
        hits = [mb("Kadaram Kondan", "2019", "Tamil, Telugu")]
        self.assertTrue(covered_by(lib("Kadaram Kondan", "2019", "Tamil"), hits))
        self.assertIsNone(covered_by(lib("Kadaram Kondan", "2019", "Hindi"), hits), "a Hindi dub MovieBox lacks stays")

    def test_unknown_original_beside_a_tagged_copy(self):
        hits = [mb("Jawan", "2023", None), mb("Jawan [Bengali]", "2023", None)]
        self.assertTrue(covered_by(lib("Jawan", "2023", "Bangla Dubbed"), hits))
        self.assertTrue(covered_by(lib("Jawan", "2023", "Hindi"), hits))


class Dups(unittest.TestCase):
    def test_direct_files_win_and_series_keep_their_seasons(self):
        t = [
            {"num": 1, "type": 1, "title": "Bidai Byomkesh", "year": 2018, "language": "Bangla", "playable": 2, "files_n": 3, "first_seen": 1, "adult": 0},
            {"num": 2, "type": 1, "title": "Bidai Byomkesh", "year": 2018, "language": "Bengali", "playable": 1, "files_n": 3, "first_seen": 2, "adult": 0},
            {"num": 3, "type": 2, "title": "Show", "year": 2020, "language": "Hindi", "playable": 1, "files_n": 9, "first_seen": 1, "adult": 0, "seasons": [1]},
            {"num": 4, "type": 2, "title": "Show", "year": 2020, "language": "Hindi", "playable": 1, "files_n": 3, "first_seen": 2, "adult": 0, "seasons": [2]},
            {"num": 5, "type": 2, "title": "Show", "year": 2020, "language": "Hindi", "playable": 1, "files_n": 2, "first_seen": 3, "adult": 0, "seasons": [1]},
        ]
        # one card per show: season 2's post (4) joins the main card too, and the worker lists both seasons on it
        self.assertEqual(dup_groups(t), {1: 2, 4: 3, 5: 3})

    def test_a_show_posted_season_by_season_is_one_card(self):
        t = [
            {"num": 10, "type": 2, "title": "XO, Kitty", "year": 2025, "language": "Hindi Dubbed + English (Dual Audio)", "tmdb_id": 195670, "playable": 1, "files_n": 3, "first_seen": 1, "adult": 0, "seasons": [2]},
            {"num": 11, "type": 2, "title": "XO, Kitty", "year": 2026, "language": "Hindi Dubbed + English (Dual Audio)", "tmdb_id": 195670, "playable": 1, "files_n": 3, "first_seen": 2, "adult": 0, "seasons": [3]},
            {"num": 12, "type": 2, "title": "The Boys", "year": 2019, "language": "Hindi-English", "tmdb_id": 76479, "playable": 1, "files_n": 24, "first_seen": 1, "adult": 0, "seasons": [1, 2, 3, 4]},
            {"num": 13, "type": 2, "title": "The Boys", "year": 2026, "language": "Hindi Dubbed + English", "tmdb_id": 76479, "playable": 1, "files_n": 32, "first_seen": 2, "adult": 0, "seasons": [5]},
        ]
        self.assertEqual(dup_groups(t), {10: 11, 13: 12})

    def test_shows_that_only_share_a_name_stay_apart(self):
        t = [
            {"num": 20, "type": 2, "title": "The Office", "year": 2005, "language": "English", "tmdb_id": 2316, "playable": 1, "files_n": 9, "first_seen": 1, "adult": 0, "seasons": [1]},
            {"num": 21, "type": 2, "title": "The Office", "year": 2001, "language": "English", "tmdb_id": 2996, "playable": 1, "files_n": 9, "first_seen": 2, "adult": 0, "seasons": [1]},
            {"num": 22, "type": 2, "title": "Undekhi", "year": 2020, "language": "Hindi", "tmdb_id": 105759, "playable": 1, "files_n": 6, "first_seen": 1, "adult": 0, "seasons": [2]},
            {"num": 23, "type": 2, "title": "Undekhi", "year": 2020, "language": "Bengali Dubbed", "tmdb_id": 105759, "playable": 1, "files_n": 6, "first_seen": 2, "adult": 0, "seasons": [3]},
            {"num": 24, "type": 1, "title": "Indian", "year": 1996, "language": "Hindi", "tmdb_id": 1, "playable": 1, "files_n": 4, "first_seen": 1, "adult": 0},
            {"num": 25, "type": 1, "title": "Indian", "year": 2001, "language": "Hindi", "tmdb_id": 1, "playable": 1, "files_n": 2, "first_seen": 2, "adult": 0},
        ]
        self.assertEqual(dup_groups(t), {})

    def test_key(self):
        self.assertEqual(key("Wednesday S2"), "wednesday")
        self.assertEqual(key("K.G.F: Chapter 2"), key("KGF: Chapter 2"))


if __name__ == "__main__":
    unittest.main()
