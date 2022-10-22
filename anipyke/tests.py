from .lib import *
import unittest

class TestFilepathToAnipikePath(unittest.TestCase):
    def test_filepath_to_anipike_path_1(self):
        self.assertEqual(filepath_to_anipike_path("websites/www.anime.jyu.fi:80/20000407224235/~anipike/a/b.html"), "a/b.html")

    def test_filepath_to_anipike_path_2(self):
        self.assertEqual(filepath_to_anipike_path("websites/www.anipike.com/20000407224235/a/c.html"), "a/c.html")

    def test_filepath_to_anipike_path_3(self):
        self.assertEqual(filepath_to_anipike_path("manual_websites/DOKAN13.iso_Webs/www.anipike.com/a/d.html"), "a/d.html")

    def test_filepath_to_anipike_path_4(self):
        self.assertEqual(filepath_to_anipike_path("manual_websites/www.geocities.com/Tokyo"), None)


if __name__ == '__main__':
    unittest.main()