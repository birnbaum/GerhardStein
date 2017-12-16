import unittest
from .generate_dataset import clean_tags

class TestCleanTags(unittest.TestCase):

    def test_normal_string(self):
        self.assertEqual(clean_tags('Test 123'), 'Test 123')

    def test_single_tag(self):
        self.assertEqual(clean_tags('@Peter'), '')

    def test_double_space(self):
        self.assertEqual(clean_tags('@Peter  what up test'), 'what up test')

    def test_double_name(self):
        self.assertEqual(clean_tags('@Peter Müller what up test'), 'what up test')

    def test_space_plus_double_space(self):
        self.assertEqual(clean_tags('@ Peter  what up test'), 'what up test')

    def test_space_plus_double_name(self):
        self.assertEqual(clean_tags('@ Peter Müller what up test'), 'what up test')

    def test_colon(self):
        self.assertEqual(clean_tags('@Peter: what up test'), 'what up test')

    def test_space_plus_colon(self):
        self.assertEqual(clean_tags('@ Peter: what up test'), 'what up test')

    def test_double_name_and_colon(self):
        self.assertEqual(clean_tags('@Peter Müller: what up test'), 'what up test')

    def test_space_plus_double_name_and_colon(self):
        self.assertEqual(clean_tags('@ Peter Müller: what up test'), 'what up test')

    def test_comma(self):
        self.assertEqual(clean_tags('@Peter, what up test'), 'what up test')

    def test_dot(self):
        self.assertEqual(clean_tags('@Peter. what up test'), 'what up test')

    def test_tag_not_at_the_start(self):
        self.assertEqual(clean_tags('Hallo @Peter Müller what up test'), 'Hallo Peter Müller what up test')



if __name__ == '__main__':
    unittest.main()