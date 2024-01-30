import unittest
from scripts.process_csv import extract_company_from_email


class TestExtractCompanyFromEmail(unittest.TestCase):
    def test_extract_company(self):
        self.assertEqual(extract_company_from_email("user@example.com"), "example")

    def test_extract_company_with_subdomain(self):
        self.assertEqual(extract_company_from_email("user@mail.example.com"), "mail")

    def test_invalid_email(self):
        self.assertEqual(extract_company_from_email("userexample.com"), "NA")

    def test_empty_email(self):
        self.assertEqual(extract_company_from_email("userexample.com"), "NA")


# This allows the test to be run from the command line
if __name__ == "__main__":
    unittest.main()
