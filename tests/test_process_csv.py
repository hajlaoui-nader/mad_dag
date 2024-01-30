import unittest
import pandas as pd
from scripts.process_csv import assure_true_duplicates


class TestAssureTrueDuplicates(unittest.TestCase):
    def test_no_duplicates(self):
        df = pd.DataFrame(
            {
                "id": ["1", "2", "3"],
                "email": ["a@example.com", "b@example.com", "c@example.com"],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        result_df = assure_true_duplicates(df)
        self.assertEqual(len(result_df), 3)
        self.assertListEqual(list(result_df["id"]), ["1", "2", "3"])

    def test_true_duplicates(self):
        df = pd.DataFrame(
            {
                "id": ["1", "1", "2"],
                "email": ["a@example.com", "a@example.com", "b@example.com"],
                "name": ["Alice", "Alice", "Bob"],
            }
        )
        result_df = assure_true_duplicates(df)
        self.assertEqual(len(result_df), 3)
        self.assertListEqual(list(result_df["id"]), ["1", "1", "2"])

    def test_false_duplicates(self):
        df = pd.DataFrame(
            {
                "id": ["1", "1", "2"],
                "email": ["a@example.com", "different@example.com", "b@example.com"],
                "name": ["Alice", "Alice", "Bob"],
            }
        )
        result_df = assure_true_duplicates(df)
        self.assertEqual(len(result_df), 3)
        self.assertEqual(len(result_df["id"].unique()), 3)


if __name__ == "__main__":
    unittest.main()
