import pandas as pd


class Tester:
    """Class to organize data tests."""

    @staticmethod
    def test_bank_name(
            df: pd.DataFrame,
            bank_name: str,
            bank_name_col: str):
        """Assert all bank names in `bank_name_col` are the same and equal to `bank_name`."""

        bank_name = bank_name.lower()
        banks_in_column = df[bank_name_col].str.lower().unique()
        if not banks_in_column == [bank_name]:
            raise Exception(f"Different bank name(s) identified. Actual banks: {banks_in_column}. Expected bank: {bank_name}")  # noqa
        assert banks_in_column == [bank_name]
        print('Passed test_bank_name.\n')
