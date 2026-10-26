"""Reusable DataFrame helpers shared by the silver and gold layers."""


class reusable:
    def dropColumns(self, df, columns):
        """Return `df` without the given columns."""
        return df.drop(*columns)
