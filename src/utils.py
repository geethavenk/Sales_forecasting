import pandas as pd
import numpy as np


def merge_df(df, holidays, oil, stores, transactions):
    """
    Merge data with other informative dataframes.

    Args:
    df: pd.DataFrame
        Main dataframe with which other dataframes should be merged.
    holidays: pd.DataFrame
        Dataframe containing holidays information.
    oil: pd.DataFrame 
        Dataframe with daily oil prices.
    stores: pd.DataFrame
        Stores metadata including city, state, type and cluster.
    transactions: pd.DataFrame
        Dataframe with transactions data.

    Returns
    df: pd.DataFrame
        Merged dataframe    

    """
    try:
        df = df.merge(holidays, on='date', how='left')
        df = df.merge(oil, on='date', how='left')
        df = df.merge(stores, on='store_nbr', how='left')
        df = df.merge(transactions, on=['date', 'store_nbr'], how='left')
        return df
    except Exception as e:
        print(f'Error occured during merging dataframes, {str(e)}')


def info_table(df, num_unique_threshold = 50):
    """
    Comprehensive DataFrame Summary Table Generator
    
    For a given dataframe, this function creates a table with column names, data types, count, mean, 
    standard deviation, minimum, 25%, 50%, 75% quantiles, maximum, number of unique values, 
    unique values list, number of null values and 
    percentage of null values.

    Args:
    df: pd.DataFrame
        Input dataframe.
    num_unique_threshold: int, optional (default = 50)  
        Threshold for listing unique values.

    Returns:
    df_info: pd.DataFrame
        A summary table with detailed statistics for each column.

    Raises:
    Exception
        If a error occurs while computing the details.    

    """
    try:
        df_info = pd.DataFrame({
            'column': df.columns,
            'dtype': df.dtypes.values,
            'count': df.count().values,
            'mean': df.apply(lambda col: col.mean() if pd.api.types.is_numeric_dtype(col) else np.nan),
            'std': df.apply(lambda col: round(col.std(),2) if pd.api.types.is_numeric_dtype(col) else np.nan),
            'min': df.apply(lambda col: col.min() if pd.api.types.is_numeric_dtype(col) else np.nan),
            '25%': df.apply(lambda col: col.quantile(0.25) if pd.api.types.is_numeric_dtype(col) else np.nan),
            '50%': df.apply(lambda col: col.quantile(0.5) if pd.api.types.is_numeric_dtype(col) else np.nan),
            '75%': df.apply(lambda col: col.quantile(0.75) if pd.api.types.is_numeric_dtype(col) else np.nan),
            'max': df.apply(lambda col: col.max() if pd.api.types.is_numeric_dtype(col) else np.nan),
            'nunique': df.nunique().values,
            'unique_values': df.apply(lambda col : col.unique() if col.nunique() < num_unique_threshold else ''),
            'num_null_values': df.isna().sum().values,
            'null_perct': (df.isna().sum().values/df.shape[0]*100).round(2)

        })

        return df_info
    
    except Exception as e:
        print(f'Error occurred while computing information for the dataframe: {str(e)}')
