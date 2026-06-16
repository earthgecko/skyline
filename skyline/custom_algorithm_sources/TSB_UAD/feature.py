"""Classes of feature mapping for model type B
"""
# Author: Yinchen Wu <yinchen@uchicago.edu>

import numpy as np
import pandas as pd
import math
from builtins import range


class Window:
    """
    Construct a pandas DataFrame of shape (n_samples-m,m) corresponding to all consecutive subsequences in the time series
    
    Parameters
    ----------
    window : int
        Subsequence length.
    """  

    def __init__(self,  window = 100):
        self.window = window
    
    def convert(self, X):
        """Convert the time series X into a pandas DataFrame of shape (n_samples-m,m) corresponding to all consecutive subsequences in the time series.
        
        Parameters
        ----------
        X : numpy array of shape (n_samples, )
            The time series to be transformed.
        
        Returns
        -------
        df : pandas DataFrame
            all consecutive subsequences (of length window) in the time series.
        """

        n = self.window
        X = pd.Series(X)
        L = []
        if n == 0:
            df = X
        else:
            for i in range(n):
                L.append(X.shift(i))
            df = pd.concat(L, axis = 1)
            df = df.iloc[n-1:]
        return df

