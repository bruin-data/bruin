""" @bruin

name: python_test
type: python
rerun_cooldown: 900

@bruin """

import pandas as pd
import time

def main():
    print("Running Python asset for rerun cooldown test")
    
    # Simple data processing
    data = {
        'id': [1, 2, 3],
        'value': ['python_test_1', 'python_test_2', 'python_test_3'],
        'timestamp': pd.Timestamp.now()
    }
    
    df = pd.DataFrame(data)
    print(f"Created DataFrame with {len(df)} rows")
    
    return df

if __name__ == "__main__":
    main()