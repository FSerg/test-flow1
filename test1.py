import os
import pandas as pd
from prefect import flow, task

from subtest1 import get_rows


@task
def get_data():
    data = {
        'name': ['Xavier', 'Ann', 'Jana', 'Yi', 'Robin', 'Amal', 'Nori'],
        'city': ['Mexico City', 'Toronto', 'Prague', 'Shanghai',
                 'Manchester', 'Cairo', 'Osaka'],
        'age': [41, 28, 33, 34, 38, 31, 37],
        'py-score': [88.0, 79.0, 81.0, 80.0, 68.0, 61.0, 84.0]
    }
    return data


@flow(name="My Test1 Flow")
def test(folder: str = '', filename: str = 'data.csv'):
    df = pd.DataFrame(data=get_data(), index=get_rows())

    full_filename = os.path.join(folder, filename)
    df.to_csv(full_filename)


if __name__ == "__main__":
    test("data", "data.csv")
