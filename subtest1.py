from prefect import task


@task
def get_rows():
    row_labels = [101, 102, 103, 104, 105, 106, 107]
    return row_labels
