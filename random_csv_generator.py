import random
import pandas as pd
import string


# Function to generate random data
def generate_random_data(num_rows: int) -> pd.DataFrame:
    """
    Generates a DataFrame with random data.

    Args:
        num_rows (int): The number of rows of random data to generate.

    Returns:
        pd.DataFrame: A DataFrame containing the generated random data.
    """
    # Example columns for the generated data
    columns = ["id", "name", "age", "email", "date_of_birth", "city"]

    # Generate random data for each column
    data = []
    for _ in range(num_rows):
        row = {
            "id": random.randint(1, 1000),  # Random ID between 1 and 1000
            "name": "".join(
                random.choices(string.ascii_uppercase + string.ascii_lowercase, k=8)
            ),  # Random name (8 characters)
            "age": random.randint(18, 99),  # Random age between 18 and 99
            "email": "".join(
                random.choices(string.ascii_lowercase + string.digits, k=8)
            )
            + "@example.com",  # Random email
            "date_of_birth": f"{random.randint(1, 31):02d}/{random.randint(1, 12):02d}/{random.randint(1960, 2005)}",  # Random DOB (between 1960 and 2005)
            "city": random.choice(
                [
                    "New York",
                    "Los Angeles",
                    "Chicago",
                    "Houston",
                    "Phoenix",
                    "Philadelphia",
                    "San Antonio",
                ]
            ),  # Random city
        }
        data.append(row)

    # Create a DataFrame from the data
    df = pd.DataFrame(data, columns=columns)
    return df


# Function to save the random data to a CSV file
def save_data_to_csv(df: pd.DataFrame, filename: str):
    """
    Saves the DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        filename (str): The name of the CSV file to save the data in.
    """
    df.to_csv(filename, index=False)
    print(f"Data has been saved to {filename}")


# Main function to generate random data and save it as a CSV
def generate_and_save_random_csv(num_rows: int, filename: str):
    """
    Generates random data and saves it to a CSV file.

    Args:
        num_rows (int): The number of rows of data to generate.
        filename (str): The name of the CSV file to save the data in.
    """
    df = generate_random_data(num_rows)
    save_data_to_csv(df, filename)


# Example usage: Generate a CSV file with 100 rows of random data
generate_and_save_random_csv(100, "random_data_1.csv")
generate_and_save_random_csv(120, "random_data_2.csv")
