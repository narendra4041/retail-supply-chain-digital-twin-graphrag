from pathlib import Path

import pandas as pd


def main() -> None:
    base_path = Path("data/synthetic/master")

    datasets = [
        "suppliers",
        "products",
        "warehouses",
        "stores",
        "customers",
    ]

    for dataset in datasets:
        file_path = base_path / dataset / f"{dataset}.parquet"
        df = pd.read_parquet(file_path)

        print("\n" + "=" * 80)
        print(dataset.upper())
        print("=" * 80)
        print(f"Rows: {len(df):,}")
        print(df.head())


if __name__ == "__main__":
    main()