from src.common.schema_validator import validate_event_file


def main() -> None:
    validate_event_file(
        event_file_path="data/sample/order_created_event_sample.json",
        schema_file_path="schemas/events/order_created.schema.json",
    )

    print("Sample order_created event is valid.")


if __name__ == "__main__":
    main()