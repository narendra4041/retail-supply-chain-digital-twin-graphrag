from src.common.config_loader import load_config


def test_load_dev_config() -> None:
    config = load_config("dev")

    assert config["environment"] == "dev"
    assert "unity_catalog" in config
    assert "event_hubs" in config
    assert "paths" in config


def test_dev_config_has_required_event_hubs() -> None:
    config = load_config("dev")

    event_configs = config["event_hubs"]["events"]

    assert "order_created" in event_configs
    assert "inventory_updated" in event_configs
    assert "shipment_created" in event_configs
    assert "supplier_performance" in event_configs