import importlib.util
import pathlib
import sys
import unittest


SCRIPT_PATH = pathlib.Path(__file__).with_name("seed_powermonitor.py")


def load_seed_module():
    spec = importlib.util.spec_from_file_location("seed_powermonitor", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class PowerMonitorSeedTests(unittest.TestCase):
    def test_demo_topology_contains_profiles_assets_and_devices_for_power_monitor(self):
        module = load_seed_module()

        self.assertEqual(
            {profile["name"] for profile in module.device_profiles()},
            {"PowerSwitcher", "PowerMeter", "LightSwitch"},
        )
        self.assertEqual(
            {profile["name"] for profile in module.asset_profiles()},
            {"Site", "ElectricalPanel", "LoadEquipment"},
        )
        self.assertEqual(len(module.demo_assets()), 4)
        self.assertEqual(len(module.demo_devices()), 4)
        switcher_profile = next(
            profile
            for profile in module.device_profiles()
            if profile["name"] == "PowerSwitcher"
        )
        self.assertIn("brightness_pct", switcher_profile["telemetry_schema"])
        self.assertIn("set_brightness", switcher_profile["reporting_settings"]["rpc"]["methods"])
        self.assertEqual(
            {device["device_id"] for device in module.demo_devices()},
            {
                "demo-switcher-01",
                "demo-meter-01",
                "demo-pump-meter-01",
                "demo-light-switcher-01",
            },
        )
        lighting = next(
            device
            for device in module.demo_devices()
            if device["device_id"] == "demo-light-switcher-01"
        )
        self.assertEqual(lighting["device_profile_id"], module.LIGHT_SWITCH_PROFILE_ID)

    def test_demo_events_include_power_switcher_state_and_power_metrics(self):
        module = load_seed_module()
        events = module.demo_events(module.rounded_now())

        switcher = [event for event in events if event["device_id"] == "demo-switcher-01"]
        meter = [event for event in events if event["device_id"] == "demo-meter-01"]

        self.assertEqual(len(switcher), 24)
        self.assertEqual(len(meter), 24)
        self.assertEqual(len(events), 96)
        self.assertIn("switch_state", switcher[-1]["measurements"])
        self.assertTrue(switcher[-1]["measurements"]["switch_state"])
        self.assertGreater(switcher[-1]["measurements"]["power_w"], 0)
        self.assertGreater(meter[-1]["measurements"]["power_w"], 0)

    def test_timescale_reset_keeps_users_but_clears_power_monitor_domain_tables(self):
        module = load_seed_module()
        sql = module.timescale_reset_sql()

        self.assertIn("TRUNCATE", sql)
        self.assertIn("telemetry", sql)
        self.assertIn("device_profiles", sql)
        self.assertIn("resource_shares", sql)
        self.assertIn("device_claim_codes", sql)
        self.assertNotIn("users", sql.lower())

    def test_seed_assigns_demo_resources_to_the_default_viewer_when_available(self):
        module = load_seed_module()
        sql = module.timescale_seed_owner_sql()

        self.assertIn("username = 'viewer'", sql)
        self.assertIn("account_class = 'user'", sql)


if __name__ == "__main__":
    unittest.main()
