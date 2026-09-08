# Lighting Switcher Reconnect and Energy Hardening

Base commit: `28fd191dddc0e1a4485fca5b57096954b91e1390`

## Red

Added broker-free tests before implementation changes and ran:

`python3 -m unittest debug/test_demo_lighting_switcher.py -v`

Result: 38 tests ran, with 4 failures and 2 errors. The failures covered powered-on invalid elapsed values and `[1, 1]` SUBACK grants. The errors showed raw configuration and TLS initialization exceptions. Lifecycle tests also exposed missing callback registration.

## Green

After implementation:

`python3 -m unittest debug/test_demo_lighting_switcher.py -v`

Result: 38 tests passed.

The final verification also includes `py_compile` and `git diff --check`, both with exit code 0.

Implementation commit: `7121cda`
