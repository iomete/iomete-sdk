# DEVELOPMENT NOTES

**Prepare the dev environment**

```shell
virtualenv .env #or python3 -m venv .env
source .env/bin/activate

pip install -e ."[dev]"
```

**Run Test Suit**

- Update configs in `tests/__init__.py`
- Run all the tests
  ```shell
  pytest
  ```
