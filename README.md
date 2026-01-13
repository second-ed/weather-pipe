

# Repo map
```
├── .github
│   └── workflows
│       ├── ci_tests.yaml
│       └── run_raw_layer.yaml
├── configs
│   ├── config.yaml
│   └── raw_layer.yaml
├── docs
│   └── dev_readme.md
├── src
│   └── weather_pipe
│       ├── v2
│       │   ├── adapters
│       │   │   ├── __init__.py
│       │   │   └── io_funcs.py
│       │   ├── core
│       │   │   ├── __init__.py
│       │   │   ├── constants.py
│       │   │   ├── event.py
│       │   │   ├── logger.py
│       │   │   └── message_bus.py
│       │   ├── layers
│       │   │   ├── raw
│       │   │   │   ├── __init__.py
│       │   │   │   ├── data_structures.py
│       │   │   │   ├── pipe.py
│       │   │   │   └── transform.py
│       │   │   └── __init__.py
│       │   ├── pipelines
│       │   │   ├── __init__.py
│       │   │   └── raw_pipe.py
│       │   └── __init__.py
│       └── __init__.py
├── tests
│   ├── v2
│   │   ├── pipelines
│   │   │   ├── __init__.py
│   │   │   └── test_raw_pipe.py
│   │   └── __init__.py
│   ├── __init__.py
│   └── conftest.py
├── warehouse
│   └── models
│       └── bronze
│           └── raw_to_bronze.sql
├── .pre-commit-config.yaml
├── README.md
├── dbt_project.yml
├── pyproject.toml
├── ruff.toml
└── uv.lock
::
```
