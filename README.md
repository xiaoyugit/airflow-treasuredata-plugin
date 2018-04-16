## Airflow plugin for TreasureData

### Introduction
A plugin for interacting airflow and treasuredata to make our life easier, including two components:

- TreasureHook
- TreasureToPostgresOperator

### Deploy
- Git clone this repository
- Move treasuredata_plugin to {AIRFLOW_PLUGINS_FOLDER}
- Restart airflow

### Usage
```python
from airflow.hooks import TreasureHook
from airflow.operators import TreasureToPostgresOperator
```