## Developer setup — dbt-watsonx-spark

This guide helps new contributors get a local development environment ready for building, testing, and contributing to `dbt-watsonx-spark`.

Goal: install the project in editable mode, run unit tests, and (optionally) run functional tests using Docker.

Assumptions
- You're on Linux, macOS, or Windows Subsystem for Linux.
- Python 3.9+ is available (project classifiers indicate support for 3.9–3.12). See `python_requires` in `setup.py`.
- You have Git installed.

Quick start

1. Clone the repo

```bash
git clone https://github.com/ibm/dbt-watsonx-spark.git
cd dbt-watsonx-spark
```

2. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

3. Upgrade packaging tools and install requirements

```bash
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

4. Install the package in editable mode

```bash
pip install -e .
```

This installs the adapter and its dependencies so changes in the repository are available immediately to Python.

Running tests

- Unit tests

```bash
pytest tests/unit -q
```

Developer tips
- If you change packaging metadata or dependencies, re-run `pip install -e .`.
- Keep your virtualenv active while working locally to avoid system package interference.
- If you need to run a specific functional test module, run pytest with a path or `-k` expression.

Reporting problems
- If contributors run into unexpected Spark errors, include: Python version, output of `pip freeze`.

Further reading
- Project README: `README.md`
- Contribution guidelines: `CONTRIBUTING.md`
