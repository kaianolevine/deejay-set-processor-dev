# Python Project Template

[![Build](https://github.com/kaianolevine/python-project-template/actions/workflows/test.yml/badge.svg)](https://github.com/kaianolevine/python-project-template/actions/workflows/test.yml)
[![Coverage](https://img.shields.io/badge/coverage-auto--updated-brightgreen.svg)](https://github.com/kaianolevine/python-project-template)
[![Version](https://img.shields.io/github/v/tag/kaianolevine/python-project-template?label=version)](https://github.com/kaianolevine/python-project-template/releases)

A batteries-included Python template wired like **kaiano-common-utils** — with Poetry, pre-commit, tests, coverage, and CI/CD.  
It also depends on your shared library: **kaiano-common-utils** (tracking `main`).

---

## 🧑‍💻 Local Development Setup

### 1) Prerequisites
- Python ≥ 3.10 (tested on 3.13)
- Poetry ≥ 1.8 (`pip install poetry`)
- Git

### 2) Clone & install
```bash
git clone git@github.com:kaianolevine/python-project-template.git
cd python-project-template
poetry install
```

### 3) Enable pre-commit
```bash
poetry run pre-commit install
poetry run pre-commit run --all-files   # optional first sweep
```

### 4) Run tests & coverage
```bash
poetry run pytest --cov=project_name --cov-report=term-missing
```

### 5) Formatting
```bash
poetry run black . && poetry run isort . && poetry run flake8
```

---

## 🚀 Quickstart (One-Liner Bootstrap)
```bash
curl -sSL https://install.python-poetry.org | python3 - && export PATH="$HOME/.local/bin:$PATH" && poetry install && poetry run pre-commit install && poetry run pytest --maxfail=1 --disable-warnings -q
```

---

## 🔗 Using kaiano-common-utils
This template pulls your shared utilities directly from GitHub:
```toml
kaiano-common-utils = { git = "https://github.com/kaianolevine/kaiano-common-utils.git", branch = "main" }
```

Example usage:
```python
from kaiano_common_utils import helpers
```

---

## 🏷️ Versioning & CI
- **Auto Version Bump**: every push to `main` increments PATCH and tags `vX.Y.Z`.
- **CI**: every push/PR runs pre-commit, tests, and coverage.

If your repo doesn’t expose “Workflow permissions,” this template’s `version-bump.yml` already requests `contents: write` so tags can be pushed.

---

## 🪄 Initialize a New Project (Rename)
Use the included script to rename the placeholder package (`project_name`) to your new name:
```bash
python init_project.py my_new_project
```

What it updates:
- `src/project_name` → `src/my_new_project`
- `tests/project_name` → `tests/my_new_project`
- `pyproject.toml` `[tool.poetry].name` and package path
- Basic import paths in tests

Then run:
```bash
poetry install
pre-commit install
poetry run pytest
```

---

## 🧱 Build Locally
```bash
poetry build
```
Creates `dist/*.whl` and `dist/*.tar.gz` (no `poetry.lock` committed by default).

---

## 🧾 License
MIT © 2025 Kaiano Levine
