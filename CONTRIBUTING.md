# Contributing to EduThreat-CTI

Thank you for your interest in contributing to EduThreat-CTI! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Adding New Sources](#adding-new-sources)
- [Development Setup](#development-setup)
- [Pull Request Process](#pull-request-process)
- [Code Style](#code-style)

## Code of Conduct

This project adheres to a code of conduct. By participating, you are expected to uphold this code.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/sagarkishore-7/EduThreat-CTI.git`
3. Create a branch: `git checkout -b feature/your-feature-name`
4. Set up development environment (see [Development Setup](#development-setup))
5. Make your changes
6. Test your changes
7. Submit a pull request

## Adding New Sources

Adding new data sources is one of the most valuable contributions. See [docs/ADDING_SOURCES.md](docs/ADDING_SOURCES.md) for detailed instructions.

### Quick Start

1. **Determine source type**:
   - **Curated**: Sources with dedicated education sector sections (e.g., KonBriefing)
   - **News**: Keyword-based search sources (e.g., The Hacker News)
   - **RSS**: RSS feed sources (e.g., DataBreaches RSS)

2. **Create source builder**:
   - Copy an existing source as a template from `edu_cti/sources/`
   - Implement `build_<source>_incidents()` function
   - Follow the existing patterns

3. **Register source**:
   - Add to appropriate registry in `edu_cti/core/sources.py`
   - Follow naming conventions

4. **Test**:
   - Run source builder function
   - Verify incidents are created correctly
   - Check deduplication works

5. **Document**:
   - Add source to `docs/SOURCES.md`
   - Update README if needed

See [docs/ADDING_SOURCES.md](docs/ADDING_SOURCES.md) for detailed guide with examples.

## Development Setup

```bash
# Clone repository
git clone https://github.com/sagarkishore-7/EduThreat-CTI.git
cd EduThreat-CTI

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
flake8 src/
black src/ --check
```

## Pull Request Process

1. **Update documentation** if needed
2. **Add tests** for new features
3. **Ensure all tests pass**: `pytest`
4. **Update CHANGELOG.md** with your changes
5. **Submit PR** with clear description

### PR Checklist

- [ ] Code follows project style guidelines
- [ ] Tests added/updated and passing
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] No linter errors

## Code Style

- Follow PEP 8
- Use type hints where possible
- Write docstrings for public functions
- Keep functions focused and modular
- Use meaningful variable names

### Formatting

We use Black for code formatting:

```bash
black src/
```

### Linting

We use flake8 for linting:

```bash
flake8 src/
```

## Testing

See [tests/README.md](tests/README.md) for detailed testing documentation.

### Running Tests

```bash
# Run all tests
pytest

# Run Phase 1 tests only
pytest tests/phase1/

# Run Phase 2 tests only
pytest tests/phase2/

# Run with coverage
pytest --cov=src.edu_cti --cov-report=html
```

## Project Structure

```
src/edu_cti/
├── core/              # Core functionality (shared across phases)
│   ├── models.py      # Data models
│   ├── config.py      # Configuration
│   ├── db.py          # Database operations
│   └── ...
├── sources/           # Source implementations
│   ├── curated/       # Curated sources (dedicated education sections)
│   ├── news/          # News sources (keyword-based search)
│   └── rss/           # RSS feed sources
└── pipeline/          # Phase-based pipelines
    ├── phase1/        # Phase 1: Ingestion & Baseline
    └── phase2/        # Phase 2: LLM Enrichment
```

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed architecture documentation.

## Questions?

- Open an issue for questions
- Check existing issues first
- Join discussions in issues

Thank you for contributing to EduThreat-CTI!

