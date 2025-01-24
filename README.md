# JSON Data Processing Project

This project processes JSON files containing meter readings and combines them into a pandas DataFrame for analysis.

## Setup

1. Install pipenv if you haven't already:
```bash
pip install pipenv
```

2. Clone this repository and navigate to the project directory

3. Install dependencies:
```bash
pipenv install
```

4. Activate the virtual environment:
```bash
pipenv shell
```

## Usage

Place your JSON files in the `readings` directory and run:
```bash
python test.py
```

This will process all JSON files and output a summary of the data.

## Development

For development, additional tools are included:
- black: Code formatting
- flake8: Code linting
- pytest: Testing

Install development dependencies:
```bash
pipenv install --dev
``` 