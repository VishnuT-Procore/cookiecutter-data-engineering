import re
import sys

SLUG = "{{ cookiecutter.project_slug }}"
RAW_SCHEMA = "{{ cookiecutter.raw_schema }}"

IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")

for label, value in (("project_slug", SLUG), ("raw_schema", RAW_SCHEMA)):
    if not IDENTIFIER.match(value):
        print(f"ERROR: {label} '{value}' must match {IDENTIFIER.pattern} (lowercase letters, digits, underscores).")
        sys.exit(1)
