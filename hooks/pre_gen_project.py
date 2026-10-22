import re
import sys

IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")

for label, value in (
    ("project_slug", "{{ cookiecutter.project_slug }}"),
    ("landing_namespace", "{{ cookiecutter.landing_namespace }}"),
):
    if not IDENTIFIER.match(value):
        print(f"ERROR: {label} '{value}' must match {IDENTIFIER.pattern} (lowercase letters, digits, underscores).")
        sys.exit(1)
