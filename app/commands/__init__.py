"""User commands package (step 3): RESET / LIST / TEMP / PROMPT / HELP.

:mod:`.handler` parses commands (case-insensitive, before any LLM call) and
executes them against the session store, producing SMS-sized replies. The
submodule is imported here so ``commands.handler`` is always addressable,
independent of module import order.
"""

from . import handler  # noqa: F401
