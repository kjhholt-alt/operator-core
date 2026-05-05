# Recipe Redirects

These `run-<name>.bat` files are drop-in replacements for the legacy
`operator-scripts/run-*.bat` shims. Each one calls
`operator recipe run <name>` instead of the old standalone script.

## Migration

The legacy files in `C:\Users\Kruz\Desktop\Projects\operator-scripts\`
can be replaced one-for-one with the matching redirect here, OR the
Windows Task Scheduler entries can be re-pointed via:

```powershell
operator schedule install
```

That command reads `schedules/schedule.yaml` and re-registers every
enabled recipe as a task named `operator-recipe-<name>` -- which makes
the .bat redirects redundant. See `operator schedule list` for the
current view of installed tasks.

## Why both?

Some users prefer the explicit per-recipe `.bat` invocation in Task
Scheduler so they can edit each task's working directory, log path,
or run-as account independently. The redirects exist for that
workflow.
