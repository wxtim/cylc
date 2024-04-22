The workflow-state command and xtrigger now looks for task outputs (a.k.a triggers)
not the corresponding task messages, as well as task status. This is a consequence
of the new optional output support in Cylc 8.
