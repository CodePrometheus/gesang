# Verification requirements

- Submit a real goal, close the client, restart the host, and retrieve the original goal and actual result from another client.
- Lose a response after durable acceptance, retry the original request, and prove that execution is not duplicated.
- Revoke a task grant while clients are offline and prove that new actions are denied. Previously issued side effects must remain visible and reconcilable.
- Recover a long command by its original operation reference, output cursor, and exit status. An unknown result must never become an automatic unsafe replay.
- Exercise competing controllers, a partitioned old writer, storage handoff, and stale generations. Database lease changes alone do not pass this requirement.
- Interrupt each external-anchor commit boundary and verify accepted-commit freshness, content integrity, and correct recovery without duplicate side effects.
- Replace or disable a plugin during in-flight work; verify dependency shutdown, permission revocation, pinned recovery definitions, and visible failure states.
- Run recursive improvement beside a long goal, preserving independent budgets, evaluation gates, adoption boundaries, and rollback records.
- Verify native release startup, input, scrolling, system navigation, font scaling, accessibility, offline drafts, and notification deep links on Android.
- Establish memory, storage, latency, and package-size baselines under equivalent encryption, durability, isolation, and history workloads. No performance claim is established yet.
- Verify a clean single-host deployment and an embedded cluster deployment. Keep account mapping, custody, backups, and credential renewal operational after restart.
- Finalize license, distribution identity, dependency delivery, and push configuration before public release.
