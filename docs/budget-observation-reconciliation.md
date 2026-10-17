# Correcting a retained finite-budget diagnostic

A retained blocked-phase observation can outlive the bug that misclassified a
finite navigation budget. Automatic observation deliberately does not treat a new
invocation or unfinished navigation as proof that an actual failure recovered.
Do not clear such a diagnostic by acknowledgment or by invoking paid work merely
to obtain a successful phase receipt.

After inspecting the producer's retained jobs and canonical event receipts, an
operator may explicitly correct a demonstrated misclassification. In the observed
Eko case, the old failed event and its successor were applied; the later job held
completed navigation with no halted decision, failed canonical event or unresolved
provider dispatch. Its current receipt explicitly said navigation_budget_exhausted.
The source incident still represented that normal pending work as a failure.

Stop only the selected observer unit, retaining its state, and use its existing
owner-bound configuration with:

```
python3 scripts/observer.py --config <existing-private-config> \
  --reconcile-budget-phase capture --worker-sha256 <inspected-receipt-sha256> \
  --reason <bounded-source-grounded-diagnostic-correction>
```

The receiving operator supplies paths and hashes; examples grant no access.
The observer's exclusive lock and existing configuration binding apply. The
operation refuses changed receipts, real current phase failures, provider alerts,
other unhealthy observations and inconsistent replay. It retains the previous
incident, blocked phases, reason and receipt hash in its audit, changes no worker
receipt, source, canonical memory or pending job, and invokes no processing.
Restart the same observer after the correction. Configuration rollback retains
this audit and live exporter state.

Resolution explicitly states that the diagnostic was corrected and producer work
remains pending. It does not claim source processing completed. Verify the same
service incident at a higher revision and inspect resolution delivery separately.
This is an exceptional explicit maintenance operation, not automatic dismissal of
budget-limited work following a genuine unresolved failure.
