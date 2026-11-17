# cindy-omp worktree context

- Branch `omp-engine` on this worktree (`D:\Development\cindy-omp`); fork `origin` = `https://github.com/olvchk/cindy`; `upstream` = `makecindy/cindy`.
- Upstream isolation: `upstream` push URL is `no-push` (set via `git remote set-url --push upstream no-push`); any `git push upstream` must fail. No commit, push, branch, PR, or issue against `makecindy/*`, ever. All pushes go to `origin` only.
- `D:\Development\cindy` (main checkout) stays on `main`, receives no commits or pushes; it is a reference copy.
- `agents/pi/**` is read-only for this work: reuse by import, never modify. The pi-harness rules doc governs that tree.
- omp owns credentials, memory, skills, and providers in `~/.omp`; Cindy injects nothing into omp.
- Commit convention for this work: `feat(omp): ...`. Every commit must carry a DCO `Signed-off-by` line consistent with the author/committer.
- `cindy-omp-harness-plan.md` is a local-only plan file (gitignored); the running ledger is the session artifact `local://cindy-omp-status.md`.
