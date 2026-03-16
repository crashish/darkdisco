# Git Sync Proposal: Preventing Mayor/Refinery/Origin Drift

## Problem

During the March 13-16 2026 session, significant time was wasted on:
- Cherry-picking polecat commits that conflicted with mayor's direct changes
- Discovering refinery had 20+ unsynced commits
- Multiple rounds of conflict resolution on the same files
- Features merged on refinery but never deployed (missing migrations, missing code)

## Root Cause

The git flow is broken:

```
Current (broken):
  Polecat → branch off refinery HEAD (detached, diverged from main)
  Polecat → push branch to origin
  Refinery → merge polecat branch into detached HEAD (NOT main)
  Mayor → cherry-pick from polecats/refinery (conflicts with own changes)
  Mayor → never pushes to origin
  → Drift accumulates on all three copies
```

## Proposed Flow

```
Fixed:
  origin/main = single source of truth (equals mayor/main at all times)

  Polecat → branch off origin/main
  Polecat → push branch to origin
  Refinery → checkout main, merge polecat branch, push to origin/main
  Mayor → pull origin/main (always clean, fast-forward)
  Mayor → commit directly, push to origin/main immediately
```

## Implementation

### 1. Mayor: Auto-push after commit

Every commit in mayor/rig should be followed by `git push origin main`.
Options:
- Git post-commit hook in `.git/hooks/post-commit`
- Convention enforced by the session end checklist
- GT hook that auto-pushes after mayor commits

### 2. Refinery: Merge to main, not detached HEAD

The refinery's merge process must:
- `git checkout main`
- `git merge polecat/branch`
- `git push origin main`
- NOT merge into a detached HEAD

This is a Gas Town refinery configuration change.

### 3. Polecats: Branch from origin/main

Polecat worktrees should be created from `origin/main` (freshly fetched),
not from the refinery's potentially-stale HEAD.

### 4. Conflict prevention

- Mayor should NOT cherry-pick from polecats. Instead:
  - Wait for refinery to merge to main
  - Pull origin/main (fast-forward)
- If mayor needs to make direct fixes, commit and push immediately
- Polecats branching after the push will include the fix

## Exceptions

- Emergency fixes can be committed directly on mayor and pushed
- The refinery should `git pull origin main` before each merge to pick up
  any direct mayor commits
