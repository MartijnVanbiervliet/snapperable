# Releasing snapperable

This document describes how to publish a new version of **snapperable** to PyPI.  
Only the repository owner may approve a production PyPI publish.

---

## Prerequisites

Before you begin:

- You must be a maintainer of the `MartijnVanbiervliet/snapperable` GitHub repository.
- The `pypi` GitHub Environment must have a **required reviewer** set to the repository owner.  
  See [Set up GitHub Environments](#set-up-github-environments) below.
- PyPI **Trusted Publishing** (OIDC) must be configured on both PyPI and TestPyPI for this
  repository and workflow (no API tokens are needed). See [Configure Trusted Publishing](#configure-trusted-publishing).

---

## Release workflow overview

```
workflow_dispatch (manual trigger)
         │
         ▼
   validate         ← checks: main branch only, no forks,
         │            version in pyproject.toml matches input,
         │            tag does not already exist
         ▼
    build            ← lint + test + build wheel & sdist
         │
         ▼
 publish-testpypi   ← publishes to TestPyPI (testpypi environment)
         │
         ▼
  publish-pypi      ← **requires owner approval** (pypi environment)
         │
         ▼
 github-release     ← creates a GitHub Release with artifacts attached
```

The workflow is defined in `.github/workflows/release.yml`.

---

## Step-by-step release process

### 1. Bump the version

Update `version` in `pyproject.toml`:

```toml
[project]
version = "X.Y.Z"
```

Commit and push the change to `main`:

```bash
git commit -am "chore: bump version to X.Y.Z"
git push origin main
```

> **Note:** The release workflow validates that the version you enter when triggering it
> matches the value in `pyproject.toml`. The workflow will fail immediately if they differ.

### 2. Trigger the release workflow

1. Go to **Actions → Release** in the GitHub UI.
2. Click **Run workflow**.
3. Enter the version string (e.g. `1.2.3`) — must match `pyproject.toml` exactly.
4. Click **Run workflow**.

The workflow will:

- Verify the version and confirm the git tag does not already exist.
- Lint, test, and build the wheel and sdist **once**.
- Publish the artifacts to **TestPyPI** automatically.

### 3. Approve the PyPI publish

After TestPyPI succeeds, the `publish-pypi` job waits for **required reviewer** approval.

1. A reviewer notification is sent to the repository owner.
2. The owner reviews the TestPyPI release at:  
   `https://test.pypi.org/p/snapperable/`
3. If everything looks correct, approve the deployment from the **Actions** run page.

The package is then published to PyPI and a GitHub Release is created automatically
with the wheel and sdist attached.

---

## Set up GitHub Environments

Two environments must be created in **Settings → Environments**:

| Environment | Protection rules |
|-------------|-----------------|
| `testpypi`  | None required (auto-approves) |
| `pypi`      | **Required reviewer**: repository owner |

Steps:

1. Navigate to **Settings → Environments → New environment**.
2. Name it `testpypi` and save (no protection rules needed).
3. Repeat for `pypi` and add the repository owner as a required reviewer.

---

## Configure Trusted Publishing

Trusted Publishing lets GitHub Actions publish to PyPI without storing API tokens.

### TestPyPI

1. Log in to <https://test.pypi.org>.
2. Go to your account → **Publishing** → **Add a new pending publisher**.
3. Fill in:
   - **PyPI project name**: `snapperable`
   - **Owner**: `MartijnVanbiervliet`
   - **Repository name**: `snapperable`
   - **Workflow filename**: `release.yml`
   - **Environment name**: `testpypi`

### PyPI

1. Log in to <https://pypi.org>.
2. Go to your account → **Publishing** → **Add a new pending publisher**.
3. Fill in the same details but with **Environment name**: `pypi`.

> After the first successful publish, the "pending publisher" becomes a full trusted publisher
> and no further configuration is required.

---

## Rollback steps

### If TestPyPI publish fails

No user-visible impact. Fix the issue, push to `main`, and re-trigger the workflow.

### If PyPI publish fails mid-flight

1. Check the workflow run logs for the exact error.
2. If no files were uploaded, fix the issue and re-trigger the workflow.
3. If a partial upload occurred, log in to PyPI, navigate to the release, and delete the
   broken files before re-triggering.

### If a bad release reaches PyPI

PyPI does **not** allow re-uploading the same version. To issue a fix:

1. **Yank** the bad release on PyPI (Settings → Manage → Yank release).  
   Yanking prevents new installs of that version but does not break pinned installs.
2. Bump the version (patch increment, e.g. `1.2.3` → `1.2.4`), fix the issue, and
   follow the normal release process.

### Deleting a GitHub Release / tag

If the GitHub Release must be removed (e.g. to re-run the workflow after a failed PyPI
publish that created the release):

```bash
gh release delete "vX.Y.Z" --yes
git push --delete origin "vX.Y.Z"
```

Then re-trigger the release workflow.

---

## FAQ

**Q: Can I release from a branch other than `main`?**  
A: No. The workflow has a guard that exits immediately if `github.ref` is not `refs/heads/main`.

**Q: Can a fork trigger a release?**  
A: No. The workflow checks `github.repository == 'MartijnVanbiervliet/snapperable'` and exits
otherwise.

**Q: What if I accidentally trigger the workflow with the wrong version?**  
A: The `validate` job will fail because the input version must match `pyproject.toml`.
Cancel the run and fix `pyproject.toml` if needed.

**Q: Do I need to create the git tag manually?**  
A: No. The `github-release` job calls `gh release create vX.Y.Z`, which creates the tag
automatically.
