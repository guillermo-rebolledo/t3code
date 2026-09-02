# GitHub Copilot

T3 Code uses the GitHub Copilot CLI installed on the connected environment. It does not install the
CLI or store its credentials. With a remote environment, install and authenticate Copilot on the
server machine, not on the browser or phone controlling it.

Install the Copilot CLI and sign in:

```sh
copilot auth login
```

Then open **Settings** → **Providers**, add **GitHub Copilot**, and optionally set **Binary path**.
Use an explicit path if a packaged desktop launch cannot see the same `PATH` as your shell. T3 Code
also checks common Homebrew and user-level install directories and respects provider-specific
environment variables and Windows executable extensions.

Provider status distinguishes a missing executable, invalid version output, CLI failure, SDK
timeout, signed-out account, organization-policy or entitlement rejection, and a ready account. A
successful check lists only the models returned for the current account. Temporary SDK failures keep
the last successful list; T3 Code never adds a made-up Copilot model.

Multiple Copilot provider instances may use different executable paths or environments. Unless that
configuration changes where Copilot stores its state, the instances share the operating-system
Copilot credential store.

This release establishes Copilot discovery and configuration. Starting Copilot-backed threads is
not available in this slice yet.
