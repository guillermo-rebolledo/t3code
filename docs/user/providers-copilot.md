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

Copilot-backed threads support ordinary prompts, uploaded images and files, streaming assistant and
reasoning output, tool progress, usage updates, and stopping. Stopping a running turn takes effect
immediately and leaves the thread ready for the next message. If Copilot reports an error or its
process ends unexpectedly, the turn stops with that reason instead of appearing to run forever.
Uploaded attachments are resolved from T3 Code's attachment store; arbitrary server paths are never
accepted as attachment identifiers.

Copilot threads keep working across restarts. T3 Code remembers which Copilot session belongs to a
thread and reattaches to it, so a thread you come back to continues where it left off. A thread that
recorded a session belonging to a different Copilot provider instance, or one Copilot no longer has,
reports why it cannot continue instead of silently starting a blank conversation.

Restoring an earlier point in a Copilot thread rewinds the conversation Copilot holds along with the
thread. Files come back through T3 Code's own checkpoints, so a restore never depends on Copilot's
file history. If Copilot is busy or refuses the rewind, the thread stays exactly as it was and tells
you why.

Changing the model or a model option, such as reasoning effort or context window, applies to the
running Copilot session and takes effect on your next message; the conversation is not restarted. A
model your account cannot use, or an option a model does not offer, is reported instead of quietly
falling back to a different one.
