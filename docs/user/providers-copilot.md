# GitHub Copilot

T3 Code uses the GitHub Copilot CLI installed on the connected environment. It does not install the
CLI or store its credentials. With a remote environment, install and authenticate Copilot on the
server machine, not on the browser or phone controlling it.

## Install and connect

Install the stable [Copilot CLI](https://docs.github.com/copilot/how-tos/set-up/install-copilot-cli)
on the environment machine, then sign in from a standalone terminal there:

```sh
copilot login
```

Then open **Settings** → **Providers**, add **GitHub Copilot**, and optionally set **Binary path**.
Use an explicit path if a packaged desktop launch cannot see the same `PATH` as your shell. T3 Code
also checks common Homebrew and user-level install directories and respects provider-specific
environment variables and Windows executable extensions.

Copilot CLI 1.0.79 or newer is required. Run `copilot version` to see the installed version and
`copilot update stable` to update it. T3 Code reports an older CLI as incompatible before it opens a
session, with the minimum version in the error.

Provider status distinguishes a missing executable, invalid version output, CLI failure, SDK
timeout, signed-out account, organization-policy or entitlement rejection, and a ready account. A
successful check lists only the models returned for the current account. Temporary SDK failures keep
the last successful list; T3 Code never adds a made-up Copilot model.

Multiple Copilot provider instances may use different executable paths or environments. Unless that
configuration changes where Copilot stores its state, the instances share the operating-system
Copilot credential store.

For a work or organization-managed account, complete `copilot login` with that account and ask its
administrator to enable Copilot CLI if provider status reports an organization-policy or entitlement
failure. T3 Code cannot bypass account policy. It lists only the models that the signed-in account
can use, and it reports unavailable models or unsupported model options instead of substituting a
different choice.

The environment always owns the Copilot process, executable, sessions, and credentials. This stays
the same when a browser, packaged desktop app, or mobile app connects locally, directly over the
network, through a relay, or through a T3 Connect tunnel. The controlling client does not need a
Copilot installation or credentials.

An enabled, ready Copilot instance can also generate thread titles, branch names, commit messages,
and pull-request copy. These background requests use the model and supported options selected for
that workflow. They run in a temporary session with no tools available; every permission request is
rejected, and the session is disconnected when the request succeeds, fails, or times out.

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

Copilot's own slash commands appear in the composer for the project or worktree you are working
in, alongside the skills Copilot finds there. Picking one, or typing it yourself, runs it as a
Copilot command rather than sending it as text, and its output lands in the thread like any other
reply. Text that merely starts with a slash Copilot does not recognize is sent as an ordinary
prompt, so a message can still begin with a path or a fraction. A few commands are left out because
T3 Code already owns them: use the model picker and the thread's mode instead of Copilot's `/model`,
`/plan`, and `/default`.

Picking a skill inserts it as a `$name` chip, the same as every other provider. T3 Code hands that
to Copilot in the form Copilot understands, so a message that is just the skill runs it directly,
and a skill named in the middle of a sentence reaches Copilot as part of the request. A `$` that
names nothing Copilot offers is left exactly as you typed it, so `$HOME` in a sentence stays
`$HOME`.

Commands and skills belong to the directory they were found in. Another project sees its own, and a
project that Copilot cannot answer for keeps working as an ordinary conversation - you lose the
command list for that directory, not the ability to talk to Copilot.

Changing the model or a model option, such as reasoning effort or context window, applies to the
running Copilot session and takes effect on your next message; the conversation is not restarted. A
model your account cannot use, or an option a model does not offer, is reported instead of quietly
falling back to a different one.

## Troubleshooting

- **CLI not found:** run `copilot version` on the environment machine. If that works only in an
  interactive shell, set the provider's **Binary path** to the absolute executable path.
- **CLI too old:** run `copilot update stable`, restart the environment, and check provider status
  again.
- **Signed out:** run `copilot login` in a standalone terminal on the environment machine. Packaged
  desktop launches use the same operating-system credential store; do not paste credentials into T3
  Code.
- **Account or model rejected:** confirm the work-account policy and model entitlement with the
  organization administrator. The model list in T3 Code is the account's live Copilot inventory.
- **A turn will not finish:** stop it from the thread. If the Copilot process exited, the thread shows
  the failure and can continue after the provider is healthy again.
