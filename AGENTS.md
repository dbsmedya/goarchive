# GoArchive agent instructions

Keep machine-specific tooling instructions, including RTK configuration, in the user's
global agent instructions.

Read [CLAUDE.md](CLAUDE.md) for the project's development workflow, code and documentation
owners, and test procedures.

The primary development workflow is **dev-contract**. Read
the operator's globally installed `dev-contract` skill (typically
`~/.claude/skills/dev-contract/SKILL.md`) and its relevant references before designing,
planning, reviewing, dispatching, implementing or preparing a PR. An optional
`.agents/skills/dev-contract` discovery link is machine-specific and gitignored; it is not
provided by this repository.

Follow the operator's rulings and approved spec first, then the plan. dev-contract takes
precedence over the older development-workflow, software-architect and Superpowers workflow
instructions. Other skills may support the work within this contract.

Adopting the workflow does not approve development of a pending plan. Converting an existing
plan preserves its approved spec and requires the contract's plan review before dispatch.
