# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root.
- **`CONTEXT-MAP.md`** at the repo root if it exists.
- **`docs/adr/`** — read ADRs that touch the area you're about to work in.

If any of these files don't exist, **proceed silently**. Domain documentation is created lazily when terms or decisions are resolved.

## File structure

This is a single-context repository:

/
├── CONTEXT.md
├── docs/adr/
└── src/

## Use the glossary's vocabulary

When output names a domain concept, use the term defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

If a needed concept isn't in the glossary, reconsider the terminology or note the gap for domain modeling.

## Flag ADR conflicts

If output contradicts an existing ADR, surface it explicitly rather than silently overriding.
