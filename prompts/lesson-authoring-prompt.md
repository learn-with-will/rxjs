You are a senior RxJS educator writing a single self-contained lesson for a
frontend-only "RxJS Learning Portal". Produce content that is accurate for
**RxJS 7+** (pipeable operators, TypeScript, `import { ... } from 'rxjs'` and
`from 'rxjs/operators'` where relevant). Do not invent APIs.

>>> TRUTH ABOVE ALL: Every statement, API name, signature, default, marble
diagram, and code example MUST be factually correct and verifiable against the
official RxJS documentation (rxjs.dev) and reactivex.io. Never invent, guess, or
approximate operator behaviour. If you are not certain something is true, leave it
out rather than state it — an accurate lesson with less content beats a fuller one
with a single falsehood. Be especially careful with: which operators complete vs.
stay open, unicast vs. multicast, cold vs. hot semantics, error/retry behaviour,
and deprecation status (e.g. `retryWhen` is deprecated; `toPromise()` is removed
in RxJS 8 — use `firstValueFrom`/`lastValueFrom`).

=== INPUTS (I fill these in) ===
Topic:         "Marble Testing with the TestScheduler"
Level:         advanced
Lesson number: 18

=== WHAT TO OUTPUT ===
Return EXACTLY two fenced blocks and nothing else (no preamble, no closing chatter):

1. A ```markdown block: the complete lesson file, including YAML front-matter.
2. A ```json block: the single object to add to course-manifest.json.

=== FILE 1: the lesson Markdown ===
It will be saved to:  public/content/<level>/<slug>.md
(<slug> is a kebab-case slug you derive from the Topic, e.g. "creating-observables".)

Start the file with YAML front-matter delimited by lines of exactly three dashes:

---
id: lesson-NN            # if I gave a number use lesson-<that number, zero-padded to 2>, else literally: lesson-NN
slug: <kebab-slug>
title: "<Human Title>"
level: <the level I gave>
order: NN                # same number as id, or literally: NN if I gave none
duration: <realistic whole-minute reading time, ~15–60 based on depth>
tags:
  - <tag>
  - <tag>               # 3–6 lowercase kebab-case tags
---

Then the lesson body, using these TOP-LEVEL `#` section headings, IN THIS ORDER
(omit a section only if it truly does not apply):

# Learning Objectives         (4–6 bullet "you will be able to…" items)
# Why This Concept Exists      (motivation: the problem it solves, when to reach for it)
# Concept Explanation          (the core teaching; use ### subsections as needed)
# Key Terminology              (bulleted term → definition list)
# Syntax Overview              (one focused code block showing the essential shapes)
# Real World Analogy           (one vivid, accurate analogy)
# Examples                     (use ## subheadings, see rules below)
# Common Mistakes              (bulleted; each names the mistake and the fix)
# Best Practices               (bulleted, actionable)
# Summary                      (bulleted recap of every key point)
# Flash Cards                  (SPECIAL FORMAT — see below)
# Exercises                    (### Easy, ### Medium, ### Challenging)
# Further Reading              (bulleted links to official RxJS docs / MDN)

Examples section rules:
- Provide three, each as a `##` subheading:
  `## Example 1 — Basic: <name>`, `## Example 2 — Real-world: <name>`,
  `## Example 3 — Best practice: <name>`.
- After each example's code, add a short "**Why this is good:** …" paragraph.
- Where it clarifies timing, include an ASCII marble diagram in a comment or code
  fence, but keep it accurate.

=== FLASH CARDS — EXACT FORMAT (this is parsed by the app) ===
Under the `# Flash Cards` heading, write 5–7 cards. Each card is TWO ADJACENT
lines with NO blank line between them, and a blank line BETWEEN cards:

Q: <question>
A: <answer>

Q: <next question>
A: <next answer>

Do NOT use headings, bullets, or bold labels inside Flash Cards — just the
`Q:` / `A:` line pairs exactly as shown. Keep each Q and each A on a single line.

=== CODE RULES ===
- Every code fence MUST declare a language so highlighting works. Use only:
  ts, js, bash, json, css, html.
- Use `ts` for RxJS examples (typed Observables, operators, Subjects).
- Keep examples runnable and idiomatic for RxJS 7+: `pipe()` with pipeable
  operators, `throwError(() => new Error(...))` factory form, `firstValueFrom` /
  `lastValueFrom` instead of `toPromise()`, `combineLatest([...])` array form. No
  deprecated patterns (`retryWhen`, `.toPromise()`, chained prototype operators).
- Prefer TypeScript. Show imports where they aid understanding.

=== HEADING / MARKDOWN RULES ===
- Only `#`, `##`, `###` headings appear in the table of contents — structure the
  lesson with those; never skip from `#` to `###`.
- The Flash Cards heading text must be exactly `Flash Cards`.
- Standard GitHub-flavored Markdown is supported (tables, blockquotes, lists,
  inline `code`, links).

=== FILE 2: the course-manifest.json entry ===
Output a single JSON object whose fields MATCH the front-matter exactly (id, slug,
title, level, order, duration, tags) plus:
- "file": "<level>/<slug>.md"
- "summary": a one–two sentence description for the lesson card.

{
  "id": "lesson-NN",
  "slug": "<kebab-slug>",
  "title": "<Human Title>",
  "level": "<level>",
  "order": NN,
  "duration": <minutes>,
  "file": "<level>/<slug>.md",
  "summary": "<one–two sentence summary>",
  "tags": ["<tag>", "<tag>"]
}

=== QUALITY BAR ===
- Technically precise and current for RxJS 7+; correct terminology and marble timing.
- Concrete and example-driven; explain the "why", not just the "how".
- Self-contained: a reader at the given Level can follow it end to end.
- Match the requested Level: beginner = gentle pace, more scaffolding; advanced =
  assumes fundamentals, goes deeper into trade-offs, concurrency, and edge cases.
