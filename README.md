# RxJS Learning Portal

A **frontend-only** RxJS learning platform: read Markdown lessons, take quizzes,
and track your progress — all in the browser, no backend required. The course
takes you from your first Observable to schedulers, multicasting, marble testing,
and production streaming patterns.

The portal shell is built with React 19, Vite, TypeScript, React Router,
Tailwind CSS v4, marked, and PrismJS. The **content** it renders is a complete
RxJS 7+ curriculum (all code targets RxJS 7 / `rxjs@7`, the pipeable-operator era).

## Features

- **Roadmap** of lessons grouped by level (beginner / intermediate / advanced).
- **Lesson reader** with Markdown rendering, Prism syntax highlighting, a table of
  contents, previous/next navigation, mark-complete, and bookmarking.
- **Quizzes** supporting five question types: single-choice, multiple-choice,
  fill-blank, ordering, and match-pair — with grading, review mode, and explanations.
- **Dashboard** with completion stats, per-level progress, and quiz history.
- **Search** and **bookmarks** pages.
- **Dark mode** with persistence, and a responsive layout.
- All progress persists to `localStorage`.

## Run it

```bash
npm install
npm run dev        # dev server (Vite) at http://localhost:5173
npm run build      # production build into dist/
npm run preview    # serve the production build locally
```

## Project structure

```text
src/
├── core/
│   ├── models/        # Lesson, Quiz, Progress types + constants
│   ├── services/      # lesson/quiz loading, search, grading, asset URLs
│   ├── context/       # Course, Progress, Theme providers (React state)
│   └── hooks/         # useDocumentTitle, prism setup
├── shared/components/ # Navbar, Sidebar, Footer, LessonCard, TableOfContents,
│                      # LessonNavigation, LessonContent, QuizQuestion,
│                      # QuizProgress, QuizResult, RxjsLogo, ScrollManager
├── features/          # home, roadmap, lesson, quiz, dashboard, search, bookmarks
├── App.tsx            # layout + lazy-loaded routes
└── main.tsx           # providers + bootstrap

public/
├── content/           # lesson Markdown + course-manifest.json
└── quizzes/           # one <lessonId>.json per quiz
```

## Adding content — no code changes required

### Add a lesson

1. Create a Markdown file under `public/content/<level>/`, e.g.
   `public/content/beginner/what-is-rxjs.md`. Start it with YAML front-matter:

   ```md
   ---
   id: lesson-01
   slug: what-is-rxjs
   title: What Is RxJS? Observables and Reactive Thinking
   level: beginner
   order: 1
   duration: 22
   tags: [rxjs, observable, reactive, streams]
   ---

   # Learning Objectives
   ...
   ```

2. Add a matching entry to `public/content/course-manifest.json`:

   ```json
   {
     "id": "lesson-01",
     "slug": "what-is-rxjs",
     "title": "What Is RxJS? Observables and Reactive Thinking",
     "level": "beginner",
     "order": 1,
     "duration": 22,
     "file": "beginner/what-is-rxjs.md",
     "summary": "What an Observable is, how it differs from a Promise, and why RxJS models everything as a stream of values over time.",
     "tags": ["rxjs", "observable", "reactive", "streams"]
   }
   ```

The lesson now appears in the roadmap, sidebar, search, and home — automatically.

> **Flash cards:** a heading titled `Flash Cards` followed by a list (or paragraphs)
> of `Q: … / A: …` pairs is auto-transformed into interactive flip cards.

### Add a quiz

Create `public/quizzes/<lessonId>.json` (e.g. `public/quizzes/lesson-01.json`):

```json
{
  "id": "quiz-lesson-01",
  "lessonId": "lesson-01",
  "title": "What Is RxJS?",
  "passingScore": 60,
  "questions": [
    { "id": "q1", "type": "single-choice", "prompt": "…",
      "options": [{ "id": "a", "text": "…" }], "answer": "a",
      "explanation": "…" }
  ]
}
```

Supported question `type` values: `single-choice`, `multiple-choice`,
`fill-blank`, `ordering`, `match-pair`.

## Tech stack

React 19 · Vite · TypeScript · React Router · Tailwind CSS v4 · marked · PrismJS ·
localStorage. Course content targets **RxJS 7+**.

## Deploying to GitHub Pages

This repo ships a GitHub Actions workflow ([`.github/workflows/deploy.yml`](.github/workflows/deploy.yml))
that builds the site and publishes it to GitHub Pages on every push to `main`.

One-time setup:

1. In the repo on GitHub, go to **Settings → Pages** and set **Source** to
   **GitHub Actions**.
2. Push to `main` (or run the workflow manually from the **Actions** tab).

The site is then served at `https://<user>.github.io/<repo>/`. How it works:

- The workflow builds with `BASE_PATH=/<repo>/`, which sets Vite's `base` so all
  assets and content resolve under the sub-path. `<BrowserRouter basename>` is
  aligned to the same value, so client-side routes work too.
- A `postbuild` step copies `dist/index.html` to `dist/404.html`, giving the SPA a
  fallback so deep links and refreshes (e.g. `/<repo>/lesson/…`) load correctly.

> If you deploy to a **user/organization site** (`<user>.github.io`) instead of a
> project site, the base path is `/` — set `BASE_PATH: /` in the workflow.

For other static hosts (Netlify, Vercel), the `dist/` output works as-is; just
configure an SPA fallback to `index.html`.
