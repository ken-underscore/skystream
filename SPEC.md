# Spec: Word search links

## Objective

Visitors watching the cloud see a word trending and want to know why. Clicking a word opens Bluesky's search results for it, `https://bsky.app/search?q=<word>`, so they can read the posts behind the count.

Acceptance criteria:

- Clicking any drawn word opens that word's Bluesky search in a new tab. The live cloud keeps running in the original tab.
- Keyboard users can reach each word with Tab and open it with Enter. Focus is visible.
- Screen readers announce each word as a link.
- Middle-click, Ctrl/Cmd-click, and the context menu's "copy link" work because each word is a real link.
- Hovering shows a pointer cursor and a visual highlight. The tooltip still shows the count.
- Links work with every transition (crossfade, smooth movement, fade, instant), in demo mode, and after resizing. In smooth movement, a word that moves keeps the link for its own text.

Out of scope: in-page post previews, search filters (language, date, author), analytics on clicks, and backend changes.

## Tech Stack

Frontend only: TypeScript 7, Vite 8, D3 7.9, d3-cloud 1.2.9, pnpm 11. No new dependencies.

## Commands

From `src/main/frontend`:

```sh
pnpm check   # TypeScript
pnpm build   # TypeScript + production build into dist/
pnpm dev     # Dev server; open with ?demo=1 to test without Java
```

From the project root, `./mvnw test` should stay green; no Java changes are expected.

## Project Structure

```
src/main/frontend/src/search.ts  → new: searchUrl(word), the Bluesky search link
src/main/frontend/src/cloud.ts   → wrap each word's <text> in an SVG <a>, in paint() and the move transition
src/main/frontend/src/style.css  → cursor, hover, and focus-visible styles for cloud links
src/main/frontend/index.html     → cloud <svg> role changed so its links are exposed to assistive technology
src/main/frontend/README.md      → document clicking a word; add search.ts to the Code list
```

## Code Style

Match the existing frontend: small modules with named exports, strict TypeScript, D3 chained joins, and terse comments only where intent isn't obvious.

```ts
const blueskySearch = 'https://bsky.app/search';

export function searchUrl(word: string): string {
  return `${blueskySearch}?${new URLSearchParams({ q: word })}`;
}
```

Rendering uses SVG `<a href target="_blank" rel="noopener noreferrer">` rather than a click handler with `window.open`. Native links provide keyboard, middle-click, and copy-link support without any extra code.

## Testing Strategy

The frontend has no unit-test framework, and this change does not add one.

- `pnpm check` and `pnpm build` pass.
- Manual browser check in demo mode (`?demo=1`), desktop and mobile widths:
  - Click, middle-click, and Tab + Enter open the correct search URL in a new tab.
  - Each transition mode: after several updates, links still point at their own word, and tooltips show the current count.
  - Reduced motion: links still work.
  - Words with apostrophes and numbers are encoded correctly (`don't` → `q=don%27t`).
  - A screen reader or the browser accessibility tree lists the words as links.
- `./mvnw test` still passes.

## Boundaries

- **Always:**
  - Run `pnpm check` and `pnpm build` before committing.
  - Work on a branch off `main` (`feat/word-search-links`).
  - Keep the code concise and idiomatic.
- **Ask first:**
  - Adding dependencies, including a frontend test framework.
  - Any backend or API change.
  - Changing the transition behavior beyond what links require.
- **Never:**
  - Claude attribution in commits or PRs.
  - Navigating the live page away from the cloud.
  - Building search URLs by string concatenation without encoding.

## Success Criteria

- Every drawn word is an `<a>` whose `href` equals `searchUrl(word.text)`, with `target="_blank"` and `rel="noopener noreferrer"`.
- Links are correct after updates in all four transition modes and after a resize.
- Words are reachable by keyboard, show a visible focus indicator, and appear as links in the accessibility tree.
- `pnpm check`, `pnpm build`, and `./mvnw test` pass. The frontend README describes the feature.

## Decisions

1. **New tab.** Links open in a new tab, so the live stream keeps running.
2. **One tab stop per word.** Each drawn word is its own tab stop. No roving tabindex.
3. **`role="group"`.** The cloud `<svg>` changes from `role="img"`, which hides its children from assistive technology, to `role="group"`, keeping the snapshot summary as its label.
4. **Bare word.** The query is the word itself (`q=hello`), not an exact-phrase quote.

## Open Questions

None.
