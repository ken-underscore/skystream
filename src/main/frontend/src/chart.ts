import { format, interpolateRound, select } from 'd3';
import type { WordCount } from './snapshot';
import { searchUrl } from './search';

const uses = format(',');
const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)');
const moveMs = 650;
const fadeMs = 400;
const duration = (ms: number) => reducedMotion.matches ? 0 : ms;

// Not yet in TypeScript's DOM types. Unlike insertBefore, it moves a node without removing it, so focus stays put.
type Movable = Element & { moveBefore(node: Node, child: Node | null): void };
const canMove = 'moveBefore' in Element.prototype;

/**
 * Draws words as a ranked list of bars, keyed by word. When a new snapshot arrives, rows slide to their new ranks, bars
 * and counts change in place, and words entering or leaving the list fade. D3 runs these animations rather than CSS,
 * since moving an element to its new place in the list would end its CSS transitions.
 */
export function createChart(list: HTMLOListElement) {
  return function update(words: WordCount[]) {
    const most = words[0]?.count ?? 1;
    list.style.setProperty('--rows', String(words.length));
    const rows = select(list).selectAll<HTMLLIElement, WordCount>('li:not(.leaving)').data(words, word => word.text);
    // Leaving rows keep their place while they fade, out of reach of keyboards and screen readers.
    rows.exit().classed('leaving', true).attr('inert', '')
      .transition('fade').duration(duration(fadeMs)).style('opacity', 0).remove();
    const entering = rows.enter().append('li').style('opacity', 0).style('--rank', (_, rank) => rank).call(row => {
      const link = row.append('a').attr('href', word => searchUrl(word.text))
        .attr('target', '_blank').attr('rel', 'noopener noreferrer');
      link.append('span').attr('class', 'rank');
      link.append('span').attr('class', 'word').text(word => word.text);
      link.append('span').attr('class', 'bar');
      link.append('span').attr('class', 'count');
    });
    entering.transition('fade').duration(duration(fadeMs)).style('opacity', 1);
    const all = entering.merge(rows);
    rankOrder(list, all.nodes());
    all.transition('move').duration(duration(moveMs))
      .style('--rank', (_, rank) => rank)
      .style('--share', word => word.count / most);
    all.each(function (word, rank) {
      const row = select(this);
      row.select('a').attr('aria-label', `${word.text}: ${uses(word.count)} uses, rank ${rank + 1}`);
      row.select('.rank').text(String(rank + 1).padStart(2, '0'));
      countTo(row.select<HTMLElement>('.count').node()!, word.count);
    });
  };
}

/** Puts rows in document order by rank, for keyboards and screen readers, keeping focus on the word that had it. */
function rankOrder(list: HTMLOListElement, rows: HTMLLIElement[]) {
  const focused = document.activeElement;
  for (let i = rows.length - 2; i >= 0; i--) {
    const row = rows[i]!;
    const next = rows[i + 1]!;
    if (row.compareDocumentPosition(next) & Node.DOCUMENT_POSITION_PRECEDING) {
      if (canMove) (list as unknown as Movable).moveBefore(row, next);
      else list.insertBefore(row, next);
    }
  }
  if (focused instanceof HTMLElement && focused !== document.activeElement && list.contains(focused)) {
    focused.focus({ preventScroll: true });
  }
}

/** Counts from the number shown to the new one as the row moves, or shows it at once with reduced motion. */
function countTo(element: HTMLElement, value: number) {
  const shown = Number(element.dataset.value ?? value);
  if (shown === value || reducedMotion.matches) {
    element.dataset.value = String(value);
    select(element).interrupt().text(uses(value));
    return;
  }
  const between = interpolateRound(shown, value);
  select(element).transition().duration(moveMs).textTween(() => t => {
    const count = between(t);
    element.dataset.value = String(count);
    return uses(count);
  });
}
