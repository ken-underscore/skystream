import { format, interpolateRound, select } from 'd3';
import type { WordCount } from './snapshot';
import { searchUrl } from './search';

const uses = format(',');
const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)');
/** As long as the CSS transitions that move rows and fade them out. */
const moveMs = 650;
const leaveMs = 400;

/**
 * Draws words as a ranked list of bars, keyed by word. When a new snapshot arrives, rows slide to their new ranks, bars
 * and counts change in place, and words entering or leaving the list fade.
 */
export function createChart(list: HTMLOListElement) {
  return function update(words: WordCount[]) {
    const most = words[0]?.count ?? 1;
    list.style.setProperty('--rows', String(words.length));
    select(list).selectAll<HTMLLIElement, WordCount>('li:not(.leaving)').data(words, word => word.text).join(
      enter => enter.append('li').call(row => {
        const link = row.append('a').attr('href', word => searchUrl(word.text))
          .attr('target', '_blank').attr('rel', 'noopener noreferrer');
        link.append('span').attr('class', 'rank');
        link.append('span').attr('class', 'word').text(word => word.text);
        link.append('span').attr('class', 'bar');
        link.append('span').attr('class', 'count');
      }),
      update => update,
      // Leaving rows keep their place while CSS fades them, then go.
      exit => exit.classed('leaving', true).transition().duration(reducedMotion.matches ? 0 : leaveMs).remove(),
    )
      // Document order follows rank, for keyboards and screen readers.
      .order()
      .style('--rank', (_, rank) => rank)
      .style('--share', word => word.count / most)
      .each(function (word, rank) {
        const row = select(this);
        row.select('a').attr('aria-label', `${word.text}: ${uses(word.count)} uses, rank ${rank + 1}`);
        row.select('.rank').text(String(rank + 1).padStart(2, '0'));
        row.select<HTMLElement>('.count').call(count => countTo(count.node()!, word.count));
      });
  };
}

/** Counts up or down to the new value over the time a row takes to move, or shows it at once with reduced motion. */
function countTo(element: HTMLElement, value: number) {
  const from = Number(element.dataset.value ?? value);
  element.dataset.value = String(value);
  if (from === value || reducedMotion.matches) {
    select(element).interrupt().text(uses(value));
    return;
  }
  const between = interpolateRound(from, value);
  select(element).transition().duration(moveMs).tween('text', () => t => { element.textContent = uses(between(t)); });
}
