import { easeCubicInOut, scaleOrdinal, scaleSqrt, select } from 'd3';
import type { EnterElement, Selection } from 'd3';
import cloud from 'd3-cloud';
import type { WordCount } from './snapshot';
import { searchUrl } from './search';

interface PositionedWord extends WordCount { size: number; x?: number | undefined; y?: number | undefined }
export type CloudTransition = 'crossfade' | 'move' | 'fade' | 'none';

const position = (word: PositionedWord) => `translate(${word.x ?? 0},${word.y ?? 0})`;
const tooltip = (word: PositionedWord) => `${word.text}: ${word.count}`;

export function createCloud(container: HTMLElement, onDraw: (placed: number, selected: number) => void) {
  const svg = select(container).select<SVGSVGElement>('svg');
  const group = svg.append('g');
  let layer = group.append('g').attr('class', 'cloud-layer');
  const color = scaleOrdinal<string, string>(['#79c8ff', '#a5e3d5', '#c4b5fd', '#f5c985', '#f7a9ba']);
  let latest: WordCount[] = [];
  let layout: ReturnType<typeof cloud<PositionedWord>> | undefined;
  let generation = 0;
  let transitionMode: CloudTransition = 'crossfade';
  let hasDrawn = false;
  const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)');

  // Each word is a link wrapping its text. Words are keyed by text, so href, color, and text never change.
  function appendWords(enter: Selection<EnterElement, PositionedWord, SVGGElement, unknown>) {
    const links = enter.append<SVGAElement>('a').attr('href', word => searchUrl(word.text))
      .attr('target', '_blank').attr('rel', 'noopener noreferrer');
    links.append('text').attr('text-anchor', 'middle').attr('font-family', 'sans-serif')
      .attr('fill', word => color(word.text)).text(word => word.text).append('title');
    return links;
  }

  function paint(target: typeof layer, words: PositionedWord[]) {
    const links = target.selectAll<SVGAElement, PositionedWord>('a').data(words, word => word.text)
      .join(appendWords).attr('opacity', 1);
    links.select('title').text(tooltip);
    links.select('text').attr('font-size', word => word.size).attr('transform', position);
  }

  function render(words: PositionedWord[]) {
    // Cancel unfinished animations and retire their old layers before drawing the newest layout.
    group.selectAll('*').interrupt('cloud');
    group.selectAll<SVGGElement, unknown>('.cloud-layer').filter(function () { return this !== layer.node(); }).remove();
    layer.attr('opacity', 1);
    const mode = reducedMotion.matches || !hasDrawn ? 'none' : transitionMode;
    hasDrawn = true;
    const duration = 650;
    if (mode === 'none') {
      paint(layer, words);
    } else if (mode === 'move') {
      const links = layer.selectAll<SVGAElement, PositionedWord>('a').data(words, word => word.text);
      links.exit().transition('cloud').duration(duration).attr('opacity', 0).remove();
      const entering = appendWords(links.enter()).attr('opacity', 0);
      entering.select('text').attr('font-size', word => word.size).attr('transform', position);
      const joined = entering.merge(links);
      joined.select('title').text(tooltip);
      joined.transition('cloud').duration(duration).ease(easeCubicInOut).attr('opacity', 1)
        .select('text').attr('font-size', word => word.size).attr('transform', position);
    } else {
      const outgoing = layer;
      layer = group.append('g').attr('class', 'cloud-layer').attr('opacity', 0);
      paint(layer, words);
      const fadeDuration = mode === 'fade' ? duration / 2 : duration;
      outgoing.transition('cloud').duration(fadeDuration).attr('opacity', 0).remove();
      layer.transition('cloud').delay(mode === 'fade' ? fadeDuration : 0)
        .duration(fadeDuration).attr('opacity', 1);
    }
  }

  function draw() {
    layout?.stop();
    const current = ++generation;
    const width = Math.floor(container.clientWidth);
    const height = Math.floor(container.clientHeight);
    if (width < 1 || height < 1) return;
    svg.attr('viewBox', `0 0 ${width} ${height}`);
    group.attr('transform', `translate(${width / 2},${height / 2})`);
    const selected = latest.slice(0, 100);
    if (!selected.length) { render([]); onDraw(0, 0); return; }
    const maximum = selected[0]!.count;
    const maxSize = Math.min(76, width / 9, height / 5);
    const minSize = Math.min(14, maxSize);
    const size = scaleSqrt().domain([1, maximum]).range([minSize, maxSize]);
    const words = selected.map(word => ({ ...word, size: maximum === 1 ? (minSize + maxSize) / 2 : size(word.count) }));
    layout = cloud<PositionedWord>().size([width, height]).words(words).padding(3).rotate(0)
      .font('sans-serif').fontSize(word => word.size).timeInterval(12)
      .on('end', placed => {
        if (current !== generation) return;
        render(placed);
        onDraw(placed.length, selected.length);
      });
    layout.start();
  }
  const observer = new ResizeObserver(() => { if (received) draw(); });
  let received = false;
  observer.observe(container);
  return {
    update(words: WordCount[]) { received = true; latest = words; draw(); },
    setTransition(mode: CloudTransition) { transitionMode = mode; if (received) draw(); },
    destroy() { ++generation; layout?.stop(); group.selectAll('*').interrupt('cloud'); group.remove(); observer.disconnect(); },
  };
}
