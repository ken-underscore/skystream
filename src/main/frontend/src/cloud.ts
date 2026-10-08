import { easeCubicInOut, scaleOrdinal, scaleSqrt, select } from 'd3';
import cloud from 'd3-cloud';
import type { WordCount } from './bucket';

interface PositionedWord extends WordCount { size: number; x?: number | undefined; y?: number | undefined }
export type CloudTransition = 'crossfade' | 'move' | 'fade' | 'none';

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

  function paint(target: typeof layer, words: PositionedWord[]) {
    target.selectAll<SVGTextElement, PositionedWord>('text').data(words, word => word.text).join('text')
      .attr('text-anchor', 'middle').attr('font-family', 'sans-serif').attr('font-size', word => word.size).attr('opacity', 1)
      .attr('fill', word => color(word.text))
      .attr('transform', word => `translate(${word.x ?? 0},${word.y ?? 0})`)
      .text(word => word.text).append('title').text(word => `${word.text}: ${word.count}`);
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
      const text = layer.selectAll<SVGTextElement, PositionedWord>('text').data(words, word => word.text);
      text.exit().transition('cloud').duration(duration).attr('opacity', 0).remove();
      const entering = text.enter().append('text').attr('opacity', 0)
        .attr('font-size', word => word.size)
        .attr('transform', word => `translate(${word.x ?? 0},${word.y ?? 0})`);
      const joined = entering.merge(text)
        .attr('text-anchor', 'middle').attr('font-family', 'sans-serif').attr('fill', word => color(word.text))
        .text(word => word.text);
      joined.append('title').text(word => `${word.text}: ${word.count}`);
      joined.transition('cloud').duration(duration).ease(easeCubicInOut)
        .attr('font-size', word => word.size).attr('opacity', 1)
        .attr('transform', word => `translate(${word.x ?? 0},${word.y ?? 0})`);
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
