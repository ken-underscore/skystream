import { scaleOrdinal, scaleSqrt, select } from 'd3';
import cloud from 'd3-cloud';
import type { WordCount } from './bucket';

interface PositionedWord extends WordCount { size: number; x?: number | undefined; y?: number | undefined }

export function createCloud(container: HTMLElement, onDraw: (placed: number, selected: number) => void) {
  const svg = select(container).select<SVGSVGElement>('svg');
  const group = svg.append('g');
  const color = scaleOrdinal<string, string>(['#79c8ff', '#a5e3d5', '#c4b5fd', '#f5c985', '#f7a9ba']);
  let latest: WordCount[] = [];
  let layout: ReturnType<typeof cloud<PositionedWord>> | undefined;
  let generation = 0;
  function draw() {
    layout?.stop();
    const current = ++generation;
    const width = Math.floor(container.clientWidth);
    const height = Math.floor(container.clientHeight);
    if (width < 1 || height < 1) return;
    svg.attr('viewBox', `0 0 ${width} ${height}`);
    group.attr('transform', `translate(${width / 2},${height / 2})`);
    const selected = latest.slice(0, 100);
    if (!selected.length) { group.selectAll('text').remove(); onDraw(0, 0); return; }
    const maximum = selected[0]!.count;
    const maxSize = Math.min(76, width / 9, height / 5);
    const minSize = Math.min(14, maxSize);
    const size = scaleSqrt().domain([1, maximum]).range([minSize, maxSize]);
    const words = selected.map(word => ({ ...word, size: maximum === 1 ? (minSize + maxSize) / 2 : size(word.count) }));
    layout = cloud<PositionedWord>().size([width, height]).words(words).padding(3).rotate(0)
      .font('sans-serif').fontSize(word => word.size).timeInterval(12)
      .on('end', placed => {
        if (current !== generation) return;
        group.selectAll<SVGTextElement, PositionedWord>('text').data(placed, word => word.text).join('text')
          .attr('text-anchor', 'middle').attr('font-family', 'sans-serif').attr('font-size', word => word.size)
          .attr('fill', word => color(word.text))
          .attr('transform', word => `translate(${word.x ?? 0},${word.y ?? 0})`)
          .text(word => word.text).append('title').text(word => `${word.text}: ${word.count}`);
        onDraw(placed.length, selected.length);
      });
    layout.start();
  }
  const observer = new ResizeObserver(() => { if (received) draw(); });
  let received = false;
  observer.observe(container);
  return {
    update(words: WordCount[]) { received = true; latest = words; draw(); },
    destroy() { ++generation; layout?.stop(); observer.disconnect(); },
  };
}
