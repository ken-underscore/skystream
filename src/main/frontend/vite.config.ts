import { defineConfig } from 'vite';
export default defineConfig({
  // Two pages: the word cloud and post search.
  build: { rolldownOptions: { input: ['index.html', 'search.html'] } },
  server: { proxy: { '/api': { target: 'http://localhost:8080', changeOrigin: true } } },
});
