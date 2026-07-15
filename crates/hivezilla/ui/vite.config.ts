import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [sveltekit()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: process.env.HIVEZILLA_API_URL ?? 'http://127.0.0.1:8787',
        changeOrigin: true
      }
    }
  }
});
