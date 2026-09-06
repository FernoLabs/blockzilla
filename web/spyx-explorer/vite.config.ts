import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

const devApiProxy = process.env.SPYX_DEV_API_PROXY?.trim();

export default defineConfig({
  plugins: [sveltekit()],
  server: devApiProxy
    ? {
        proxy: {
          '/api': { target: devApiProxy },
          '/healthz': { target: devApiProxy }
        }
      }
    : undefined
});
