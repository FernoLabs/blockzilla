import adapter from '@sveltejs/adapter-static';

const config = {
  kit: {
    adapter: adapter({
      fallback: '200.html',
      strict: false
    })
  }
};

export default config;
