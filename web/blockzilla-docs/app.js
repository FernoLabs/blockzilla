const menu = document.getElementById('menu-toggle');
const sidebar = document.getElementById('sidebar');
const theme = document.getElementById('theme-toggle');
const announcement = document.getElementById('announcement');

function closeMenu() {
  menu.setAttribute('aria-expanded', 'false');
  sidebar.classList.remove('is-open');
}
menu.addEventListener('click', () => {
  const open = menu.getAttribute('aria-expanded') !== 'true';
  menu.setAttribute('aria-expanded', String(open));
  sidebar.classList.toggle('is-open', open);
});
document.addEventListener('keydown', event => {
  if (event.key === 'Escape' && menu.getAttribute('aria-expanded') === 'true') { closeMenu(); menu.focus(); }
});
document.addEventListener('click', event => {
  if (!sidebar.contains(event.target) && !menu.contains(event.target)) closeMenu();
});

function isDark() {
  return document.documentElement.dataset.theme ? document.documentElement.dataset.theme === 'dark' : window.matchMedia('(prefers-color-scheme: dark)').matches;
}
function updateThemeLabel() { theme.setAttribute('aria-label', `Use ${isDark() ? 'light' : 'dark'} theme`); theme.textContent = isDark() ? 'Light' : 'Dark'; }
theme.addEventListener('click', () => {
  const next = isDark() ? 'light' : 'dark';
  document.documentElement.dataset.theme = next;
  try { localStorage.setItem('blockzilla-docs-theme', next); } catch {}
  updateThemeLabel();
});
window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', updateThemeLabel);
updateThemeLabel();

for (const button of document.querySelectorAll('.copy-code')) {
  button.addEventListener('click', async () => {
    const code = button.closest('.code-block').querySelector('code');
    try {
      await navigator.clipboard.writeText(code.textContent);
      button.textContent = 'Copied';
      announcement.textContent = 'Code copied to clipboard.';
      setTimeout(() => { button.textContent = 'Copy'; }, 1600);
    } catch {
      const selection = window.getSelection();
      const range = document.createRange();
      range.selectNodeContents(code);
      selection.removeAllRanges();
      selection.addRange(range);
      button.textContent = 'Select and copy';
      announcement.textContent = 'Code selected. Use your copy shortcut.';
    }
  });
}

const tocLinks = [...document.querySelectorAll('.contents a')];
if ('IntersectionObserver' in window) {
  const observer = new IntersectionObserver(entries => {
    for (const entry of entries) {
      if (!entry.isIntersecting) continue;
      for (const link of tocLinks) {
        if (link.hash === `#${entry.target.id}`) link.setAttribute('aria-current', 'location');
        else link.removeAttribute('aria-current');
      }
    }
  }, { rootMargin: '-64px 0px -65% 0px' });
  document.querySelectorAll('article h2').forEach(heading => observer.observe(heading));
}
