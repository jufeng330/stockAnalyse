(() => {
  function escapeHtml(value) {
    return String(value)
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }

  function formatValue(value) {
    if (value === null) return 'null';
    if (typeof value === 'boolean') return value ? 'true' : 'false';
    if (typeof value === 'object') return `<pre class="json-block">${escapeHtml(JSON.stringify(value, null, 2))}</pre>`;
    return `<span>${escapeHtml(value)}</span>`;
  }

  function renderSection(title, value) {
    const rows = typeof value === 'object' && value !== null && !Array.isArray(value)
      ? Object.entries(value).map(([key, item]) => `
          <tr>
            <th>${escapeHtml(key)}</th>
            <td>${formatValue(item)}</td>
          </tr>
        `).join('')
      : `
        <tr>
          <th>${escapeHtml(title)}</th>
          <td>${formatValue(value)}</td>
        </tr>
      `;

    return `
      <section class="config-card">
        <div class="config-card-header">
          <h3>${escapeHtml(title)}</h3>
        </div>
        <div class="config-table-wrap">
          <table class="config-table">
            <tbody>${rows}</tbody>
          </table>
        </div>
      </section>
    `;
  }

  async function bootstrap() {
    const container = document.querySelector('[data-config-scope]');
    if (!container) return;

    const scope = container.dataset.configScope;
    const endpoint = scope === 'ai' ? '/api/config/ai' : '/api/config/business';
    const rawContainer = document.querySelector('[data-config-raw]');

    try {
      const response = await fetch(endpoint);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();

      if (scope === 'ai') {
        container.innerHTML = renderSection('AI 配置', data);
      } else {
        container.innerHTML = Object.entries(data)
          .map(([title, value]) => renderSection(title, value))
          .join('');
      }

      if (rawContainer) {
        rawContainer.innerHTML = `<pre class="json-block">${escapeHtml(JSON.stringify(data, null, 2))}</pre>`;
      }
    } catch (error) {
      container.innerHTML = `
        <section class="config-card error-card">
          <div class="config-card-header"><h3>配置读取失败</h3></div>
          <p class="config-error">${escapeHtml(error.message)}</p>
        </section>
      `;
      if (rawContainer) {
        rawContainer.innerHTML = '';
      }
    }
  }

  document.addEventListener('DOMContentLoaded', bootstrap);
})();
