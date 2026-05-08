(() => {
  const groups = [
    {
      key: 'stock-features',
      label: '选股功能',
      href: '/stock-screener',
      children: [
        { key: 'stock-screener', label: '选股策略', href: '/stock-screener' },
        { key: 'batch-analysis', label: '批量分析', href: '/batch-analysis' },
        { key: 'single-stock-analysis', label: '单股分析', href: '/single-stock-analysis' },
        { key: 'single-stock-analysis-legacy', label: '单股分析_老版本', href: '/single-stock-analysis-legacy' },
      ],
    },
    {
      key: 'watch-stocks-group',
      label: '关注股票列表',
      href: '/index',
      children: [
        { key: 'watch-stocks', label: '股票列表', href: '/index' },
        { key: 'entry-decision', label: '进场决策', href: '/entry-decision' },
        { key: 'stock-analysis-record', label: '股票分析', href: '/stock-analysis-record' },
        { key: 'trade-plan-analysis', label: '持仓计划分析', href: '/trade-plan-analysis' },
        { key: 'watch-records', label: '历史记录', href: '/index#watch-records' },
      ],
    },
    {
      key: 'holding-stocks-group',
      label: '持仓股票列表',
      href: '/holding-stocks#holding-table',
      children: [
        { key: 'holding-table', label: '持仓列表', href: '/holding-stocks#holding-table' },
        { key: 'holding-reanalysis', label: '二次分析', href: '/holding-reanalysis' },
        { key: 'position-decision', label: '买卖决策', href: '/position-decision' },
        { key: 'holding-review', label: '复盘', href: '/holding-review' },
        { key: 'holding-records', label: '历史记录', href: '/holding-records' },
      ],
    },
    {
      key: 'portfolio-review-group',
      label: '整体分析和复盘',
      href: '/portfolio-review',
      children: [
        { key: 'portfolio-review', label: '组合概览', href: '/portfolio-review' },
        { key: 'portfolio-summary', label: '组合概览', href: '/portfolio-review#portfolio-summary' },
        { key: 'portfolio-records', label: '历史记录', href: '/portfolio-review#portfolio-records' },
      ],
    },
    {
      key: 'system-config-group',
      label: '系统配置',
      href: '/ai-config',
      children: [
        { key: 'ai-config', label: 'AI配置', href: '/ai-config' },
        { key: 'business-config', label: '业务配置', href: '/business-config' },
      ],
    },
  ];

  const storageKey = 'trading-decision-nav-collapsed';
  const collapsedState = new Set(JSON.parse(localStorage.getItem(storageKey) || '[]'));

  const pageMeta = {
    'stock-screener': { groupKey: 'stock-features', groupActive: true, currentKey: 'stock-screener', brandText: '选股与分析兼容入口，统一到当前系统风格。' },
    'batch-analysis': { groupKey: 'stock-features', groupActive: true, currentKey: 'batch-analysis', brandText: '选股与分析兼容入口，统一到当前系统风格。' },
    'single-stock-analysis': { groupKey: 'stock-features', groupActive: true, currentKey: 'single-stock-analysis', brandText: '选股与分析兼容入口，统一到当前系统风格。' },
    'single-stock-analysis-legacy': { groupKey: 'stock-features', groupActive: true, currentKey: 'single-stock-analysis-legacy', brandText: '选股与分析兼容入口，统一到当前系统风格。' },
    'watch-stocks': { groupKey: 'watch-stocks-group', groupActive: true, currentKey: 'watch-stocks', brandText: '树状栏目导航，统一切换三个主页面与按钮动作页面。' },
    'entry-decision': { groupKey: 'watch-stocks-group', groupActive: true, currentKey: 'entry-decision', brandText: '决策动作页，统一承接买前研究与记录追踪。' },
    'stock-analysis-record': { groupKey: 'watch-stocks-group', groupActive: true, currentKey: 'stock-analysis-record', brandText: '决策动作页，统一承接买前研究与记录追踪。' },
    'trade-plan-analysis': { groupKey: 'watch-stocks-group', groupActive: true, currentKey: 'trade-plan-analysis', brandText: '决策动作页，统一承接买前研究与记录追踪。' },
    'holding-stocks': { groupKey: 'holding-stocks-group', groupActive: true, currentKey: 'holding-table', expandedGroupKeys: ['watch-stocks-group', 'holding-stocks-group'], brandText: '持有管理工作台，统一查看累计成本、买卖明细、决策记录与阶段复盘。' },
    'holding-records': { groupKey: 'holding-stocks-group', groupActive: true, currentKey: 'holding-records', expandedGroupKeys: ['watch-stocks-group', 'holding-stocks-group'], brandText: '持有管理工作台，统一查看二次分析、买卖决策与复盘历史记录。' },
    'holding-review': { groupKey: 'holding-stocks-group', groupActive: true, currentKey: 'holding-review', expandedGroupKeys: ['watch-stocks-group', 'holding-stocks-group'], brandText: '持有管理工作台，统一沉淀复盘结论，并在页面内选择周、月或季度视角。' },
    'holding-reanalysis': { groupKey: 'holding-stocks-group', groupActive: true, currentKey: 'holding-reanalysis', expandedGroupKeys: ['watch-stocks-group', 'holding-stocks-group'], brandText: '持有管理工作台，统一检查原逻辑是否成立、变化因素和计划影响。' },
    'position-decision': { groupKey: 'holding-stocks-group', groupActive: true, currentKey: 'position-decision', expandedGroupKeys: ['watch-stocks-group', 'holding-stocks-group'], brandText: '持有管理工作台，统一承接补仓、减仓、卖出三类买卖决策。' },
    'add-position-decision': { groupKey: 'holding-stocks-group', groupActive: true, currentKey: 'position-decision', expandedGroupKeys: ['watch-stocks-group', 'holding-stocks-group'], brandText: '持有管理工作台，统一承接补仓、减仓、卖出三类买卖决策。' },
    'reduce-position-decision': { groupKey: 'holding-stocks-group', groupActive: true, currentKey: 'position-decision', expandedGroupKeys: ['watch-stocks-group', 'holding-stocks-group'], brandText: '持有管理工作台，统一承接补仓、减仓、卖出三类买卖决策。' },
    'sell-decision': { groupKey: 'holding-stocks-group', groupActive: true, currentKey: 'position-decision', expandedGroupKeys: ['watch-stocks-group', 'holding-stocks-group'], brandText: '持有管理工作台，统一承接补仓、减仓、卖出三类买卖决策。' },
    'holding-status-refresh': { groupKey: 'holding-stocks-group', groupActive: true, currentKey: 'holding-table', expandedGroupKeys: ['watch-stocks-group', 'holding-stocks-group'], brandText: '持有管理工作台，状态刷新会回填到持仓列表当前摘要。' },
    'weekly-holding-review': { groupKey: 'holding-stocks-group', groupActive: true, currentKey: 'holding-review', expandedGroupKeys: ['watch-stocks-group', 'holding-stocks-group'], brandText: '持有管理工作台，统一沉淀复盘结论，并在页面内选择周、月或季度视角。' },
    'monthly-holding-review': { groupKey: 'holding-stocks-group', groupActive: true, currentKey: 'holding-review', expandedGroupKeys: ['watch-stocks-group', 'holding-stocks-group'], brandText: '持有管理工作台，统一沉淀复盘结论，并在页面内选择周、月或季度视角。' },
    'quarterly-holding-review': { groupKey: 'holding-stocks-group', groupActive: true, currentKey: 'holding-review', expandedGroupKeys: ['watch-stocks-group', 'holding-stocks-group'], brandText: '持有管理工作台，统一沉淀复盘结论，并在页面内选择周、月或季度视角。' },
    'portfolio-review': { groupKey: 'portfolio-review-group', groupActive: true, currentKey: 'portfolio-review', brandText: '组合层视角的整体分析与阶段复盘入口。' },
    'ai-config': { groupKey: 'system-config-group', groupActive: true, currentKey: 'ai-config', brandText: '系统配置入口，统一查看 AI 与业务配置。' },
    'business-config': { groupKey: 'system-config-group', groupActive: true, currentKey: 'business-config', brandText: '系统配置入口，统一查看 AI 与业务配置。' },
  };

  function persist() {
    localStorage.setItem(storageKey, JSON.stringify(Array.from(collapsedState)));
  }

  function shouldCollapsed(groupKey, currentGroupKey, expandedGroupKeys = []) {
    if (groupKey === currentGroupKey) return false;
    if (expandedGroupKeys.includes(groupKey)) return false;
    return collapsedState.has(groupKey);
  }

  function renderSidebar(sidebar) {
    const pageKey = sidebar.dataset.page;
    const meta = pageMeta[pageKey] || {};
    const brandText = meta.brandText || '交易决策中心统一导航入口。';
    const currentGroupKey = meta.groupKey;
    const currentKey = meta.currentKey;
    const expandedGroupKeys = Array.isArray(meta.expandedGroupKeys) ? meta.expandedGroupKeys : [];

    const brand = document.createElement('div');
    brand.className = 'brand';
    brand.innerHTML = `<h1>交易决策中心</h1><p>${brandText}</p>`;

    const title = document.createElement('div');
    title.className = 'tree-title';
    title.textContent = '主页面';

    const tree = document.createElement('ul');
    tree.className = 'tree';

    groups.forEach((group) => {
      const li = document.createElement('li');
      li.dataset.groupKey = group.key;
      if (shouldCollapsed(group.key, currentGroupKey, expandedGroupKeys)) {
        li.classList.add('collapsed');
      }

      const button = document.createElement('button');
      button.type = 'button';
      button.className = 'group-toggle';
      if (group.key === currentGroupKey && meta.groupActive) {
        button.classList.add('active');
      }
      button.innerHTML = `<span class="label">${group.label}</span><span class="chevron">▾</span>`;
      button.addEventListener('click', () => {
        li.classList.toggle('collapsed');
        if (li.classList.contains('collapsed')) {
          collapsedState.add(group.key);
        } else {
          collapsedState.delete(group.key);
        }
        persist();
      });

      const children = document.createElement('ul');
      children.className = 'children';
      group.children.forEach((child) => {
        const childLi = document.createElement('li');
        const link = document.createElement('a');
        link.href = child.href;
        link.textContent = child.label;
        if (child.key === currentKey) {
          link.classList.add('current');
        }
        childLi.appendChild(link);
        children.appendChild(childLi);
      });

      li.appendChild(button);
      li.appendChild(children);
      tree.appendChild(li);
    });

    sidebar.innerHTML = '';
    sidebar.appendChild(brand);
    sidebar.appendChild(title);
    sidebar.appendChild(tree);
  }

  document.addEventListener('DOMContentLoaded', () => {
    document.querySelectorAll('.sidebar[data-page]').forEach(renderSidebar);
  });
})();
