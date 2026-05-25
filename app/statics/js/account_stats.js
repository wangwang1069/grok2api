/**
 * Request Statistics Dashboard for the Account Overview page.
 *
 * Renders a self-contained panel with:
 *   - Period selector (1d / 3d / 7d / all)
 *   - Overall success / fail / rate cards
 *   - Per-account success-rate table
 *   - Error-type distribution bar chart (pure CSS)
 *
 * Usage (in account.html):
 *   <div id="request-stats-panel"></div>
 *   <script src="/admin/statics/js/account_stats.js"></script>
 *   <script>RequestStatsDashboard.init('#request-stats-panel');</script>
 */

/* eslint-disable no-unused-vars */
const RequestStatsDashboard = (() => {
  let _container = null;
  let _currentPeriod = '1d';
  let _data = null;

  const PERIODS = [
    { key: '1d', label: '最近1天' },
    { key: '3d', label: '最近3天' },
    { key: '7d', label: '最近7天' },
    { key: 'all', label: '全部' },
  ];

  const ERROR_COLORS = {
    rate_limited: '#ea580c',
    forbidden: '#dc2626',
    auth_failure: '#9333ea',
    server_error: '#64748b',
    timeout: '#f59e0b',
    quota_exhausted: '#0ea5e9',
    other: '#94a3b8',
  };

  const ERROR_LABELS = {
    rate_limited: '限流 (429)',
    forbidden: '禁止 (403)',
    auth_failure: '认证失败',
    server_error: '服务错误 (5xx)',
    timeout: '超时',
    quota_exhausted: '配额耗尽',
    other: '其他',
  };

  async function _api(method, path) {
    if (typeof window._api === 'function') {
      return window._api(method, path);
    }
    const key = await window.adminKey?.get?.() || '';
    const r = await fetch((window.ADMIN_API || '/admin/api') + path, {
      method,
      headers: { Authorization: `Bearer ${key}` },
    });
    if (!r.ok) throw new Error(`API ${r.status}`);
    return r.json();
  }

  function fmt(n) {
    if (n == null) return '-';
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return String(n);
  }

  function fmtRate(rate) {
    if (rate == null) return '-';
    return (rate * 100).toFixed(1) + '%';
  }

  function fmtDate(ms) {
    if (!ms) return '-';
    const d = new Date(ms);
    const pad = n => String(n).padStart(2, '0');
    return `${d.getMonth() + 1}/${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
  }

  function rateColor(rate) {
    if (rate == null) return '#94a3b8';
    if (rate >= 0.9) return '#16a34a';
    if (rate >= 0.7) return '#ea580c';
    return '#dc2626';
  }

  function _render() {
    if (!_container || !_data) return;
    const d = _data;
    const o = d.overall;

    _container.innerHTML = `
      <div class="rs-dashboard">
        <div class="rs-header">
          <div class="rs-title">请求数据看板</div>
          <div class="rs-period-tabs">
            ${PERIODS.map(p =>
              `<button class="rs-tab${p.key === _currentPeriod ? ' active' : ''}" data-period="${p.key}">${p.label}</button>`
            ).join('')}
          </div>
        </div>

        <div class="rs-overview">
          <div class="rs-card">
            <div class="rs-card-label">请求总数</div>
            <div class="rs-card-value">${fmt(o.total)}</div>
          </div>
          <div class="rs-card rs-card-success">
            <div class="rs-card-label">成功数</div>
            <div class="rs-card-value">${fmt(o.success)}</div>
          </div>
          <div class="rs-card rs-card-fail">
            <div class="rs-card-label">失败数</div>
            <div class="rs-card-value">${fmt(o.fail)}</div>
          </div>
          <div class="rs-card">
            <div class="rs-card-label">成功率</div>
            <div class="rs-card-value" style="color:${rateColor(o.success_rate)}">${fmtRate(o.success_rate)}</div>
          </div>
          <div class="rs-card">
            <div class="rs-card-label">失败率</div>
            <div class="rs-card-value" style="color:${o.fail_rate != null && o.fail_rate > 0.3 ? '#dc2626' : '#64748b'}">${fmtRate(o.fail_rate)}</div>
          </div>
        </div>

        <div class="rs-body">
          <div class="rs-left">
            <div class="rs-section-title">分账号统计</div>
            <div class="rs-table-wrap">
              <table class="rs-table">
                <thead>
                  <tr>
                    <th>账号</th>
                    <th>池</th>
                    <th>成功</th>
                    <th>失败</th>
                    <th>成功率</th>
                    <th>最近失败原因</th>
                  </tr>
                </thead>
                <tbody>
                  ${d.per_account.map(a => `
                    <tr>
                      <td class="rs-mono" title="${a.token}">${a.token}</td>
                      <td><span class="rs-pool rs-pool-${a.pool}">${a.pool}</span></td>
                      <td>${fmt(a.success)}</td>
                      <td>${a.fail > 0 ? `<span style="color:#dc2626">${fmt(a.fail)}</span>` : fmt(a.fail)}</td>
                      <td style="color:${rateColor(a.success_rate)};font-weight:600">${fmtRate(a.success_rate)}</td>
                      <td class="rs-reason">${a.last_fail_reason || '-'}</td>
                    </tr>
                  `).join('')}
                </tbody>
              </table>
            </div>
          </div>
          <div class="rs-right">
            <div class="rs-section-title">错误类型分布</div>
            <div class="rs-error-chart">
              ${_renderErrorChart(d.error_distribution, o.fail)}
            </div>
          </div>
        </div>
      </div>
    `;

    _container.querySelectorAll('.rs-tab').forEach(btn => {
      btn.addEventListener('click', () => {
        _currentPeriod = btn.dataset.period;
        _load();
      });
    });
  }

  function _renderErrorChart(dist, totalFail) {
    const entries = Object.entries(dist).sort((a, b) => b[1] - a[1]);
    if (!entries.length) {
      return '<div class="rs-empty">暂无错误数据</div>';
    }
    const maxVal = entries[0][1];

    return entries.map(([key, count]) => {
      const pct = totalFail > 0 ? (count / totalFail * 100).toFixed(1) : '0.0';
      const barW = maxVal > 0 ? (count / maxVal * 100) : 0;
      const color = ERROR_COLORS[key] || ERROR_COLORS.other;
      const label = ERROR_LABELS[key] || key;
      return `
        <div class="rs-bar-row">
          <div class="rs-bar-label">${label}</div>
          <div class="rs-bar-track">
            <div class="rs-bar-fill" style="width:${barW}%;background:${color}"></div>
          </div>
          <div class="rs-bar-count">${fmt(count)} <span class="rs-bar-pct">(${pct}%)</span></div>
        </div>
      `;
    }).join('');
  }

  async function _load() {
    try {
      _data = await _api('GET', `/stats/requests?period=${_currentPeriod}`);
      _render();
    } catch (e) {
      if (_container) {
        _container.innerHTML = `<div class="rs-dashboard"><div class="rs-error">加载统计数据失败: ${e.message}</div></div>`;
      }
    }
  }

  function init(selector) {
    _container = document.querySelector(selector);
    if (!_container) return;
    _load();
  }

  function refresh() {
    if (_container) _load();
  }

  return { init, refresh };
})();
