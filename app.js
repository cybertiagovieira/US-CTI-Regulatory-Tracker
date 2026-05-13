import { fetchSha, syncToGitHubAtomic } from './api.js';
import { typeConfig, initChartDefaults, renderCharts, exportChart } from './charts.js';

let rawData = [];
let currentFileSha = '';
let isProcessing = false;
let filterTimeout = null;

function sanitizeHTML(str) {
    if (!str) return "";
    const temp = document.createElement('div');
    temp.textContent = str;
    return temp.innerHTML;
}

function calculateDays(targetDateStr) {
    if(!targetDateStr) return null;
    const parts = targetDateStr.split('-');
    if(parts.length !== 3) return null;
    const today = new Date();
    today.setHours(0,0,0,0);
    const target = new Date(parts[0], parts[1] - 1, parts[2]);
    if(isNaN(target.getTime())) return null;
    target.setHours(0,0,0,0);
    return Math.ceil((target - today) / (1000 * 60 * 60 * 24));
}

async function init() {
    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.get('admin') === 'true') document.body.classList.add('admin-mode');

    if (window.INJECTED_DATA) {
        rawData = window.INJECTED_DATA;
    } else if (window.location.protocol !== 'file:') {
        try {
            const response = await fetch(`master_data.json?t=${new Date().getTime()}`);
            if(response.ok) rawData = await response.json();
        } catch (error) { console.error("Data inaccessible."); }
    }

    const repo = document.getElementById('gh-repo')?.value;
    currentFileSha = await fetchSha(repo);
    
    populateFilterOptions();
    generateQuickFilters();
    initChartDefaults();
    applyFilters();
}

function populateFilterOptions() {
    const monthSelect = document.getElementById('filter-month');
    monthSelect.innerHTML = '<option value="All">All Time</option>';
    const months = [...new Set(rawData.filter(d => d.date && d.date.length >= 7).map(d => d.date.substring(0, 7)))].sort().reverse();
    months.forEach(m => {
        const label = new Date(m + "-02").toLocaleString('default', { month: 'short', year: 'numeric' });
        monthSelect.innerHTML += `<option value="${m}">${label}</option>`;
    });

    const selects = ['filter-agency', 'filter-type', 'filter-theme', 'filter-sector'];
    const keys = ['agency', 'type', 'theme', 'target_sector'];
    const labels = ['Agencies', 'Action Types', 'Threat Themes', 'Target Sectors'];

    selects.forEach((id, i) => {
        const el = document.getElementById(id);
        el.innerHTML = `<option value="All">All ${labels[i]}</option>`;
        const unique = [...new Set(rawData.map(d => d[keys[i]]).filter(Boolean))].sort();
        unique.forEach(val => el.innerHTML += `<option value="${val}">${val}</option>`);
    });
}

function generateQuickFilters() {
    const container = document.getElementById('quick-filters');
    if(!container) return;
    container.innerHTML = '';
    const terms = ["Stablecoin", "AML", "Whistleblower", "Crypto", "Reporting", "Cyber"];
    terms.forEach(term => {
        const btn = document.createElement('button');
        btn.className = 'chip';
        btn.textContent = term;
        btn.onclick = () => {
            document.getElementById('filter-text').value = term;
            applyFilters();
        };
        container.appendChild(btn);
    });
}

function debounceFilters() {
    if (filterTimeout) clearTimeout(filterTimeout);
    filterTimeout = setTimeout(() => applyFilters(), 300);
}

function switchView(viewType) {
    document.getElementById('view-timeline').classList.remove('active');
    document.getElementById('view-kanban').classList.remove('active');
    const mainContent = document.querySelector('.main-content');
    if (viewType === 'timeline') {
        document.getElementById('view-timeline').classList.add('active');
        mainContent.classList.remove('kanban-mode');
    } else {
        document.getElementById('view-kanban').classList.add('active');
        mainContent.classList.add('kanban-mode');
    }
}

function switchTab(tab) {
    document.getElementById('tab-timeline').classList.remove('active');
    document.getElementById('tab-quarantine').classList.remove('active');
    const viewControls = document.getElementById('active-view-controls');
    const mainContent = document.querySelector('.main-content');
    if (tab === 'timeline') {
        document.getElementById('tab-timeline').classList.add('active');
        mainContent.classList.remove('quarantine-mode');
        if(viewControls) viewControls.style.display = 'flex';
    } else {
        document.getElementById('tab-quarantine').classList.add('active');
        mainContent.classList.add('quarantine-mode');
        if(viewControls) viewControls.style.display = 'none';
    }
}

function applyFilters() {
    const filters = {
        month: document.getElementById('filter-month').value,
        agency: document.getElementById('filter-agency').value,
        type: document.getElementById('filter-type').value,
        theme: document.getElementById('filter-theme').value,
        sector: document.getElementById('filter-sector').value,
        text: document.getElementById('filter-text').value.toLowerCase()
    };

    const quarantineRegex = /(rate limit exceeded|llm processing error)/i;
    let validData = [];
    let quarantineData = [];

    rawData.forEach(item => {
        const safeTitle  = item.title ? String(item.title).toLowerCase() : "";
        const safeSummary = item.summary ? String(item.summary).toLowerCase() : "";
        const safeId     = item.id ? String(item.id).toLowerCase() : "";
        const safeDate   = item.date ? String(item.date) : "";
        const safeAgency = item.agency ? String(item.agency) : "";
        const safeType   = item.type ? String(item.type) : "";
        const safeTheme  = item.theme ? String(item.theme) : "";
        const safeSector = item.target_sector ? String(item.target_sector) : "";

        let matchText = filters.text === "" || safeTitle.includes(filters.text) || safeSummary.includes(filters.text) || safeId.includes(filters.text);
        
        const matchesFilters = (filters.month === "All" || safeDate.startsWith(filters.month)) &&
               (filters.agency === "All" || safeAgency === filters.agency) &&
               (filters.type === "All" || safeType === filters.type) &&
               (filters.theme === "All" || safeTheme === filters.theme) &&
               (filters.sector === "All" || safeSector === filters.sector) &&
               matchText;

        if (matchesFilters) {
            if (quarantineRegex.test(safeSummary)) { quarantineData.push(item); } 
            else { validData.push(item); }
        }
    });

    const sortLogic = (a, b) => {
        const daysA = calculateDays(a.effective_date);
        const daysB = calculateDays(b.effective_date);
        if (daysA !== null && daysA >= 0 && daysB !== null && daysB >= 0) return daysA - daysB;
        if (daysA !== null && daysA >= 0) return -1;
        if (daysB !== null && daysB >= 0) return 1;
        const timeA = a.date ? new Date(a.date).getTime() : 0;
        const timeB = b.date ? new Date(b.date).getTime() : 0;
        return (isNaN(timeB) ? 0 : timeB) - (isNaN(timeA) ? 0 : timeA);
    };

    validData.sort(sortLogic);
    quarantineData.sort(sortLogic);
    
    const qBadge = document.getElementById('count-quarantine');
    qBadge.textContent = quarantineData.length;
    if (quarantineData.length > 0) qBadge.classList.remove('zero');
    else qBadge.classList.add('zero');

    updateKPIs(validData); 
    renderCharts(validData);
    renderTimeline(validData, 'timelineContainer', false);
    renderKanban(validData);
    renderTimeline(quarantineData, 'quarantineContainer', true);
}

function updateKPIs(data) {
    document.getElementById('kpi-total').textContent = data.length;
    if (data.length === 0) {
        document.getElementById('kpi-agency').textContent = "-";
        document.getElementById('kpi-theme').textContent = "-";
        document.getElementById('kpi-high-sev').textContent = "0";
        document.getElementById('kpi-deadlines').textContent = "0";
        return;
    }
    const counts = { agency: {}, theme: {} };
    let highSev = 0; let nearDeadline = 0;

    data.forEach(d => {
        if(d.agency) counts.agency[d.agency] = (counts.agency[d.agency] || 0) + 1;
        if(d.theme && d.theme !== "General") counts.theme[d.theme] = (counts.theme[d.theme] || 0) + 1;
        if(d.severity === 'High') highSev++;
        const days = calculateDays(d.effective_date);
        if(days !== null && days >= 0 && days <= 90) nearDeadline++;
    });

    const topAgency = Object.keys(counts.agency).length ? Object.keys(counts.agency).reduce((a, b) => counts.agency[a] > counts.agency[b] ? a : b) : "-";
    const topTheme = Object.keys(counts.theme).length ? Object.keys(counts.theme).reduce((a, b) => counts.theme[a] > counts.theme[b] ? a : b) : "-";

    document.getElementById('kpi-agency').textContent = topAgency;
    document.getElementById('kpi-theme').textContent = topTheme;
    document.getElementById('kpi-high-sev').textContent = highSev;
    document.getElementById('kpi-deadlines').textContent = nearDeadline;
}

function createEventCard(item, isQuarantine) {
    const cfg = typeConfig[item.type] || { color: "#475569" };
    const badgeTextColor = item.type === "NPRM" ? "#000000" : "#ffffff";
    const sevClass = item.severity ? `sev-${item.severity}` : '';
    const quarantineClass = isQuarantine ? 'quarantine-flag' : '';

    const eventDiv = document.createElement('div');
    eventDiv.className = `event ${sevClass} ${quarantineClass}`;

    const deleteBtn = document.createElement('button');
    deleteBtn.className = 'btn-delete';
    deleteBtn.textContent = '×';
    deleteBtn.onclick = () => deleteEntry(item.id);
    eventDiv.appendChild(deleteBtn);

    const metaDiv = document.createElement('div');
    metaDiv.className = 'meta';

    if(item.id) {
        const idBadge = document.createElement('span');
        idBadge.className = 'badge id-tag';
        idBadge.textContent = String(item.id).substring(0,8);
        idBadge.title = item.id;
        metaDiv.appendChild(idBadge);
    }
    const typeBadge = document.createElement('span');
    typeBadge.className = 'badge';
    typeBadge.style.background = cfg.color;
    typeBadge.style.color = badgeTextColor;
    typeBadge.textContent = item.type;
    metaDiv.appendChild(typeBadge);

    if(item.agency) {
        const agencyBadge = document.createElement('span');
        agencyBadge.className = 'badge agency';
        agencyBadge.textContent = item.agency;
        metaDiv.appendChild(agencyBadge);
    }
    if(item.theme && item.theme !== "General") {
        const themeBadge = document.createElement('span');
        themeBadge.className = 'badge theme';
        themeBadge.textContent = item.theme;
        metaDiv.appendChild(themeBadge);
    }
    if(item.target_sector && item.target_sector !== "All Entities") {
        const sectorBadge = document.createElement('span');
        sectorBadge.className = 'badge sector';
        sectorBadge.textContent = item.target_sector;
        metaDiv.appendChild(sectorBadge);
    }

    const daysLeft = calculateDays(item.effective_date);
    if (daysLeft !== null && daysLeft >= 0) {
        const cdBadge = document.createElement('span');
        if (item.type === 'NPRM') {
            cdBadge.className = `badge countdown ${daysLeft <= 15 ? 'urgent' : ''}`;
            cdBadge.style.background = '#d97706';
            cdBadge.textContent = `Comment Closes: T-${daysLeft} Days`;
        } else {
            cdBadge.className = `badge countdown ${daysLeft <= 30 ? 'urgent' : ''}`;
            cdBadge.textContent = `Compliance: T-${daysLeft} Days`;
        }
        metaDiv.appendChild(cdBadge);
    }

    const dateSpan = document.createElement('span');
    dateSpan.className = 'date';
    dateSpan.textContent = `Issued: ${item.date}`;
    metaDiv.appendChild(dateSpan);
    eventDiv.appendChild(metaDiv);

    const titleH2 = document.createElement('h2');
    titleH2.className = 'title';
    titleH2.textContent = item.title;
    eventDiv.appendChild(titleH2);

    const summaryP = document.createElement('p');
    summaryP.className = 'summary';
    summaryP.textContent = item.summary;
    eventDiv.appendChild(summaryP);

    if(item.source_url) {
        const link = document.createElement('a');
        link.className = 'source-link';
        link.href = item.source_url;
        link.target = '_blank';
        link.textContent = 'Access Source Document →';
        eventDiv.appendChild(link);
    }
    return eventDiv;
}

function renderTimeline(data, containerId = 'timelineContainer', isQuarantine = false) {
    const container = document.getElementById(containerId);
    if(!container) return;
    container.innerHTML = ''; 
    if (data.length === 0) {
        container.innerHTML = `<div class="empty-state">No ${isQuarantine ? 'corrupted ' : ''}intelligence artifacts match the current filter parameters.</div>`;
        return;
    }
    const fragment = document.createDocumentFragment();
    data.forEach(item => fragment.appendChild(createEventCard(item, isQuarantine)));
    container.appendChild(fragment);
}

function renderKanban(data) {
    const kbProposed = document.getElementById('kb-proposed');
    const kbActive = document.getElementById('kb-active');
    const kbPunitive = document.getElementById('kb-punitive');
    if(!kbProposed || !kbActive || !kbPunitive) return;
    kbProposed.innerHTML = ''; kbActive.innerHTML = ''; kbPunitive.innerHTML = '';

    data.forEach(item => {
        const card = createEventCard(item, false);
        if (item.type === 'NPRM') kbProposed.appendChild(card);
        else if (item.type === 'Enforcement Action' || item.type === 'Examination Priority') kbPunitive.appendChild(card);
        else kbActive.appendChild(card);
    });
}

async function addEntry() {
    if (isProcessing) return; 
    const payload = {
        id: Date.now().toString(),
        date: document.getElementById('in-date').value,
        effective_date: document.getElementById('in-effective-date').value,
        agency: document.getElementById('in-agency').value,
        type: document.getElementById('in-type').value,
        severity: document.getElementById('in-severity').value,
        theme: document.getElementById('in-theme').value,
        target_sector: document.getElementById('in-sector').value,
        title: document.getElementById('in-title').value,
        summary: document.getElementById('in-summary').value
    };
    if(!payload.date || !payload.title || !payload.summary) return alert('Define Date, Title, and Summary.');
    
    isProcessing = true;
    const btn = document.querySelector('.btn-action');
    btn.textContent = "Syncing with GitHub...";
    btn.style.opacity = "0.5";
    
    const token = document.getElementById('gh-token').value;
    const repo = document.getElementById('gh-repo').value;
    const result = await syncToGitHubAtomic('ADD', payload, token, repo);
    
    if(result.success) {
        rawData = result.newData;
        currentFileSha = result.sha;
        document.getElementById('in-title').value = '';
        document.getElementById('in-summary').value = '';
        document.getElementById('in-effective-date').value = '';
        populateFilterOptions();
        applyFilters();
    }
    
    isProcessing = false;
    btn.textContent = "Execute Injection";
    btn.style.opacity = "1";
}

async function deleteEntry(id) {
    if (isProcessing) return;
    if(!confirm("Execute deletion protocol? This action is permanent.")) return;
    isProcessing = true;
    
    const token = document.getElementById('gh-token').value;
    const repo = document.getElementById('gh-repo').value;
    const result = await syncToGitHubAtomic('DELETE', id, token, repo);
    
    if(result.success) { 
        rawData = result.newData;
        currentFileSha = result.sha;
        populateFilterOptions(); 
        applyFilters(); 
    }
    isProcessing = false;
}

function exportData(format) {
    let content, mime, ext;
    if (format === 'csv') {
        const headers = ['id', 'date', 'effective_date', 'severity', 'agency', 'type', 'theme', 'target_sector', 'title', 'summary', 'source_url'];
        const rows = rawData.map(row => headers.map(h => { let val = row[h] ? String(row[h]).replace(/"/g, '""') : ''; return `"${val}"`; }).join(','));
        content = [headers.join(','), ...rows].join('\n');
        mime = 'text/csv;charset=utf-8;'; ext = 'csv';
    } else if (format === 'json') {
        content = JSON.stringify(rawData, null, 2);
        mime = 'application/json'; ext = 'json';
    }
    const blob = new Blob([content], { type: mime });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `US_CTI_Extract_${new Date().toISOString().split('T')[0]}.${ext}`;
    link.click();
}

function exportArtifact() {
    ['filter-month', 'filter-agency', 'filter-type', 'filter-theme', 'filter-sector'].forEach(id => {
        const el = document.getElementById(id);
        if (!el) return;
        Array.from(el.options).forEach(opt => opt.removeAttribute('selected'));
        const selectedOpt = el.querySelector(`option[value="${el.value}"]`);
        if (selectedOpt) selectedOpt.setAttribute('selected', 'selected');
    });
    const textFilter = document.getElementById('filter-text');
    if (textFilter) textFilter.setAttribute('value', textFilter.value);
    
    const docClone = document.documentElement.cloneNode(true);
    docClone.querySelector('body').classList.remove('admin-mode');
    const adminPanel = docClone.querySelector('#admin-panel');
    if (adminPanel) adminPanel.remove();
    
    const cleanData = JSON.stringify(rawData).replace(/</g, '\\u003c');
    let dataNode = docClone.querySelector('#data-layer');
    if (!dataNode) {
        dataNode = document.createElement('script');
        dataNode.id = 'data-layer';
        docClone.head.appendChild(dataNode);
    }
    dataNode.textContent = `window.INJECTED_DATA = ${cleanData};`;
    
    const blob = new Blob(["<!DOCTYPE html>\n" + docClone.outerHTML], { type: 'text/html' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `US_CTI_Report_${new Date().toISOString().split('T')[0]}.html`;
    link.click();
}

function generateExecBrief() {
    const thirtyDaysAgo = new Date(); thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
    
    const briefData = rawData.filter(d => {
        const isHighSev = d.severity === 'High';
        const daysToEffective = calculateDays(d.effective_date);
        const impending = daysToEffective !== null && daysToEffective <= 90 && daysToEffective >= 0;
        const isRecent = d.date ? new Date(d.date) >= thirtyDaysAgo : false;
        const isCriticalType = d.type === 'Final Rule' || d.type === 'Enforcement Action';
        const recentCritical = isRecent && isCriticalType;
        return isHighSev || impending || recentCritical;
    }).sort((a, b) => new Date(b.date) - new Date(a.date));

    let highSevCount = 0; let cliffCount = 0; let agencyCounts = {}; let themeCounts = {};
    briefData.forEach(d => {
        if (d.severity === 'High') highSevCount++;
        const days = calculateDays(d.effective_date);
        if (days !== null && days >= 0 && days <= 30) cliffCount++;
        if (d.agency) agencyCounts[d.agency] = (agencyCounts[d.agency] || 0) + 1;
        if (d.theme && d.theme !== 'General') themeCounts[d.theme] = (themeCounts[d.theme] || 0) + 1;
    });

    const dominantAgency = Object.keys(agencyCounts).length ? Object.keys(agencyCounts).reduce((a, b) => agencyCounts[a] > agencyCounts[b] ? a : b) : 'N/A';
    const dominantTheme = Object.keys(themeCounts).length ? Object.keys(themeCounts).reduce((a, b) => themeCounts[a] > themeCounts[b] ? a : b) : 'N/A';

    const formatBLUF = (text) => {
        if (!text) return "";
        const firstPeriod = text.indexOf('. ');
        if (firstPeriod !== -1) {
            const lead = text.substring(0, firstPeriod + 1);
            const remainder = text.substring(firstPeriod + 1);
            return `<strong>${sanitizeHTML(lead)}</strong>${sanitizeHTML(remainder)}`;
        }
        return `<strong>${sanitizeHTML(text)}</strong>`;
    };

    let html = `<html><head><title>CTI Executive Briefing</title><style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif; padding: 40px; color: #0f172a; max-width: 1000px; margin: 0 auto; } 
        h1 { border-bottom: 3px solid #dc2626; padding-bottom: 10px; color: #0f172a; margin-bottom: 5px; font-size: 28px; text-transform: uppercase; letter-spacing: 1px;} 
        .subtitle { color: #64748b; font-size: 13px; margin-bottom: 30px; font-weight: bold; }
        .macro-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-bottom: 35px; }
        .macro-box { border: 1px solid #cbd5e1; padding: 20px 10px; border-radius: 8px; text-align: center; background: #f8fafc; }
        .macro-value { font-size: 32px; font-weight: bold; color: #dc2626; margin-bottom: 5px; }
        .macro-value.dark { color: #0f172a; }
        .macro-label { font-size: 11px; color: #64748b; text-transform: uppercase; font-weight: bold; letter-spacing: 0.5px; }
        .theme-section { margin-bottom: 20px; page-break-before: always; }
        .theme-section:first-of-type { page-break-before: auto; }
        .theme-banner { background: #1e293b; color: #ffffff; padding: 12px 20px; border-radius: 6px; font-size: 16px; text-transform: uppercase; margin-top: 20px; margin-bottom: 15px; letter-spacing: 1px; page-break-after: avoid; }
        .triage-header { font-size: 13px; font-weight: bold; margin: 25px 0 15px 10px; text-transform: uppercase; letter-spacing: 1px; page-break-after: avoid; }
        .triage-red { color: #dc2626; border-bottom: 2px solid #dc2626; display: inline-block; padding-bottom: 3px; }
        .triage-yellow { color: #d97706; border-bottom: 2px solid #d97706; display: inline-block; padding-bottom: 3px; }
        .item { border: 1px solid #e2e8f0; padding: 20px; margin-bottom: 15px; border-radius: 6px; page-break-inside: avoid; background: #ffffff; } 
        .meta-grid { display: flex; justify-content: space-between; align-items: flex-start; border-bottom: 1px solid #e2e8f0; padding-bottom: 10px; margin-bottom: 15px; }
        .meta-block { display: flex; flex-direction: column; }
        .meta-right { text-align: right; }
        .meta-label { font-size: 10px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; font-weight: bold; margin-bottom: 3px; }
        .meta-value { font-size: 12px; font-weight: bold; color: #334155; text-transform: uppercase; }
        .meta-value.alert { color: #dc2626; }
        .title { margin: 0 0 10px 0; font-size: 17px; color: #0f172a; line-height: 1.3;} 
        .summary { margin: 0; line-height: 1.6; font-size: 14px; color: #334155; white-space: pre-wrap; } 
        @media print { body { padding: 0; } .macro-box, .theme-banner { -webkit-print-color-adjust: exact; print-color-adjust: exact; } }
    </style></head><body>
    <h1>US CTI Executive Brief</h1>
    <div class="subtitle">GENERATED: ${new Date().toLocaleDateString()} | SCOPE: ACTIVE HIGH SEVERITY & IMPENDING DEADLINES</div>`;

    if (briefData.length === 0) {
        html += `<p style="font-weight:bold; color:#64748b;">No critical events detected within the operational threshold.</p>`;
    } else {
        html += `<div class="macro-grid"><div class="macro-box"><div class="macro-value">${highSevCount}</div><div class="macro-label">High Severity Alerts</div></div><div class="macro-box"><div class="macro-value dark">${dominantAgency}</div><div class="macro-label">Dominant Agency</div></div><div class="macro-box"><div class="macro-value dark" style="font-size: 18px; line-height: 38px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${dominantTheme}">${dominantTheme}</div><div class="macro-label">Primary Threat Theme</div></div><div class="macro-box"><div class="macro-value">${cliffCount}</div><div class="macro-label">Deadlines < 30 Days</div></div></div>`;

        const groupedData = {};
        briefData.forEach(d => {
            const themeName = (d.theme && d.theme !== 'General') ? d.theme : 'Uncategorized / Broad Mandates';
            if (!groupedData[themeName]) groupedData[themeName] = [];
            groupedData[themeName].push(d);
        });

        const sortedThemes = Object.keys(groupedData).sort((a, b) => {
            if (a === 'Uncategorized / Broad Mandates') return 1;
            if (b === 'Uncategorized / Broad Mandates') return -1;
            return a.localeCompare(b);
        });

        sortedThemes.forEach(theme => {
            html += `<div class="theme-section"><div class="theme-banner">${theme}</div>`;
            const redZone = []; const yellowZone = [];

            groupedData[theme].forEach(d => {
                const days = calculateDays(d.effective_date);
                const isRed = d.type === 'Final Rule' || d.type === 'Enforcement Action' || (days !== null && days < 45);
                if (isRed) redZone.push({ data: d, days: days });
                else yellowZone.push({ data: d, days: days });
            });

            const renderItem = (obj, isRed) => {
                const d = obj.data; const days = obj.days;
                const dlString = (days !== null && days >= 0) ? `T-${days} DAYS` : 'NO DEADLINE';
                const alertClass = isRed ? 'alert' : '';
                return `<div class="item"><div class="meta-grid"><div class="meta-block"><div class="meta-label">Agency | Date | Type</div><div class="meta-value">${d.agency || 'UNKNOWN'} &nbsp;&bull;&nbsp; ${d.date || 'UNKNOWN'} &nbsp;&bull;&nbsp; ${d.type || 'UNKNOWN'}</div></div><div class="meta-block meta-right"><div class="meta-label">Severity | Deadline</div><div class="meta-value ${alertClass}">${d.severity || 'N/A'} &nbsp;&bull;&nbsp; ${dlString}</div></div></div><h3 class="title">${sanitizeHTML(d.title)}</h3><p class="summary">${formatBLUF(d.summary)}</p></div>`;
            };

            if (redZone.length > 0) {
                html += `<div class="triage-header triage-red">Red Zone (Critical Intelligence & Near-Term Horizon)</div>`;
                redZone.forEach(obj => html += renderItem(obj, true));
            }
            if (yellowZone.length > 0) {
                html += `<div class="triage-header triage-yellow">Yellow Zone (Strategic Horizon)</div>`;
                yellowZone.forEach(obj => html += renderItem(obj, false));
            }
            html += `</div>`; 
        });
    }

    html += `</body></html>`;
    const printWindow = window.open('', '_blank');
    printWindow.document.write(html); 
    printWindow.document.close(); 
    printWindow.focus();
    setTimeout(() => printWindow.print(), 500);
}

// Map essential functions to the global window object to support inline HTML event handlers (e.g., onclick="applyFilters()")
window.applyFilters = applyFilters;
window.debounceFilters = debounceFilters;
window.switchView = switchView;
window.switchTab = switchTab;
window.addEntry = addEntry;
window.deleteEntry = deleteEntry;
window.exportData = exportData;
window.exportArtifact = exportArtifact;
window.generateExecBrief = generateExecBrief;
window.exportChart = exportChart;

// Global Event Listeners
window.addEventListener("beforeunload", function (e) {
    const title = document.getElementById('in-title')?.value;
    const summary = document.getElementById('in-summary')?.value;
    if (title || summary) { e.preventDefault(); e.returnValue = ''; }
});

window.addEventListener('DOMContentLoaded', () => {
    const tokenInput = document.getElementById('gh-token');
    if (localStorage.getItem('cti_gh_token')) { localStorage.removeItem('cti_gh_token'); }
    const sessionToken = sessionStorage.getItem('cti_gh_token_ephemeral');
    if (sessionToken) { tokenInput.value = sessionToken; }
    tokenInput.addEventListener('change', (e) => { sessionStorage.setItem('cti_gh_token_ephemeral', e.target.value); });
    init();
});