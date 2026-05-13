export const typeConfig = {
    "Final Rule": { color: "rgb(239, 123, 91)" },
    "Enforcement Action": { color: "rgb(255, 160, 91)" },
    "NPRM": { color: "rgb(255, 222, 51)" },
    "Guidance/Circular": { color: "rgb(0, 145, 90)" },
    "Examination Priority": { color: "#475569" },
    "Speech/Statement": { color: "#94a3b8" }
};

export const charts = {};

export function initChartDefaults() {
    if (typeof Chart !== 'undefined') {
        Chart.defaults.color = '#64748b';
        Chart.defaults.borderColor = '#e2e8f0';
        Chart.defaults.font.size = 11;
    }
}

export function renderCharts(data) {
    if (typeof Chart === 'undefined') return;

    try {
        const months = [...new Set(data.map(d => d.date ? d.date.substring(0, 7) : "Unknown").filter(d => d !== "Unknown"))].sort().slice(-6);
        const volData = months.map(m => data.filter(d => d.date && d.date.startsWith(m)).length);
        
        if(charts.velocity) charts.velocity.destroy();
        charts.velocity = new Chart(document.getElementById('velocityChart'), {
            type: 'line',
            data: { 
                labels: months, 
                datasets: [{ label: 'Actions Issued', data: volData, borderColor: '#3b82f6', backgroundColor: 'rgba(59, 130, 246, 0.1)', fill: true, tension: 0.3 }] 
            },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } } }
        });

        const types = Object.keys(typeConfig);
        const typeCounts = types.map(t => data.filter(d => d.type === t).length);
        const typeColors = types.map(t => typeConfig[t].color);

        if(charts.donut) charts.donut.destroy();
        charts.donut = new Chart(document.getElementById('donutChart'), {
            type: 'doughnut',
            data: { labels: types, datasets: [{ data: typeCounts, backgroundColor: typeColors, borderWidth: 0 }] },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right', labels: { boxWidth: 10 } } }, cutout: '70%' }
        });

        const agencies = [...new Set(data.map(d => d.agency).filter(Boolean))].sort();
        
        const datasets = types.map(type => {
            return {
                label: type,
                data: agencies.map(a => data.filter(d => d.agency === a && d.type === type).length),
                backgroundColor: typeConfig[type].color
            };
        });

        if(charts.bar) charts.bar.destroy();
        charts.bar = new Chart(document.getElementById('barChart'), {
            type: 'bar',
            data: { labels: agencies, datasets: datasets },
            options: { 
                indexAxis: 'y', responsive: true, maintainAspectRatio: false, 
                plugins: { legend: { display: true, position: 'bottom', labels: { boxWidth: 10, font: {size: 9} } } }, 
                scales: { x: { stacked: true, beginAtZero: true, ticks: { stepSize: 1 } }, y: { stacked: true } } 
            }
        });

    } catch (e) {
        console.error("Chart generation failed.", e);
    }
}

export function exportChart(canvasId, name) {
    if (typeof Chart === 'undefined') return alert("Charts offline.");
    const link = document.createElement('a');
    link.download = `CTI_Chart_${name}.png`;
    link.href = document.getElementById(canvasId).toDataURL('image/png', 1.0);
    link.click();
}