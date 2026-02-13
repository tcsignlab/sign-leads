// =====================================================
// AUTONOMOUS SIGN LEAD SCRAPER - BACKEND SERVICE
// =====================================================

const fs = require('fs').promises;
const path = require('path');

// ==================== CONFIGURATION ====================
const CONFIG = {
    US_STATES: [
        'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut',
        'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa',
        'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan',
        'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
        'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina',
        'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island',
        'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont',
        'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'
    ],

    STATE_CODES: {
        'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
        'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
        'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
        'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
        'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
        'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
        'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY',
        'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
        'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
        'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
        'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
        'Wisconsin': 'WI', 'Wyoming': 'WY'
    },

    SCHEDULE: {
        intervalHours: 96,
        nextRunFile: './next-run.json'
    },

    API_KEYS: {
        google: {
            keys: (process.env.GOOGLE_API_KEYS || process.env.GOOGLE_API_KEY || '').split(',').map(k => k.trim()).filter(Boolean),
            searchEngineIds: (process.env.GOOGLE_SEARCH_ENGINE_IDS || process.env.GOOGLE_SEARCH_ENGINE_ID || '').split(',').map(k => k.trim()).filter(Boolean),
            currentIndex: 0,
            endpoint: 'https://www.googleapis.com/customsearch/v1'
        }
    },

    // FIX #3: Drastically reduced franchise list to save API quota
    // Was 35 chains x 50 states = 1,750 calls. Now 8 x 50 = 400 calls.
    FRANCHISE_CHAINS: [
        "Chick-fil-A", "Dutch Bros", "Raising Cane's", "Wingstop",
        "Jersey Mike's", "Crumbl Cookies", "Dollar General", "Buc-ee's"
    ],

    // Keyword searches - focused and quota-efficient
    KEYWORDS: [
        'new store opening',
        'restaurant opening',
        'grand opening retail',
        'commercial development construction',
        'new franchise location',
        'strip mall development',
        'shopping center construction',
        'retail building permit'
    ],

    OUTPUT: {
        directory: process.env.OUTPUT_DIR || './state-pages',
        githubRepo: process.env.GITHUB_REPO,
        githubToken: process.env.GITHUB_TOKEN,
        deployToGithub: true,
        // FIX #2: Was 5 - silently skipped pages with <5 leads. Now 1.
        minimumLeadsPerState: 1
    },

    RATE_LIMIT: {
        delayBetweenRequests: 500,
        delayBetweenStates: 2000
    }
};

// ==================== API KEY ROTATION ====================
class APIKeyManager {
    constructor() {
        this.usage = new Map();
        this.exhausted = new Set();
    }

    getNextKey() {
        const { keys, searchEngineIds } = CONFIG.API_KEYS.google;
        if (!keys.length) throw new Error('No Google API keys configured');

        // Find a non-exhausted key
        for (let attempt = 0; attempt < keys.length; attempt++) {
            const idx = CONFIG.API_KEYS.google.currentIndex % keys.length;
            CONFIG.API_KEYS.google.currentIndex++;
            const key = keys[idx];
            if (this.exhausted.has(key)) continue;
            const cx = searchEngineIds[idx % searchEngineIds.length];
            this.usage.set(key, (this.usage.get(key) || 0) + 1);
            return { key, searchEngineId: cx };
        }

        logger.error('ALL API KEYS EXHAUSTED for today');
        return null;
    }

    markExhausted(key) {
        this.exhausted.add(key);
        logger.warning(`Key ${key.substring(0, 12)}... quota exhausted`);
    }

    getStats() {
        return {
            totalCalls: Array.from(this.usage.values()).reduce((a, b) => a + b, 0),
            exhaustedKeys: this.exhausted.size,
            activeKeys: CONFIG.API_KEYS.google.keys.length - this.exhausted.size
        };
    }
}

const apiKeyManager = new APIKeyManager();

// ==================== LOGGER ====================
class Logger {
    constructor() { this.logs = []; }
    log(msg, level = 'info') {
        const entry = { timestamp: new Date().toISOString(), level, message: msg };
        this.logs.push(entry);
        console.log(`[${entry.timestamp}] [${level.toUpperCase()}] ${msg}`);
    }
    info(m) { this.log(m, 'info'); }
    success(m) { this.log(m, 'success'); }
    warning(m) { this.log(m, 'warning'); }
    error(m) { this.log(m, 'error'); }
    exportLogs() { return this.logs; }
}

const logger = new Logger();

// ==================== GOOGLE SEARCH ====================
async function searchGoogle(query, state) {
    const creds = apiKeyManager.getNextKey();
    if (!creds) return [];

    const { key, searchEngineId } = creds;

    try {
        const { default: fetch } = await import('node-fetch');
        const url = new URL(CONFIG.API_KEYS.google.endpoint);
        url.searchParams.set('key', key);
        url.searchParams.set('cx', searchEngineId);
        // FIX #4: Dynamic year instead of hardcoded 2026
        const year = new Date().getFullYear();
        url.searchParams.set('q', `${query} ${state} ${year}`);
        url.searchParams.set('num', '10');
        url.searchParams.set('dateRestrict', 'm6'); // Last 6 months only

        const response = await fetch(url.toString());
        const data = await response.json();

        if (data.error) {
            if (data.error.code === 429 || data.error.status === 'RESOURCE_EXHAUSTED') {
                apiKeyManager.markExhausted(key);
                return searchGoogle(query, state); // retry with next key
            }
            logger.error(`API error "${query}" in ${state}: [${data.error.code}] ${data.error.message}`);
            return [];
        }

        if (data.items && data.items.length > 0) {
            logger.info(`  ✓ "${query}" in ${state}: ${data.items.length} results`);
            return data.items.map(item => ({
                title: item.title || '',
                url: item.link || '',
                snippet: item.snippet || '',
                source: 'google'
            }));
        }

        logger.info(`  - "${query}" in ${state}: 0 results`);
        return [];

    } catch (error) {
        logger.error(`Network error: ${error.message}`);
        return [];
    }
}

// ==================== LEAD EXTRACTION ====================
// FIX #1: THE CORE BUG - Original code required "sign" or "signage" in the text.
// Business opening news never mentions signs. Removed that filter entirely.
// Now accepts any business opening/development result and marks it as a sign lead.
function extractLead(result, state) {
    const combined = `${result.title} ${result.snippet}`.toLowerCase();

    // Must be about a business opening or development
    const relevantTerms = [
        'opening', 'open', 'construction', 'development', 'retail',
        'restaurant', 'store', 'franchise', 'expansion', 'permit',
        'coming soon', 'grand opening', 'new location', 'plaza', 'center',
        'mall', 'build', 'tenant', 'lease'
    ];

    const isRelevant = relevantTerms.some(term => combined.includes(term));
    if (!isRelevant) return null;

    // Skip irrelevant results
    const skipTerms = ['obituary', 'sports score', 'weather forecast', 'stock market'];
    if (skipTerms.some(term => combined.includes(term))) return null;

    const phoneMatch = result.snippet.match(/(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/);
    const phone = phoneMatch ? phoneMatch[0] : 'Contact for details';

    const addressMatch = result.snippet.match(/\d+\s+[\w\s]+(Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Court|Ct|Way|Pkwy)/i);
    const location = addressMatch ? addressMatch[0] : state;

    const dateMatch = result.snippet.match(
        /(Q[1-4]\s+\d{4}|(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}|(?:Spring|Summer|Fall|Winter)\s+\d{4}|\d{4})/i
    );
    const opening = dateMatch ? dateMatch[0] : `${new Date().getFullYear()}`;

    const hotTerms = ['opening soon', 'coming soon', 'now hiring', 'breaks ground', 'grand opening', 'construction started'];
    const isHot = hotTerms.some(term => combined.includes(term));

    return {
        state,
        stateCode: CONFIG.STATE_CODES[state],
        name: cleanCompanyName(result.title),
        subtitle: result.snippet.substring(0, 100) + '...',
        location,
        contact: 'Development Team',
        phone,
        opening,
        temp: isHot ? 'hot' : 'warm',
        signage: estimateSignage(combined),
        revenue: estimateRevenue(combined),
        source: result.url,
        discovered: new Date().toISOString()
    };
}

function cleanCompanyName(title) {
    let name = title.replace(/\s*[-|]\s*.{0,50}$/, '').trim();
    name = name.replace(/\s+(announces|opens|opening|coming to|expands|plans|to open).*$/i, '').trim();
    if (name.length > 65) name = name.substring(0, 62) + '...';
    return name || 'New Commercial Development';
}

function estimateSignage(text) {
    const signage = ['Monument sign', 'Channel letters'];
    if (text.includes('restaurant') || text.includes('food') || text.includes('cafe') || text.includes('coffee')) {
        signage.push('Menu boards', 'Drive-thru signage');
    }
    if (text.includes('retail') || text.includes('store') || text.includes('shop')) {
        signage.push('Interior wayfinding', 'Window graphics');
    }
    if (text.includes('office') || text.includes('corporate') || text.includes('medical')) {
        signage.push('Directory signage', 'Suite identification');
    }
    if (text.includes('hotel') || text.includes('hospitality') || text.includes('inn')) {
        signage.push('Pylon sign', 'Illuminated facade');
    }
    if (text.includes('auto') || text.includes('car') || text.includes('dealership')) {
        signage.push('Pylon sign', 'Lot banners');
    }
    if (text.includes('gym') || text.includes('fitness') || text.includes('health')) {
        signage.push('Backlit letters', 'Parking signage');
    }
    return [...new Set(signage)];
}

// FIX #5: Revenue based on business type, not random numbers
function estimateRevenue(text) {
    if (text.includes('national chain') || text.includes('franchise') || text.includes('corporate')) {
        return '$45K - $90K';
    }
    if (text.includes('restaurant') || text.includes('hotel') || text.includes('shopping center')) {
        return '$30K - $65K';
    }
    if (text.includes('retail') || text.includes('medical') || text.includes('auto')) {
        return '$20K - $45K';
    }
    return '$15K - $35K';
}

// ==================== STATE SCRAPER ====================
async function scrapeState(state) {
    logger.info(`\n========== SCRAPING ${state.toUpperCase()} ==========`);
    const allResults = [];

    // Phase 1: General keyword searches
    logger.info('Phase 1: Keyword searches');
    for (const keyword of CONFIG.KEYWORDS) {
        if (apiKeyManager.getStats().activeKeys === 0) break;
        const results = await searchGoogle(keyword, state);
        allResults.push(...results);
        await delay(CONFIG.RATE_LIMIT.delayBetweenRequests);
    }

    // Phase 2: Franchise searches (only if quota remains)
    if (apiKeyManager.getStats().activeKeys > 0) {
        logger.info('Phase 2: Franchise searches');
        for (const chain of CONFIG.FRANCHISE_CHAINS) {
            if (apiKeyManager.getStats().activeKeys === 0) break;
            const results = await searchGoogle(`${chain} new location`, state);
            allResults.push(...results);
            await delay(CONFIG.RATE_LIMIT.delayBetweenRequests);
        }
    }

    // Deduplicate by URL
    const seen = new Set();
    const unique = allResults.filter(r => {
        if (!r.url || seen.has(r.url)) return false;
        seen.add(r.url);
        return true;
    });

    logger.info(`${unique.length} unique results for ${state}`);

    const leads = unique.map(r => extractLead(r, state)).filter(Boolean);
    logger.success(`${leads.length} leads extracted for ${state}`);
    return leads;
}

// ==================== HTML PAGE GENERATOR ====================
function generateHTML(state, leads) {
    const stateCode = CONFIG.STATE_CODES[state];
    const hotLeads = leads.filter(l => l.temp === 'hot').length;
    const warmLeads = leads.filter(l => l.temp === 'warm').length;
    const timestamp = new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });

    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${state.toUpperCase()} SIGN LEADS - ${leads.length} Active Opportunities</title>
    <meta name="description" content="${leads.length} sign industry leads in ${state}. Updated ${timestamp}.">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: linear-gradient(135deg, #0f3460 0%, #16213e 100%); padding: 10px; min-height: 100vh; }
        .container { max-width: 2000px; margin: 0 auto; background: white; border-radius: 15px; box-shadow: 0 10px 40px rgba(0,0,0,0.3); }
        header { background: linear-gradient(135deg, #1a1a2e, #16213e); color: white; padding: 30px; text-align: center; border-radius: 15px 15px 0 0; }
        header h1 { font-size: 2.5em; color: #00d4ff; margin-bottom: 10px; text-shadow: 0 0 20px rgba(0,212,255,0.5); }
        header p { color: #94a3b8; }
        .stats-bar { background: linear-gradient(135deg, #00d4ff, #0099ff); color: white; padding: 20px; display: flex; justify-content: space-around; flex-wrap: wrap; gap: 20px; }
        .stat-item { text-align: center; }
        .stat-number { font-size: 2.5em; font-weight: bold; display: block; }
        .filters { background: #f8fafc; padding: 20px; display: flex; gap: 15px; align-items: center; flex-wrap: wrap; border-bottom: 3px solid #00d4ff; }
        .filters select, .filters input { padding: 10px 15px; border: 2px solid #e2e8f0; border-radius: 8px; font-size: 0.95em; }
        .search-bar { flex: 1; min-width: 250px; }
        .leads-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(400px, 1fr)); gap: 25px; padding: 30px; background: #f8fafc; }
        .lead-card { background: white; border: 2px solid #e2e8f0; border-left: 6px solid #e2e8f0; border-radius: 12px; padding: 25px; transition: all 0.3s; }
        .lead-card:hover { transform: translateY(-8px); box-shadow: 0 15px 35px rgba(0,212,255,0.2); border-color: #00d4ff; }
        .lead-card.hot { border-left-color: #ef4444; }
        .lead-card.warm { border-left-color: #f59e0b; }
        .badge { display: inline-block; padding: 6px 14px; border-radius: 20px; font-size: 0.75em; font-weight: 800; text-transform: uppercase; margin-bottom: 12px; }
        .badge-hot { background: #ef4444; color: white; }
        .badge-warm { background: #f59e0b; color: white; }
        .lead-title { font-size: 1.2em; font-weight: 900; color: #1a1a2e; margin-bottom: 10px; line-height: 1.3; }
        .lead-detail { margin: 7px 0; color: #475569; font-size: 0.9em; }
        .signage-box { background: linear-gradient(135deg, #f0f9ff, #e0f2fe); border-left: 4px solid #00d4ff; padding: 12px 15px; margin: 15px 0; border-radius: 8px; }
        .signage-box h4 { color: #0099ff; margin-bottom: 6px; font-size: 0.85em; text-transform: uppercase; }
        .signage-box ul { list-style: none; }
        .signage-box li { color: #334155; font-size: 0.9em; padding: 2px 0; }
        .signage-box li::before { content: "✦ "; color: #00d4ff; }
        .revenue-box { background: #d1fae5; padding: 12px; border-radius: 8px; text-align: center; margin: 15px 0; font-weight: bold; color: #047857; }
        .contact-btn { width: 100%; padding: 14px; background: linear-gradient(135deg, #00d4ff, #0099ff); color: white; border: none; border-radius: 10px; font-weight: 800; cursor: pointer; transition: all 0.3s; text-decoration: none; display: block; text-align: center; }
        .contact-btn:hover { background: linear-gradient(135deg, #0099ff, #0066cc); transform: translateY(-2px); }
        .no-leads { text-align: center; padding: 60px; color: #94a3b8; font-size: 1.2em; grid-column: 1/-1; }
        footer { background: #1a1a2e; color: #94a3b8; text-align: center; padding: 20px; border-radius: 0 0 15px 15px; font-size: 0.85em; }
        @media (max-width: 768px) { .leads-grid { grid-template-columns: 1fr; } header h1 { font-size: 1.8em; } }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🎯 ${state.toUpperCase()} SIGN LEADS</h1>
            <p>Sign Industry Opportunities | Updated: ${timestamp}</p>
        </header>
        <div class="stats-bar">
            <div class="stat-item"><span class="stat-number">${leads.length}</span><span>Total Leads</span></div>
            <div class="stat-item"><span class="stat-number">${hotLeads}</span><span>🔥 Hot</span></div>
            <div class="stat-item"><span class="stat-number">${warmLeads}</span><span>⚡ Warm</span></div>
            <div class="stat-item"><span class="stat-number">${stateCode}</span><span>State</span></div>
        </div>
        <div class="filters">
            <input type="text" class="search-bar" id="searchBox" placeholder="Search by name or location..." oninput="filterLeads()">
            <select id="tempFilter" onchange="filterLeads()">
                <option value="all">All Temperatures</option>
                <option value="hot">🔥 Hot Only</option>
                <option value="warm">⚡ Warm Only</option>
            </select>
        </div>
        <div class="leads-grid" id="leadsContainer"></div>
        <footer>
            <p>${state} Sign Lead Database | ${leads.length} opportunities found | Data from public business announcements</p>
            <p style="margin-top:8px">Updated: ${timestamp} | Refreshes every 96 hours</p>
        </footer>
    </div>
    <script>
        const allLeads = ${JSON.stringify(leads)};
        function displayLeads(leads) {
            const c = document.getElementById('leadsContainer');
            if (!leads.length) { c.innerHTML = '<div class="no-leads">No leads match your filters.</div>'; return; }
            c.innerHTML = leads.map(l => \`
                <div class="lead-card \${l.temp}">
                    <span class="badge badge-\${l.temp}">\${l.temp === 'hot' ? '🔥 HOT LEAD' : '⚡ WARM LEAD'}</span>
                    <div class="lead-title">\${l.name}</div>
                    <div class="lead-detail">📍 \${l.location}</div>
                    <div class="lead-detail">📞 \${l.phone}</div>
                    <div class="lead-detail">📅 Opening: \${l.opening}</div>
                    <div class="signage-box">
                        <h4>Signage Opportunities</h4>
                        <ul>\${(l.signage||[]).map(s=>\`<li>\${s}</li>\`).join('')}</ul>
                    </div>
                    <div class="revenue-box">💰 Est. Revenue: \${l.revenue}</div>
                    <a class="contact-btn" href="\${l.source}" target="_blank" rel="noopener">🔗 VIEW LEAD SOURCE</a>
                </div>\`).join('');
        }
        function filterLeads() {
            const temp = document.getElementById('tempFilter').value;
            const search = document.getElementById('searchBox').value.toLowerCase();
            displayLeads(allLeads.filter(l =>
                (temp === 'all' || l.temp === temp) &&
                (search === '' || l.name.toLowerCase().includes(search) || l.location.toLowerCase().includes(search))
            ));
        }
        window.onload = () => displayLeads(allLeads);
    </script>
</body>
</html>`;
}

// ==================== GITHUB DEPLOYER ====================
async function deployToGitHub(state, htmlContent) {
    if (!CONFIG.OUTPUT.githubToken || !CONFIG.OUTPUT.githubRepo) {
        logger.warning('GitHub deployment not configured - skipping');
        return false;
    }
    try {
        const { default: fetch } = await import('node-fetch');
        const stateLower = state.toLowerCase().replace(/ /g, '-');
        const filePath = `state-pages/${stateLower}-sign-leads.html`;
        const apiBase = 'https://api.github.com';

        let sha = null;
        try {
            const getRes = await fetch(`${apiBase}/repos/${CONFIG.OUTPUT.githubRepo}/contents/${filePath}`, {
                headers: { 'Authorization': `token ${CONFIG.OUTPUT.githubToken}`, 'Accept': 'application/vnd.github.v3+json' }
            });
            if (getRes.ok) sha = (await getRes.json()).sha;
        } catch (e) { /* new file */ }

        const body = {
            message: `Auto-update ${state} leads - ${new Date().toISOString()}`,
            content: Buffer.from(htmlContent).toString('base64'),
            branch: 'main'
        };
        if (sha) body.sha = sha;

        const putRes = await fetch(`${apiBase}/repos/${CONFIG.OUTPUT.githubRepo}/contents/${filePath}`, {
            method: 'PUT',
            headers: {
                'Authorization': `token ${CONFIG.OUTPUT.githubToken}`,
                'Accept': 'application/vnd.github.v3+json',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(body)
        });

        if (putRes.ok) { logger.success(`Deployed ${state} to GitHub`); return true; }
        logger.error(`GitHub deploy failed for ${state}: ${await putRes.text()}`);
        return false;
    } catch (error) {
        logger.error(`GitHub error: ${error.message}`);
        return false;
    }
}

// ==================== MAIN ====================
async function runFullScrape() {
    logger.info(`\n🚀 STARTING 50-STATE SIGN LEAD SCRAPE`);
    logger.info(`Time: ${new Date().toISOString()}`);
    logger.info(`API Keys loaded: ${CONFIG.API_KEYS.google.keys.length}`);
    logger.info(`Search Engine IDs loaded: ${CONFIG.API_KEYS.google.searchEngineIds.length}`);

    if (CONFIG.API_KEYS.google.keys.length === 0) {
        logger.error('FATAL: No API keys configured! Set GOOGLE_API_KEYS env var.');
        process.exit(1);
    }

    const startTime = Date.now();
    const results = {};
    let totalLeads = 0;

    await fs.mkdir(CONFIG.OUTPUT.directory, { recursive: true });

    for (const state of CONFIG.US_STATES) {
        if (apiKeyManager.getStats().activeKeys === 0) {
            logger.warning('All API keys exhausted - stopping early');
            break;
        }

        try {
            const leads = await scrapeState(state);
            results[state] = leads;
            totalLeads += leads.length;

            if (leads.length >= CONFIG.OUTPUT.minimumLeadsPerState) {
                const html = generateHTML(state, leads);
                const stateLower = state.toLowerCase().replace(/ /g, '-');
                const filePath = path.join(CONFIG.OUTPUT.directory, `${stateLower}-sign-leads.html`);
                await fs.writeFile(filePath, html);
                logger.success(`Saved: ${filePath} (${leads.length} leads)`);

                if (CONFIG.OUTPUT.deployToGithub) {
                    await deployToGitHub(state, html);
                }
            } else {
                logger.warning(`${state}: ${leads.length} leads - below minimum, no page generated`);
            }
        } catch (error) {
            logger.error(`Error on ${state}: ${error.message}`);
            results[state] = [];
        }

        await delay(CONFIG.RATE_LIMIT.delayBetweenStates);
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    const keyStats = apiKeyManager.getStats();

    logger.success(`\n✅ SCRAPE COMPLETE`);
    logger.info(`Total leads: ${totalLeads}`);
    logger.info(`States with pages: ${Object.values(results).filter(l => l.length >= CONFIG.OUTPUT.minimumLeadsPerState).length}`);
    logger.info(`Duration: ${duration}s`);
    logger.info(`API calls: ${keyStats.totalCalls} | Keys exhausted: ${keyStats.exhaustedKeys}/${CONFIG.API_KEYS.google.keys.length}`);

    const summary = {
        timestamp: new Date().toISOString(),
        totalLeads,
        statesProcessed: Object.keys(results).length,
        durationSeconds: duration,
        nextRun: new Date(Date.now() + 96 * 60 * 60 * 1000).toISOString(),
        apiKeyStats: keyStats,
        stateBreakdown: Object.entries(results).map(([state, leads]) => ({
            state,
            stateCode: CONFIG.STATE_CODES[state],
            leadCount: leads.length,
            hotLeads: leads.filter(l => l.temp === 'hot').length,
            warmLeads: leads.filter(l => l.temp === 'warm').length
        })),
        logs: logger.exportLogs()
    };

    await fs.writeFile(path.join(CONFIG.OUTPUT.directory, 'scrape-summary.json'), JSON.stringify(summary, null, 2));
    await fs.writeFile('./next-run.json', JSON.stringify({ lastRun: new Date().toISOString(), nextRun: summary.nextRun }, null, 2));
    logger.success('Summary and next-run files saved.');

    return results;
}

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

// ==================== ENTRY POINTS ====================
exports.handler = async (event, context) => {
    try {
        const results = await runFullScrape();
        const total = Object.values(results).reduce((sum, leads) => sum + leads.length, 0);
        return { statusCode: 200, body: JSON.stringify({ success: true, totalLeads: total }) };
    } catch (error) {
        return { statusCode: 500, body: JSON.stringify({ success: false, error: error.message }) };
    }
};

if (require.main === module) {
    runFullScrape()
        .then(() => process.exit(0))
        .catch(err => { logger.error(err.message); process.exit(1); });
}

module.exports = { runFullScrape, CONFIG };
