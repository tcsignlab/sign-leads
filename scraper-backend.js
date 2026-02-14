// =====================================================
// SIGN LEAD SCRAPER — DuckDuckGo Edition
// No API keys. No quota. No billing. Completely free.
// =====================================================

const fs   = require('fs').promises;
const path = require('path');
const https = require('https');

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
        'Alabama':'AL','Alaska':'AK','Arizona':'AZ','Arkansas':'AR','California':'CA',
        'Colorado':'CO','Connecticut':'CT','Delaware':'DE','Florida':'FL','Georgia':'GA',
        'Hawaii':'HI','Idaho':'ID','Illinois':'IL','Indiana':'IN','Iowa':'IA',
        'Kansas':'KS','Kentucky':'KY','Louisiana':'LA','Maine':'ME','Maryland':'MD',
        'Massachusetts':'MA','Michigan':'MI','Minnesota':'MN','Mississippi':'MS',
        'Missouri':'MO','Montana':'MT','Nebraska':'NE','Nevada':'NV',
        'New Hampshire':'NH','New Jersey':'NJ','New Mexico':'NM','New York':'NY',
        'North Carolina':'NC','North Dakota':'ND','Ohio':'OH','Oklahoma':'OK',
        'Oregon':'OR','Pennsylvania':'PA','Rhode Island':'RI','South Carolina':'SC',
        'South Dakota':'SD','Tennessee':'TN','Texas':'TX','Utah':'UT',
        'Vermont':'VT','Virginia':'VA','Washington':'WA','West Virginia':'WV',
        'Wisconsin':'WI','Wyoming':'WY'
    },

    // Search queries — one per request, short and broad
    KEYWORDS: [
        'new restaurant opening',
        'new store opening',
        'grand opening',
        'new franchise opening',
        'retail store opening',
        'commercial construction',
        'new shopping center',
        'new business opening',
        'strip mall development',
        'new franchise location'
    ],

    // Franchise chains to specifically track
    FRANCHISE_CHAINS: [
        "Chick-fil-A opening",
        "Dutch Bros opening",
        "Raising Cane's opening",
        "Wingstop opening",
        "Jersey Mike's opening",
        "Crumbl Cookies opening",
        "Dollar General opening",
        "Buc-ee's opening"
    ],

    RATE_LIMIT: {
        delayBetweenRequests: 1500,  // ms — be respectful to DDG
        delayBetweenStates:   3000,  // ms between states
        maxRetries: 2
    },

    OUTPUT: {
        directory:          process.env.OUTPUT_DIR || './state-pages',
        githubRepo:         process.env.GITHUB_REPO,
        githubToken:        process.env.GITHUB_TOKEN,
        deployToGithub:     true,
        minimumLeadsPerState: 1
    }
};

// ==================== LOGGER ====================
class Logger {
    constructor() { this.logs = []; }
    log(msg, level = 'info') {
        const ts = new Date().toISOString();
        this.logs.push({ timestamp: ts, level, message: msg });
        console.log(`[${ts}] [${level.toUpperCase()}] ${msg}`);
    }
    info(m)    { this.log(m, 'info');    }
    success(m) { this.log(m, 'success'); }
    warning(m) { this.log(m, 'warning'); }
    error(m)   { this.log(m, 'error');   }
    exportLogs() { return this.logs; }
}

const logger = new Logger();

// ==================== DUCKDUCKGO SEARCH ====================
// Scrapes html.duckduckgo.com — static HTML, no JS required, no API key needed

function httpGet(url, headers = {}) {
    return new Promise((resolve, reject) => {
        const defaultHeaders = {
            'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept':          'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'identity',
            'Connection':      'keep-alive',
            ...headers
        };

        const urlObj = new URL(url);
        const options = {
            hostname: urlObj.hostname,
            path:     urlObj.pathname + urlObj.search,
            method:   'GET',
            headers:  defaultHeaders,
            timeout:  15000
        };

        const req = https.request(options, res => {
            // Follow redirects
            if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
                return httpGet(res.headers.location, headers).then(resolve).catch(reject);
            }

            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => resolve({ status: res.statusCode, body: data }));
        });

        req.on('error',   reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('Request timeout')); });
        req.end();
    });
}

function parseDDGResults(html) {
    const results = [];

    // DDG HTML result structure:
    // <div class="result"> ... <a class="result__a" href="URL">TITLE</a> ... <a class="result__snippet">SNIPPET</a>
    // Extract all result blocks
    const resultBlocks = html.split('<div class="result ');

    for (const block of resultBlocks.slice(1)) {
        try {
            // Extract URL
            const urlMatch = block.match(/class="result__a"[^>]*href="([^"]+)"/);
            if (!urlMatch) continue;
            let url = urlMatch[1];
            // DDG sometimes uses redirect URLs — extract the actual URL
            if (url.includes('//duckduckgo.com/l/?')) {
                const uddg = url.match(/uddg=([^&]+)/);
                if (uddg) url = decodeURIComponent(uddg[1]);
            }

            // Extract title
            const titleMatch = block.match(/class="result__a"[^>]*>([^<]+)<\/a>/);
            const title = titleMatch ? titleMatch[1].replace(/&#x27;/g,"'").replace(/&amp;/g,'&').replace(/&quot;/g,'"').trim() : '';

            // Extract snippet
            const snippetMatch = block.match(/class="result__snippet"[^>]*>([^<]*(?:<[^>]+>[^<]*)*)<\/a>/);
            let snippet = '';
            if (snippetMatch) {
                snippet = snippetMatch[1]
                    .replace(/<[^>]+>/g, ' ')
                    .replace(/&#x27;/g,"'").replace(/&amp;/g,'&').replace(/&quot;/g,'"')
                    .replace(/\s+/g, ' ').trim();
            }

            if (title && url && url.startsWith('http')) {
                results.push({ title, url, snippet });
            }
        } catch (e) {
            // Skip malformed blocks
        }
    }

    return results;
}

async function searchDDG(query, state, retries = 0) {
    const fullQuery = `${query} ${state}`;
    const encoded   = encodeURIComponent(fullQuery);
    const url       = `https://html.duckduckgo.com/html/?q=${encoded}&kl=us-en`;

    try {
        const { status, body } = await httpGet(url);

        if (status === 403 || status === 429) {
            logger.warning(`DDG rate limit hit for "${query}" in ${state} — waiting 10s`);
            await delay(10000);
            if (retries < CONFIG.RATE_LIMIT.maxRetries) {
                return searchDDG(query, state, retries + 1);
            }
            return [];
        }

        if (status !== 200) {
            logger.warning(`DDG returned ${status} for "${query}" in ${state}`);
            return [];
        }

        // Check for "no results" page
        if (body.includes('No results.') || body.includes('no-results')) {
            logger.info(`  - "${query}" in ${state}: no results`);
            return [];
        }

        const results = parseDDGResults(body);
        logger.info(`  ✓ "${query}" in ${state}: ${results.length} results`);
        return results;

    } catch (err) {
        if (retries < CONFIG.RATE_LIMIT.maxRetries) {
            logger.warning(`Retrying "${query}" in ${state}: ${err.message}`);
            await delay(3000);
            return searchDDG(query, state, retries + 1);
        }
        logger.error(`DDG search failed for "${query}" in ${state}: ${err.message}`);
        return [];
    }
}

// ==================== LEAD EXTRACTION ====================
function extractLead(result, state) {
    if (!result.title || !result.url) return null;

    const combined = `${result.title} ${result.snippet}`.toLowerCase();

    // Must be about a business opening or development
    const relevantTerms = [
        'opening', 'opens', 'grand opening', 'coming soon', 'open soon',
        'new location', 'new store', 'new restaurant', 'new franchise',
        'construction', 'development', 'retail', 'restaurant', 'store',
        'franchise', 'expansion', 'permit', 'plaza', 'shopping center',
        'strip mall', 'commercial', 'build', 'tenant', 'lease', 'relocat',
        'now open', 'soft open', 'ribbon cutting', 'breaks ground'
    ];
    if (!relevantTerms.some(t => combined.includes(t))) return null;

    // Skip junk
    const skipTerms = ['obituary', 'police', 'arrested', 'murder', 'crime', 'lawsuit',
        'stock price', 'earnings report', 'weather', 'sports score'];
    if (skipTerms.some(t => combined.includes(t))) return null;

    // Skip non-US results
    try {
        const hostname = new URL(result.url).hostname;
        // Skip obviously foreign domains
        if (hostname.endsWith('.co.uk') || hostname.endsWith('.ca') ||
            hostname.endsWith('.com.au') || hostname.endsWith('.co.nz')) return null;
    } catch(e) { return null; }

    // Extract fields
    const phoneMatch   = result.snippet.match(/(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/);
    const phone        = phoneMatch ? phoneMatch[0] : 'Contact for details';

    const addressMatch = result.snippet.match(
        /\d+\s+[\w\s]+(Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Court|Ct|Way|Pkwy|Highway|Hwy)\b/i
    );
    const location = addressMatch ? addressMatch[0] : state;

    const dateMatch = result.snippet.match(
        /(Q[1-4]\s+\d{4}|(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}|(?:Spring|Summer|Fall|Winter)\s+\d{4}|\b202[4-9]\b)/i
    );
    const opening = dateMatch ? dateMatch[0] : 'Coming soon';

    const hotTerms = ['grand opening', 'now open', 'opening soon', 'coming soon',
        'soft open', 'ribbon cutting', 'breaks ground', 'now hiring'];
    const isHot = hotTerms.some(t => combined.includes(t));

    return {
        state,
        stateCode:  CONFIG.STATE_CODES[state],
        name:       cleanTitle(result.title),
        subtitle:   result.snippet.substring(0, 120),
        location,
        contact:    'Development Team',
        phone,
        opening,
        temp:       isHot ? 'hot' : 'warm',
        signage:    estimateSignage(combined),
        revenue:    estimateRevenue(combined),
        source:     result.url,
        discovered: new Date().toISOString()
    };
}

function cleanTitle(title) {
    let name = title.replace(/\s*[-|—]\s*.{0,60}$/, '').trim();
    name = name.replace(/\s+(announces?|opens?|opening|coming to|expands?|to open|will open).*$/i, '').trim();
    if (name.length > 70) name = name.substring(0, 67) + '...';
    return name || 'New Commercial Development';
}

function estimateSignage(text) {
    const signs = new Set(['Monument sign', 'Channel letters']);
    if (text.match(/restaurant|cafe|coffee|food|dining|bar|grill|pizza|burger|taco|sushi/))
        { signs.add('Menu boards'); signs.add('Drive-thru signage'); }
    if (text.match(/retail|store|shop|boutique|outlet|clothing|apparel/))
        { signs.add('Interior wayfinding'); signs.add('Window graphics'); }
    if (text.match(/office|corporate|medical|clinic|dental|urgent care|health/))
        { signs.add('Directory signage'); signs.add('Suite identification'); }
    if (text.match(/hotel|motel|inn|suites|hospitality|lodge/))
        { signs.add('Pylon sign'); signs.add('Illuminated facade'); }
    if (text.match(/auto|car|dealer|vehicle|tire|oil change|mechanic/))
        { signs.add('Pylon sign'); signs.add('Lot banners'); }
    if (text.match(/gym|fitness|crossfit|yoga|wellness|planet fitness/))
        { signs.add('Backlit letters'); signs.add('Parking signage'); }
    if (text.match(/bank|credit union|financial|insurance/))
        { signs.add('Illuminated sign cabinet'); }
    if (text.match(/gas|fuel|station|convenience|7-eleven|circle k/))
        { signs.add('Canopy signage'); signs.add('Price sign'); }
    return [...signs];
}

function estimateRevenue(text) {
    if (text.match(/national chain|franchise|corporate|regional chain/)) return '$45K – $90K';
    if (text.match(/restaurant|hotel|shopping center|mall/))             return '$30K – $65K';
    if (text.match(/retail|medical|auto|bank|office/))                   return '$20K – $45K';
    return '$12K – $30K';
}

// ==================== STATE SCRAPER ====================
async function scrapeState(state) {
    logger.info(`\n===== ${state.toUpperCase()} =====`);
    const allResults = [];

    // Phase 1: Keyword searches
    for (const keyword of CONFIG.KEYWORDS) {
        const results = await searchDDG(keyword, state);
        allResults.push(...results);
        await delay(CONFIG.RATE_LIMIT.delayBetweenRequests);
    }

    // Phase 2: Franchise searches
    for (const chain of CONFIG.FRANCHISE_CHAINS) {
        const results = await searchDDG(chain, state);
        allResults.push(...results);
        await delay(CONFIG.RATE_LIMIT.delayBetweenRequests);
    }

    // Deduplicate by URL
    const seen   = new Set();
    const unique = allResults.filter(r => {
        if (!r.url || seen.has(r.url)) return false;
        seen.add(r.url);
        return true;
    });

    logger.info(`${unique.length} unique results → extracting leads...`);

    const leads = unique.map(r => extractLead(r, state)).filter(Boolean);
    logger.success(`${leads.length} leads found in ${state}`);
    return leads;
}

// ==================== HTML GENERATOR ====================
function generateHTML(state, leads) {
    const stateCode = CONFIG.STATE_CODES[state];
    const hotLeads  = leads.filter(l => l.temp === 'hot').length;
    const warmLeads = leads.filter(l => l.temp === 'warm').length;
    const timestamp = new Date().toLocaleDateString('en-US', { month:'long', day:'numeric', year:'numeric' });

    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${state.toUpperCase()} SIGN LEADS — ${leads.length} Active Opportunities</title>
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:'Segoe UI',Tahoma,Geneva,Verdana,sans-serif;background:linear-gradient(135deg,#0f3460,#16213e);padding:10px;min-height:100vh}
        .container{max-width:2000px;margin:0 auto;background:white;border-radius:15px;box-shadow:0 10px 40px rgba(0,0,0,.3)}
        header{background:linear-gradient(135deg,#1a1a2e,#16213e);color:white;padding:30px;text-align:center;border-radius:15px 15px 0 0}
        header h1{font-size:2.5em;color:#00d4ff;margin-bottom:8px;text-shadow:0 0 20px rgba(0,212,255,.5)}
        header p{color:#94a3b8;font-size:.9em}
        .back-link{display:inline-block;margin-top:12px;color:#00d4ff;font-size:.8em;text-decoration:none;opacity:.8}
        .back-link:hover{opacity:1}
        .stats-bar{background:linear-gradient(135deg,#00d4ff,#0099ff);color:white;padding:20px;display:flex;justify-content:space-around;flex-wrap:wrap;gap:20px}
        .stat-item{text-align:center}
        .stat-number{font-size:2.5em;font-weight:bold;display:block}
        .filters{background:#f8fafc;padding:20px;display:flex;gap:15px;align-items:center;flex-wrap:wrap;border-bottom:3px solid #00d4ff}
        .filters select,.filters input{padding:10px 15px;border:2px solid #e2e8f0;border-radius:8px;font-size:.95em}
        .search-bar{flex:1;min-width:250px}
        .leads-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(400px,1fr));gap:25px;padding:30px;background:#f8fafc;min-height:200px}
        .lead-card{background:white;border:2px solid #e2e8f0;border-left:6px solid #e2e8f0;border-radius:12px;padding:25px;transition:all .3s}
        .lead-card:hover{transform:translateY(-8px);box-shadow:0 15px 35px rgba(0,212,255,.2);border-color:#00d4ff}
        .lead-card.hot{border-left-color:#ef4444}
        .lead-card.warm{border-left-color:#f59e0b}
        .badge{display:inline-block;padding:6px 14px;border-radius:20px;font-size:.75em;font-weight:800;text-transform:uppercase;margin-bottom:12px}
        .badge-hot{background:#ef4444;color:white}
        .badge-warm{background:#f59e0b;color:white}
        .lead-title{font-size:1.2em;font-weight:900;color:#1a1a2e;margin-bottom:10px;line-height:1.3}
        .lead-detail{margin:7px 0;color:#475569;font-size:.9em}
        .signage-box{background:linear-gradient(135deg,#f0f9ff,#e0f2fe);border-left:4px solid #00d4ff;padding:12px 15px;margin:15px 0;border-radius:8px}
        .signage-box h4{color:#0099ff;margin-bottom:6px;font-size:.85em;text-transform:uppercase}
        .signage-box li{color:#334155;font-size:.9em;padding:2px 0;list-style:none}
        .signage-box li::before{content:"✦ ";color:#00d4ff}
        .revenue-box{background:#d1fae5;padding:12px;border-radius:8px;text-align:center;margin:15px 0;font-weight:bold;color:#047857}
        .contact-btn{width:100%;padding:14px;background:linear-gradient(135deg,#00d4ff,#0099ff);color:white;border:none;border-radius:10px;font-weight:800;cursor:pointer;text-decoration:none;display:block;text-align:center;transition:all .3s}
        .contact-btn:hover{background:linear-gradient(135deg,#0099ff,#0066cc);transform:translateY(-2px)}
        .no-leads{text-align:center;padding:60px;color:#94a3b8;font-size:1.1em;grid-column:1/-1}
        footer{background:#1a1a2e;color:#94a3b8;text-align:center;padding:20px;border-radius:0 0 15px 15px;font-size:.85em}
        @media(max-width:768px){.leads-grid{grid-template-columns:1fr}header h1{font-size:1.8em}}
    </style>
</head>
<body>
<div class="container">
    <header>
        <h1>🎯 ${state.toUpperCase()} SIGN LEADS</h1>
        <p>Sign Industry Opportunities | Updated: ${timestamp}</p>
        <a class="back-link" href="../index.html">← Back to National Map</a>
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
        <p>${state} Sign Lead Database | ${leads.length} opportunities | Sourced via DuckDuckGo</p>
        <p style="margin-top:8px">Updated: ${timestamp} | Auto-refreshes every 96 hours</p>
    </footer>
</div>
<script>
    const allLeads=${JSON.stringify(leads)};
    function displayLeads(leads){
        const c=document.getElementById('leadsContainer');
        if(!leads.length){c.innerHTML='<div class="no-leads">No leads match your filters.</div>';return}
        c.innerHTML=leads.map(l=>\`
            <div class="lead-card \${l.temp}">
                <span class="badge badge-\${l.temp}">\${l.temp==='hot'?'🔥 HOT LEAD':'⚡ WARM LEAD'}</span>
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
    function filterLeads(){
        const temp=document.getElementById('tempFilter').value;
        const search=document.getElementById('searchBox').value.toLowerCase();
        displayLeads(allLeads.filter(l=>
            (temp==='all'||l.temp===temp)&&
            (search===''||l.name.toLowerCase().includes(search)||l.location.toLowerCase().includes(search))
        ));
    }
    window.onload=()=>displayLeads(allLeads);
</script>
</body>
</html>`;
}

// ==================== GITHUB DEPLOYER ====================
async function deployToGitHub(state, htmlContent) {
    if (!CONFIG.OUTPUT.githubToken || !CONFIG.OUTPUT.githubRepo) return false;
    try {
        const { default: fetch } = await import('node-fetch');
        const stateLower = state.toLowerCase().replace(/ /g, '-');
        const filePath   = `state-pages/${stateLower}-sign-leads.html`;
        const apiBase    = 'https://api.github.com';

        let sha = null;
        try {
            const r = await fetch(`${apiBase}/repos/${CONFIG.OUTPUT.githubRepo}/contents/${filePath}`, {
                headers: { 'Authorization': `token ${CONFIG.OUTPUT.githubToken}`, 'Accept': 'application/vnd.github.v3+json' }
            });
            if (r.ok) sha = (await r.json()).sha;
        } catch (e) { /* new file */ }

        const body = {
            message: `Auto-update ${state} leads — ${new Date().toISOString()}`,
            content: Buffer.from(htmlContent).toString('base64'),
            branch:  'main'
        };
        if (sha) body.sha = sha;

        const put = await fetch(`${apiBase}/repos/${CONFIG.OUTPUT.githubRepo}/contents/${filePath}`, {
            method:  'PUT',
            headers: {
                'Authorization': `token ${CONFIG.OUTPUT.githubToken}`,
                'Accept':        'application/vnd.github.v3+json',
                'Content-Type':  'application/json'
            },
            body: JSON.stringify(body)
        });

        if (put.ok) { logger.success(`GitHub: deployed ${state}`); return true; }
        logger.error(`GitHub deploy failed for ${state}: ${await put.text()}`);
        return false;
    } catch (err) {
        logger.error(`GitHub error: ${err.message}`);
        return false;
    }
}

// ==================== MAIN ====================
async function runFullScrape() {
    logger.info('\n🚀 SIGN LEAD SCRAPER STARTING — DuckDuckGo Edition');
    logger.info(`Time:   ${new Date().toISOString()}`);
    logger.info(`States: ${CONFIG.US_STATES.length}`);
    logger.info(`Queries per state: ${CONFIG.KEYWORDS.length + CONFIG.FRANCHISE_CHAINS.length}`);
    logger.info('No API keys required — using DuckDuckGo HTML search');

    await fs.mkdir(CONFIG.OUTPUT.directory, { recursive: true });

    const startTime = Date.now();
    const results   = {};
    let totalLeads  = 0;

    for (const state of CONFIG.US_STATES) {
        try {
            const leads = await scrapeState(state);
            results[state] = leads;
            totalLeads += leads.length;

            if (leads.length >= CONFIG.OUTPUT.minimumLeadsPerState) {
                const html       = generateHTML(state, leads);
                const stateLower = state.toLowerCase().replace(/ /g, '-');
                const filePath   = path.join(CONFIG.OUTPUT.directory, `${stateLower}-sign-leads.html`);

                await fs.writeFile(filePath, html);
                logger.success(`Saved: ${filePath} (${leads.length} leads)`);

                if (CONFIG.OUTPUT.deployToGithub) {
                    await deployToGitHub(state, html);
                    await delay(500);
                }
            } else {
                logger.warning(`${state}: ${leads.length} leads — below minimum, skipping page`);
            }

        } catch (err) {
            logger.error(`Error on ${state}: ${err.message}`);
            results[state] = [];
        }

        await delay(CONFIG.RATE_LIMIT.delayBetweenStates);
    }

    const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(1);

    logger.success('\n✅ SCRAPE COMPLETE');
    logger.info(`Total leads:    ${totalLeads}`);
    logger.info(`States scraped: ${Object.keys(results).length}`);
    logger.info(`Duration:       ${duration} minutes`);

    // Save summary
    const summary = {
        timestamp:       new Date().toISOString(),
        totalLeads,
        statesProcessed: Object.keys(results).length,
        durationMinutes: duration,
        nextRun:         new Date(Date.now() + 96 * 60 * 60 * 1000).toISOString(),
        searchEngine:    'DuckDuckGo (free, no API key)',
        stateBreakdown:  Object.entries(results).map(([state, leads]) => ({
            state,
            stateCode:  CONFIG.STATE_CODES[state],
            leadCount:  leads.length,
            hotLeads:   leads.filter(l => l.temp === 'hot').length,
            warmLeads:  leads.filter(l => l.temp === 'warm').length
        })),
        logs: logger.exportLogs()
    };

    await fs.writeFile(path.join(CONFIG.OUTPUT.directory, 'scrape-summary.json'), JSON.stringify(summary, null, 2));
    await fs.writeFile('./next-run.json', JSON.stringify({ lastRun: new Date().toISOString(), nextRun: summary.nextRun }, null, 2));
    logger.success('Summary saved.');
    return results;
}

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

// ==================== ENTRY POINTS ====================
exports.handler = async () => {
    try {
        const results = await runFullScrape();
        const total   = Object.values(results).reduce((s, l) => s + l.length, 0);
        return { statusCode: 200, body: JSON.stringify({ success: true, totalLeads: total }) };
    } catch (err) {
        return { statusCode: 500, body: JSON.stringify({ success: false, error: err.message }) };
    }
};

if (require.main === module) {
    runFullScrape()
        .then(() => process.exit(0))
        .catch(err => { logger.error(err.message); process.exit(1); });
}

module.exports = { runFullScrape, CONFIG };
