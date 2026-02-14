// =====================================================
// SIGN LEAD SCRAPER v4 — Bing Search Edition
// Free, no API keys, works on GitHub Actions IPs,
// no bot detection issues like DDG 202 errors
// =====================================================

const fs    = require('fs').promises;
const path  = require('path');
const https = require('https');
const http  = require('http');

// ==================== CONFIGURATION ====================
const CONFIG = {
    US_STATES: [
        'Alabama','Alaska','Arizona','Arkansas','California','Colorado','Connecticut',
        'Delaware','Florida','Georgia','Hawaii','Idaho','Illinois','Indiana','Iowa',
        'Kansas','Kentucky','Louisiana','Maine','Maryland','Massachusetts','Michigan',
        'Minnesota','Mississippi','Missouri','Montana','Nebraska','Nevada',
        'New Hampshire','New Jersey','New Mexico','New York','North Carolina',
        'North Dakota','Ohio','Oklahoma','Oregon','Pennsylvania','Rhode Island',
        'South Carolina','South Dakota','Tennessee','Texas','Utah','Vermont',
        'Virginia','Washington','West Virginia','Wisconsin','Wyoming'
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

    // ── HIGH-SIGNAL SEARCH QUERIES ─────────────────────────────
    // Matches real news headlines. Short, no year, broad.
    KEYWORDS: [
        '"grand opening"',
        '"now open"',
        '"opening soon"',
        '"coming soon" restaurant OR retail',
        'new restaurant opening',
        'new store opening',
        'commercial construction permit',
        'new retail development',
        'shopping center opening',
        'new franchise opening',
        'fast food restaurant opening',
        'new drive-thru opening',
    ],

    // High-value franchise chains
    FRANCHISE_CHAINS: [
        'Chick-fil-A new location',
        'Dutch Bros Coffee opening',
        "Raising Cane's opening",
        'Wingstop new location',
        "Jersey Mike's opening",
        'Crumbl Cookies opening',
        'Whataburger opening',
        'Five Guys opening',
        'Chipotle new location',
        'Starbucks new location',
        'Panera Bread opening',
        "McDonald's new location",
        "Chili's new location",
        'Dollar General new store',
        'Aldi new store opening',
        'Costco new location',
    ],

    RATE_LIMIT: {
        minDelay:   1200,
        maxDelay:   2500,
        stateDelay: 2000,
        maxRetries: 2
    },

    OUTPUT: {
        directory:            process.env.OUTPUT_DIR || './state-pages',
        githubRepo:           process.env.GITHUB_REPO,
        githubToken:          process.env.GITHUB_TOKEN,
        deployToGithub:       true,
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

// ==================== HTTP HELPER ====================
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
];
let uaIndex = 0;
function getUA() { return USER_AGENTS[uaIndex++ % USER_AGENTS.length]; }

function httpGet(url) {
    return new Promise((resolve, reject) => {
        const mod    = url.startsWith('https') ? https : http;
        const urlObj = new URL(url);

        const options = {
            hostname: urlObj.hostname,
            path:     urlObj.pathname + urlObj.search,
            method:   'GET',
            headers: {
                'User-Agent':                getUA(),
                'Accept':                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language':           'en-US,en;q=0.9',
                'Accept-Encoding':           'identity',
                'Cache-Control':             'no-cache',
                'Pragma':                    'no-cache',
                'sec-ch-ua':                 '"Not_A Brand";v="8", "Chromium";v="121"',
                'sec-ch-ua-mobile':          '?0',
                'sec-ch-ua-platform':        '"Windows"',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest':            'document',
                'Sec-Fetch-Mode':            'navigate',
                'Sec-Fetch-Site':            'none',
                'Sec-Fetch-User':            '?1',
            },
            timeout: 20000
        };

        const req = mod.request(options, res => {
            if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
                return httpGet(res.headers.location).then(resolve).catch(reject);
            }
            let data = '';
            res.on('data', c => data += c);
            res.on('end', () => resolve({ status: res.statusCode, body: data }));
        });

        req.on('error',   reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
        req.end();
    });
}

// ==================== BING SEARCH + PARSER ====================
function decodeEntities(text) {
    return text
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'")
        .replace(/&nbsp;/g, ' ');
}

function parseBingResults(html) {
    const results = [];
    const blocks  = html.split('<li class="b_algo"');

    // Blacklist of domains to exclude
    const blacklist = [
        'dictionary.com', 'merriam-webster.com', 'wikipedia.org', 'wiktionary.org',
        'urbandictionary.com', 'thesaurus.com', 'definitions.net', 'yourdictionary.com',
        'reddit.com', 'facebook.com', 'twitter.com', 'pinterest.com', 'instagram.com',
        'youtube.com', 'tiktok.com', 'linkedin.com/posts', 'amazon.com', 'ebay.com',
        'yelp.com/search', 'tripadvisor.com', 'booking.com', 'expedia.com'
    ];

    for (const block of blocks.slice(1)) {
        try {
            const linkMatch = block.match(/<h2[^>]*>.*?<a[^>]+href="([^"]+)"[^>]*>(.*?)<\/a>/s);
            if (!linkMatch) continue;

            let url   = linkMatch[1];
            let title = linkMatch[2].replace(/<[^>]+>/g, '').trim();
            title     = decodeEntities(title);

            // Skip blacklisted domains
            const urlLower = url.toLowerCase();
            if (blacklist.some(domain => urlLower.includes(domain))) {
                continue;
            }

            // Skip if title looks like a definition
            if (/definition|meaning|what does|what is|define|synonym/i.test(title)) {
                continue;
            }

            // Extract snippet
            const snippetMatch = block.match(/<p[^>]*>(.*?)<\/p>/s);
            let snippet = '';
            if (snippetMatch) {
                snippet = snippetMatch[1].replace(/<[^>]+>/g, '').trim();
                snippet = decodeEntities(snippet);
            }

            // Must have both title and snippet
            if (!snippet || snippet.length < 20) continue;

            results.push({ title, url, snippet });
        } catch(e) {
            // Skip malformed results
        }
    }
    return results;
}

async function searchBing(query, retries = 0) {
    try {
        const url = `https://www.bing.com/search?q=${encodeURIComponent(query)}&count=20`;
        logger.info(`  Bing: ${query}`);
        
        const res = await httpGet(url);
        
        if (res.status === 200) {
            const results = parseBingResults(res.body);
            logger.info(`  Found: ${results.length} results`);
            return results;
        }
        
        if (retries < CONFIG.RATE_LIMIT.maxRetries) {
            await delay(3000);
            return searchBing(query, retries + 1);
        }
        
        logger.warning(`  Bing failed: HTTP ${res.status}`);
        return [];
    } catch(err) {
        if (retries < CONFIG.RATE_LIMIT.maxRetries) {
            await delay(3000);
            return searchBing(query, retries + 1);
        }
        logger.error(`  Search error: ${err.message}`);
        return [];
    }
}

// ==================== LEAD ENRICHMENT ====================
function enrichLead(result, state, queryType) {
    const text = (result.title + ' ' + result.snippet).toLowerCase();
    
    // QUALITY FILTERS - Skip if result doesn't look like a real business lead
    const hasBusinessIndicators = 
        /restaurant|store|retail|shop|cafe|coffee|franchise|business|location|construction|development|opening|permit/i.test(text);
    
    const hasLocationIndicators = 
        /street|avenue|road|blvd|boulevard|plaza|center|mall|district|downtown|city|county/i.test(text);
    
    // Must have at least business context
    if (!hasBusinessIndicators) {
        return null; // Skip this result
    }
    
    // Extract business name - more careful extraction
    let name = result.title.split(/[-–—|]/)[0].trim();
    name = name.replace(/\s+(opening|opens|now open|coming soon|grand opening|new location).*/i, '').trim();
    
    // Skip if name is too generic or looks like an article title
    if (name.length < 3 || /^(new|now|grand|opening|coming|see|view|what|how|the\s)/i.test(name)) {
        name = result.title.substring(0, 50).trim();
    }
    
    // Extract location with better accuracy
    let location = state;
    const cityMatch = result.snippet.match(/in\s+([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),?\s+(FL|CA|TX|NY|[A-Z]{2})/i);
    if (cityMatch) {
        location = `${cityMatch[1]}, ${cityMatch[2].toUpperCase()}`;
    } else {
        // Try to find city name in title
        const titleCityMatch = result.title.match(/([A-Z][a-z]+(?:\s[A-Z][a-z]+)*),?\s+(FL|CA|TX|NY|[A-Z]{2})/i);
        if (titleCityMatch) {
            location = `${titleCityMatch[1]}, ${titleCityMatch[2].toUpperCase()}`;
        }
    }
    
    // Temperature scoring - more accurate
    const hotKeywords = ['grand opening', 'now open', 'just opened', 'opened today', 'construction permit issued', 'breaking ground'];
    const warmKeywords = ['coming soon', 'opening soon', 'planned', 'announced', 'will open', 'under construction'];
    const isHot = hotKeywords.some(k => text.includes(k));
    const temp = isHot ? 'hot' : 'warm';
    
    // Opening timeline
    let opening = 'TBA';
    const monthMatch = text.match(/(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4}/i);
    const seasonMatch = text.match(/(spring|summer|fall|winter|early|late|mid)\s+\d{4}/i);
    
    if (monthMatch) opening = monthMatch[0];
    else if (seasonMatch) opening = seasonMatch[0];
    else if (text.includes('now open') || text.includes('just opened')) opening = 'OPEN NOW';
    else if (text.includes('coming soon') || text.includes('opening soon')) opening = 'Soon';
    else if (text.includes('2025')) opening = '2025';
    else if (text.includes('2026')) opening = '2026';
    
    // Signage opportunities based on business type
    const signage = [];
    if (/restaurant|food|dining|eatery|grill|kitchen/i.test(text)) {
        signage.push('Exterior Channel Letters', 'Interior Menu Boards', 'Window Graphics');
    } else if (/fast food|drive.?thru|quick service/i.test(text)) {
        signage.push('Drive-Thru Menu Boards', 'Pylon Sign', 'Digital Display Boards');
    } else if (/coffee|cafe|espresso/i.test(text)) {
        signage.push('Storefront Signage', 'Outdoor A-Frame', 'Menu Boards');
    } else if (/retail|store|shop|boutique/i.test(text)) {
        signage.push('Storefront Signage', 'Window Displays', 'Wayfinding Signs');
    } else if (/shopping center|mall|plaza/i.test(text)) {
        signage.push('Monument Sign', 'Directional Signage', 'Tenant Signs');
    } else if (/hotel|resort|lodging/i.test(text)) {
        signage.push('Exterior Signage', 'Wayfinding System', 'Room Signage');
    } else if (/gym|fitness|health club/i.test(text)) {
        signage.push('Channel Letters', 'Window Graphics', 'Interior Branding');
    } else {
        signage.push('Custom Exterior Signage', 'Building Identification', 'Wayfinding Signs');
    }
    
    // Revenue estimate based on business type and keywords
    let revenue = '$5,000-$15,000';
    if (/chick.?fil.?a|costco|walmart|target|whole foods/i.test(text)) {
        revenue = '$40,000-$100,000';
    } else if (/shopping center|mall|plaza|development/i.test(text)) {
        revenue = '$25,000-$75,000';
    } else if (/starbucks|chipotle|panera|five guys|shake shack/i.test(text)) {
        revenue = '$15,000-$35,000';
    } else if (/franchise|chain/i.test(text)) {
        revenue = '$12,000-$30,000';
    }
    
    return {
        name,
        location,
        phone: 'Contact via source',
        opening,
        temp,
        signage,
        revenue,
        source: result.url,
        queryType
    };
}

// ==================== STATE SCRAPER ====================
async function scrapeState(state) {
    logger.info(`\n🔍 ${state.toUpperCase()}`);
    const allResults = [];
    const allQueries = [
        ...CONFIG.KEYWORDS.map(k => `${k} ${state}`),
        ...CONFIG.FRANCHISE_CHAINS.map(f => `${f} ${state}`)
    ];
    
    for (const query of allQueries) {
        const results = await searchBing(query);
        allResults.push(...results);
        await delay(Math.random() * (CONFIG.RATE_LIMIT.maxDelay - CONFIG.RATE_LIMIT.minDelay) + CONFIG.RATE_LIMIT.minDelay);
    }
    
    // Deduplicate by URL
    const seen = new Set();
    const unique = allResults.filter(r => {
        if (seen.has(r.url)) return false;
        seen.add(r.url);
        return true;
    });
    
    // Enrich into leads and filter out nulls
    const leads = unique
        .map(r => enrichLead(r, state, 'bing'))
        .filter(lead => lead !== null); // Remove filtered-out results
    
    logger.success(`${state}: ${leads.length} unique leads (from ${unique.length} results)`);
    return leads;
}

// ==================== HTML GENERATOR ====================
function generateHTML(state, leads) {
    const stateCode = CONFIG.STATE_CODES[state];
    const ts        = new Date().toISOString().split('T')[0];
    const hotLeads  = leads.filter(l => l.temp === 'hot').length;
    const warmLeads = leads.filter(l => l.temp === 'warm').length;

    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <title>${state} Sign Leads | Live Opportunities for Sign Companies</title>
    <meta name="description" content="${leads.length} active sign opportunities in ${state}. Grand openings, new construction, retail developments - updated daily.">
    <style>
        *{margin:0;padding:0;box-sizing:border-box}
        body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:linear-gradient(135deg,#1a1a2e,#16213e);color:#e4e7eb;line-height:1.6}
        .container{max-width:1400px;margin:0 auto;padding:20px;background:rgba(255,255,255,.02);border-radius:15px}
        header{background:linear-gradient(135deg,#00d4ff,#0099ff);padding:30px;border-radius:15px 15px 0 0;text-align:center;color:white}
        header h1{font-size:2.5em;margin-bottom:8px;text-shadow:2px 2px 4px rgba(0,0,0,.3)}
        header p{font-size:1.1em;opacity:.95}
        .back-link{display:inline-block;margin-top:15px;color:white;text-decoration:none;padding:8px 20px;background:rgba(255,255,255,.2);border-radius:25px;transition:all .3s}
        .back-link:hover{background:rgba(255,255,255,.3);transform:scale(1.05)}
        .stats-bar{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:15px;margin:20px 0;padding:20px;background:rgba(255,255,255,.05);border-radius:10px}
        .stat-item{text-align:center;padding:15px;background:rgba(0,212,255,.1);border-radius:8px;border:1px solid rgba(0,212,255,.3)}
        .stat-number{display:block;font-size:2em;font-weight:800;color:#00d4ff}
        .filters{display:flex;gap:15px;margin:20px 0;flex-wrap:wrap}
        .search-bar,.filters select{flex:1;min-width:200px;padding:12px;background:rgba(255,255,255,.1);border:1px solid rgba(0,212,255,.3);border-radius:8px;color:white;font-size:1em}
        .search-bar::placeholder{color:rgba(255,255,255,.5)}
        .leads-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(350px,1fr));gap:20px;margin:20px 0}
        .lead-card{background:linear-gradient(135deg,rgba(255,255,255,.08),rgba(255,255,255,.05));padding:20px;border-radius:12px;border:1px solid rgba(0,212,255,.2);position:relative;transition:all .3s}
        .lead-card:hover{transform:translateY(-5px);box-shadow:0 10px 30px rgba(0,212,255,.3);border-color:rgba(0,212,255,.5)}
        .lead-card.hot{border-color:#ff4444;box-shadow:0 0 20px rgba(255,68,68,.2)}
        .lead-card.warm{border-color:#ffa500;box-shadow:0 0 20px rgba(255,165,0,.2)}
        .badge{position:absolute;top:15px;right:15px;padding:6px 12px;border-radius:20px;font-size:.75em;font-weight:800;text-transform:uppercase}
        .badge-hot{background:linear-gradient(135deg,#ff4444,#cc0000);color:white}
        .badge-warm{background:linear-gradient(135deg,#ffa500,#ff8c00);color:white}
        .lead-title{font-size:1.4em;font-weight:800;color:#00d4ff;margin:10px 0}
        .lead-detail{color:#94a3b8;margin:8px 0;font-size:.95em}
        .signage-box{background:rgba(0,212,255,.05);padding:12px;border-radius:8px;margin:12px 0;border-left:3px solid #00d4ff}
        .signage-box h4{color:#00d4ff;font-size:.9em;margin-bottom:8px;text-transform:uppercase}
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
        <p>Sign Industry Opportunities | Updated: ${ts}</p>
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
        <p>${state} Sign Lead Database | ${leads.length} opportunities | Sourced via Bing Search</p>
        <p style="margin-top:8px">Updated: ${ts} | Auto-refreshes every 96 hours</p>
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
async function deployToGitHub(state, html) {
    if (!CONFIG.OUTPUT.githubToken || !CONFIG.OUTPUT.githubRepo) return false;
    try {
        const { default: fetch } = await import('node-fetch');
        const filePath = `state-pages/${state.toLowerCase().replace(/ /g,'-')}-sign-leads.html`;
        const apiBase  = 'https://api.github.com';
        const headers  = { 'Authorization': `token ${CONFIG.OUTPUT.githubToken}`, 'Accept': 'application/vnd.github.v3+json' };

        let sha = null;
        try {
            const r = await fetch(`${apiBase}/repos/${CONFIG.OUTPUT.githubRepo}/contents/${filePath}`, { headers });
            if (r.ok) sha = (await r.json()).sha;
        } catch(e) { /* new file */ }

        const body = {
            message: `Auto-update ${state} — ${new Date().toISOString()}`,
            content: Buffer.from(html).toString('base64'),
            branch:  'main'
        };
        if (sha) body.sha = sha;

        const put = await fetch(`${apiBase}/repos/${CONFIG.OUTPUT.githubRepo}/contents/${filePath}`, {
            method: 'PUT', headers: { ...headers, 'Content-Type': 'application/json' }, body: JSON.stringify(body)
        });
        if (put.ok) { logger.success(`GitHub: deployed ${state}`); return true; }
        logger.error(`GitHub deploy failed ${state}: ${await put.text()}`);
        return false;
    } catch(err) { logger.error(`GitHub error: ${err.message}`); return false; }
}

// ==================== MAIN ====================
async function runFullScrape() {
    logger.info('\n🚀 SIGN LEAD SCRAPER v4 — Bing Edition');
    logger.info(`Time:          ${new Date().toISOString()}`);
    logger.info(`States:        ${CONFIG.US_STATES.length}`);
    logger.info(`Queries/state: ${CONFIG.KEYWORDS.length + CONFIG.FRANCHISE_CHAINS.length}`);
    logger.info('Engine:        Bing (free, no API key, cloud-IP friendly)');

    await fs.mkdir(CONFIG.OUTPUT.directory, { recursive: true });

    const start   = Date.now();
    const results = {};
    let total     = 0;

    for (const state of CONFIG.US_STATES) {
        try {
            const leads = await scrapeState(state);
            results[state] = leads;
            total += leads.length;

            if (leads.length >= CONFIG.OUTPUT.minimumLeadsPerState) {
                const html     = generateHTML(state, leads);
                const filePath = path.join(CONFIG.OUTPUT.directory, `${state.toLowerCase().replace(/ /g,'-')}-sign-leads.html`);
                await fs.writeFile(filePath, html);
                logger.success(`Saved: ${filePath} (${leads.length} leads)`);
                if (CONFIG.OUTPUT.deployToGithub) { await deployToGitHub(state, html); await delay(400); }
            } else {
                logger.warning(`${state}: ${leads.length} leads — skipping page`);
            }
        } catch(err) {
            logger.error(`Error on ${state}: ${err.message}`);
            results[state] = [];
        }
        await delay(CONFIG.RATE_LIMIT.stateDelay);
    }

    const mins = ((Date.now() - start) / 60000).toFixed(1);
    logger.success(`\n✅ COMPLETE — ${total} total leads | ${Object.keys(results).length} states | ${mins} min`);

    const summary = {
        timestamp: new Date().toISOString(), totalLeads: total,
        statesProcessed: Object.keys(results).length, durationMinutes: mins,
        nextRun: new Date(Date.now() + 96*60*60*1000).toISOString(),
        searchEngine: 'Bing (free, no API key)',
        stateBreakdown: Object.entries(results).map(([s,l]) => ({
            state: s, stateCode: CONFIG.STATE_CODES[s],
            leadCount: l.length, hotLeads: l.filter(x=>x.temp==='hot').length, warmLeads: l.filter(x=>x.temp==='warm').length
        })),
        logs: logger.exportLogs()
    };

    await fs.writeFile(path.join(CONFIG.OUTPUT.directory, 'scrape-summary.json'), JSON.stringify(summary, null, 2));
    await fs.writeFile('./next-run.json', JSON.stringify({ lastRun: new Date().toISOString(), nextRun: summary.nextRun }, null, 2));
    return results;
}

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

exports.handler = async () => {
    try {
        const r = await runFullScrape();
        return { statusCode: 200, body: JSON.stringify({ success: true, totalLeads: Object.values(r).reduce((s,l)=>s+l.length,0) }) };
    } catch(err) {
        return { statusCode: 500, body: JSON.stringify({ success: false, error: err.message }) };
    }
};

if (require.main === module) {
    runFullScrape().then(() => process.exit(0)).catch(err => { logger.error(err.message); process.exit(1); });
}

module.exports = { runFullScrape, CONFIG };
