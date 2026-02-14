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

    // ── HIGH-PRIORITY NEWS SOURCES ─────────────────────────────
    // Target these domains first for highest quality leads
    PRIORITY_DOMAINS: [
        'bizjournals.com', 'nrn.com', 'qsrmagazine.com', 'restaurantbusinessonline.com',
        'fsrmagazine.com', 'chainstoreage.com', 'retaildive.com', 'eater.com',
        'bisnow.com', 'therealdeal.com', 'commercialobserver.com', 'constructiondive.com',
        'shoppingcenterbusiness.com', 'icsc.com', 'azcentral.com', 'dallasnews.com',
        'ajc.com', 'miamiherald.com', 'chicagotribune.com', 'latimes.com'
    ],

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
        minDelay:   1500,
        maxDelay:   3000,
        stateDelay: 3000,
        maxRetries: 3
    },

    OUTPUT: {
        directory:            process.env.OUTPUT_DIR || './state-pages',
        deployToGithub:       false  // Files committed by GitHub Actions instead
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
        'yelp.com/search', 'tripadvisor.com', 'booking.com', 'expedia.com', 'archive.org', 'oldnews.com'
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
        const results = [];
        let page = 1;
        const count = 50; // Max ~50 per page
        while (page <= 3) { // Up to 3 pages (~150 results)
            const offset = (page - 1) * count + 1;
            const url = `https://www.bing.com/search?q=${encodeURIComponent(query)}&count=${count}&first=${offset}`;
            logger.info(`  Bing: ${query} (Page ${page})`);
            const res = await httpGet(url);
            if (res.status === 200) {
                const pageResults = parseBingResults(res.body);
                if (pageResults.length < 10) break; // Stop if few results
                results.push(...pageResults);
            } else {
                break;
            }
            await delay(1500 + Math.random() * 1000); // Gentle delay
            page++;
        }
        logger.info(`  Found: ${results.length} results`);
        return results;
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
    const url = result.url.toLowerCase();
    
    // Check if from priority domain - these get lighter filtering
    const priorityDomains = [
        'bizjournals', 'nrn.com', 'qsrmagazine', 'restaurantbusinessonline',
        'chainstoreage', 'retaildive', 'bisnow', 'therealdeal', 'eater.com',
        'azcentral', 'miamiherald', 'dallasnews', 'ajc.com', 'chicagotribune'
    ];
    const isPrioritySource = priorityDomains.some(d => url.includes(d));
    
    // Very relaxed: Timeline optional for all; default to positive if business context present
    const hasTimeline = /202[5-9]|future|upcoming|planned|will open|set to open|coming soon|now open|recently opened|just opened|grand opening|expansion/i.test(text);
    
    // 1. Business context (broadened to include more signage/positive terms)
    const hasBusinessContext = 
        /opening|opens|opened|open|new|construction|permit|building|location|store|restaurant|retail|franchise|expansion|development|grand|debut|launch|leasing|tenant|plaza|strip\s+mall|shopping\s+center|signage|sign\s+installation|digital\s+signs|wayfinding|branding|rebranding|remodel|renovation|new\s+build|project/i.test(text);
    
    if (!hasBusinessContext) {
        return null;
    }
    
    // 2. Reject only severe bad content (lenient)
    const isBadContent = 
        /closed permanently|going out of business|bankruptcy|shut down|history of the/i.test(text);
    
    if (isBadContent) {
        return null;
    }
    
    // 3. Business type (optional for priority; broadened for others)
    const hasBusinessType = 
        /restaurant|cafe|coffee|food|retail|store|shop|boutique|salon|gym|fitness|hotel|bank|clinic|medical|pharmacy|gas station|convenience|grocery|mall|plaza|franchise|chain|strip\s+mall|strip\s+center|shopping\s+center|retail\s+center|commercial\s+development|mixed.?use|signage|sign\s+vendor|sign\s+installation|office|warehouse|facility|venue|event\s+space/i.test(text);
    
    if (!isPrioritySource && !hasBusinessType) {
        return null;
    }
    
    // Extract business name (relaxed validation)
    let name = result.title.split(/[-–—|]/)[0].trim();
    name = name
        .replace(/\s+(opening|opens|opened|now open|coming soon|to open|will open|announces|set to|plans to).*/i, '')
        .replace(/^(new|a|the)\s+/i, '')
        .trim();
    
    // Improve name extraction
    if (name.length < 5 || /^(coming|opening|new|construction|grand|plans|set)/i.test(name)) {
        const namePatterns = [
            /([A-Z][a-z]+(?:'s)?(?:\s+[A-Z][a-z]+){0,4})\s+(?:restaurant|cafe|coffee|store|shop|opens|opening|will open|to open)/i,
            /([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})\s+(?:breaks ground|announces|plans)/i,
            /([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,4})\s+(?:Plaza|Center|Mall|Commons|Square)/i,
            /(?:at|in)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,4})\s+(?:Plaza|Center|Mall|Commons)/i,
        ];
        
        for (const pattern of namePatterns) {
            const match = (result.title + ' ' + result.snippet).match(pattern);
            if (match && match[1]) {
                name = match[1];
                break;
            }
        }
        
        if (name.length < 5) {
            name = result.title.substring(0, 70).replace(/\s*[-–—|].*/,'').trim();
        }
    }
    
    // Relaxed length: 3-200 chars
    if (name.length < 3 || name.length > 200) {
        return null;
    }
    
    // Extract location
    let location = state;
    const stateAbbr = CONFIG.STATE_CODES[state];
    const locationPatterns = [
        new RegExp(`([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*),?\\s+${stateAbbr}\\b`, 'i'),
        new RegExp(`in\\s+([A-Z][a-z]+(?:\\s+[A-Z][a-z]+)*)(?:,|\\s+${stateAbbr})`, 'i'),
        /in\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)/,
        /([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+area/i,
    ];
    
    for (const pattern of locationPatterns) {
        const match = text.match(pattern);
        if (match && match[1]) {
            const city = match[1].trim();
            if (city.length > 2 && !/^(the|new|old|east|west|north|south)$/i.test(city)) {
                location = `${city}, ${stateAbbr}`;
                break;
            }
        }
    }
    
    // Opening timeline (relaxed, default to 'Recent/Upcoming' if no match)
    let opening = 'Recent/Upcoming';
    const monthMatch = text.match(/(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2}(st|nd|rd|th)?,?\s+)?\d{4}/i);
    const seasonMatch = text.match(/(spring|summer|fall|winter|early|late|mid)\s+\d{4}/i);
    const quarterMatch = text.match(/q[1-4]\s+\d{4}/i);
    const yearMatch = text.match(/(202[5-9])/);
    
    if (monthMatch) {
        opening = monthMatch[0];
    } else if (quarterMatch) {
        opening = quarterMatch[0].toUpperCase();
    } else if (seasonMatch) {
        opening = seasonMatch[0];
    } else if (/opened|now open|just opened|recently opened/i.test(text)) {
        opening = 'OPEN NOW';
    } else if (/opening soon|coming soon|will open|plans to open|set to open/i.test(text)) {
        opening = 'Soon';
    } else if (yearMatch) {
        opening = yearMatch[1];
    }
    
    // Signage opportunities - expanded comprehensive list with more options
    const signage = [];
    
    if (/fast.?food|quick.?service|drive.?thru|burger|chicken|taco|pizza|sandwich/i.test(text)) {
        signage.push('Drive-Thru Menu Board System', 'Pylon/Monument Sign', 'Exterior Channel Letters', 'Digital Menu Displays', 'Window Graphics', 'Parking Lot Signs');
    } else if (/coffee|cafe|espresso|starbucks|dutch bros/i.test(text)) {
        signage.push('Exterior Channel Letters', 'Drive-Thru Menu Boards', 'Outdoor A-Frame Signs', 'Interior Menu Boards', 'Branding Wall Graphics', 'Directional Signs');
    } else if (/restaurant|dining|grill|bistro|eatery|bar\s+and\s+grill/i.test(text)) {
        signage.push('Exterior Channel Letters', 'Monument Sign', 'Window Graphics', 'Interior Menu Boards', 'Neon Signs', 'Awning Signs');
    } else if (/retail|store|shop|boutique|clothing|apparel/i.test(text)) {
        signage.push('Storefront Channel Letters', 'Window Displays & Graphics', 'Wayfinding Signage', 'Interior Branding', 'Sale Banners', 'Floor Graphics');
    } else if (/shopping\s+center|mall|plaza|strip\s+center|retail\s+development|mixed.?use/i.test(text)) {
        signage.push('Monument/Pylon Sign', 'Tenant Panel System', 'Directional Signage', 'Parking Wayfinding', 'Digital Directory', 'Entrance Signs');
    } else if (/gym|fitness|health\s+club|workout|training/i.test(text)) {
        signage.push('Exterior Channel Letters', 'Window Graphics', 'Interior Motivational Graphics', 'Wayfinding', 'Membership Banners', 'Equipment Labels');
    } else if (/hotel|resort|inn|lodging|motel/i.test(text)) {
        signage.push('Monument Sign', 'Building Identification', 'Wayfinding System', 'Room Number Signs', 'Lobby Branding', 'Pool Area Signs');
    } else if (/bank|credit\s+union|financial/i.test(text)) {
        signage.push('Monument Sign', 'Channel Letters', 'Drive-Thru Signage', 'Interior Branding', 'ATM Surrounds', 'Security Signs');
    } else if (/medical|dental|clinic|healthcare|pharmacy|urgent\s+care/i.test(text)) {
        signage.push('Monument Sign', 'Building Identification', 'Wayfinding Signage', 'ADA Compliant Signs', 'Waiting Room Graphics', 'Directional Plaques');
    } else if (/gas\s+station|convenience|c-store|fuel/i.test(text)) {
        signage.push('Pylon Sign with Price Display', 'Canopy Signage', 'Storefront Letters', 'Pump Toppers', 'Fuel Island Graphics', 'Car Wash Signs');
    } else if (/grocery|supermarket|market/i.test(text)) {
        signage.push('Monument/Pylon Sign', 'Storefront Channel Letters', 'Interior Wayfinding', 'Department Signs', 'Produce Displays', 'Sale Aisle Signs');
    } else if (/office|warehouse|facility|venue|event\s+space/i.test(text)) {
        signage.push('Building Directory', 'Exterior Identification', 'Interior Wayfinding', 'Safety Signs', 'Conference Room Plaques', 'Loading Dock Signs');
    } else {
        // Expanded default for any business
        signage.push('Exterior Building Signage', 'Channel Letter Set', 'Monument Sign', 'Interior Wayfinding', 'Window Decals', 'Vehicle Wraps', 'Banner Stands', 'Digital Displays');
    }
    
    // Revenue estimate (unchanged)
    let revenue = '$8,000-$20,000';
    
    if (/chick.?fil.?a|in.?n.?out|raising.?cane|whataburger/i.test(text)) {
        revenue = '$50,000-$100,000';
    } else if (/costco|walmart|target|whole\s+foods|kroger/i.test(text)) {
        revenue = '$60,000-$150,000';
    } else if (/shopping\s+center|mall|plaza.*development|mixed.?use/i.test(text)) {
        revenue = '$35,000-$80,000';
    } else if (/starbucks|chipotle|panera|five\s+guys|shake\s+shack|dutch\s+bros/i.test(text)) {
        revenue = '$20,000-$45,000';
    } else if (/drive.?thru|fast.?food/i.test(text)) {
        revenue = '$15,000-$35,000';
    } else if (/hotel|resort/i.test(text)) {
        revenue = '$25,000-$60,000';
    } else if (/franchise|chain/i.test(text)) {
        revenue = '$12,000-$30,000';
    }
    
    return {
        name,
        location,
        phone: 'Contact via source',
        opening,
        signage,
        revenue,
        source: result.url,
        queryType,
        isPriority: isPrioritySource
    };
}

// ==================== STATE SCRAPER ====================
async function scrapeState(state) {
    logger.info(`\n🔍 ${state.toUpperCase()}`);
    const allResults = [];
    
    // PHASE 1: Press Releases & Announcements
    logger.info(`  Phase 1: Press Releases & Announcements...`);
    const phase1Queries = [
        `"new business opening" OR "grand opening" OR "store opening" OR "franchise expansion" site:prnewswire.com OR site:businesswire.com "2025" OR "2026" OR "2027" OR "now" OR "recent" ${state}`,
        `"business expansion" OR "new location" OR "development agreement" "signage" OR "sign installation" site:globenewswire.com "2025" OR "2026" OR "now" OR "recent" ${state}`,
        `intitle:"press release" "new store" OR "ribbon cutting" OR "opening soon" "2025" OR "2026" OR "future" OR "recent" OR "current" ${state}`
    ];
    for (const query of phase1Queries) {
        const results = await searchBing(query);
        allResults.push(...results);
        await delay(Math.random() * 1000 + 1000);
    }
    
    // PHASE 2: RFPs & Tenders
    logger.info(`  Phase 2: RFPs & Tenders...`);
    const phase2Queries = [
        `"request for proposal" OR "RFP" OR "tender" "signage" OR "digital signs" OR "outdoor signs" OR "installation" filetype:pdf "2025" OR "2026" OR "2027" OR "now" OR "current" ${state}`,
        `"bid opportunity" OR "solicitation" "signage vendor" OR "sign maintenance" OR "wayfinding" site:gov OR site:org filetype:pdf ${state}`,
        `intitle:"RFP" "facility maintenance" OR "construction management" OR "remodel" "signage" filetype:pdf "2025" OR "2026" OR "current" ${state}`
    ];
    for (const query of phase2Queries) {
        const results = await searchBing(query);
        allResults.push(...results);
        await delay(Math.random() * 1000 + 1000);
    }
    
    // PHASE 3: Local Newspapers & Chambers
    logger.info(`  Phase 3: Local Newspapers & Chambers...`);
    const phase3Queries = [
        `"new business announcement" OR "store opening" OR "grand opening" site:miamiherald.com OR site:orlandosentinel.com OR site:tcpalm.com "2025" OR "2026" OR "future" OR "now" OR "recent" ${state}`,
        `"business ribbon cutting" OR "new shop opening" OR "expansion announcement" site:usatoday.com OR site:local.newspaper.com "2025" OR "2026" OR "recent" ${state}`,
        `"chamber of commerce" "new members" OR "business announcements" OR "opening events" site:chamberofcommerce.com OR site:localchamber.org "2025" OR "2026" OR "now" ${state}`
    ];
    if (state === 'Florida') {
        phase3Queries.push(`"new business opening" "Port Saint Lucie" OR "Treasure Coast" site:tcpalm.com "2025" OR "2026" OR "now"`);
    }
    for (const query of phase3Queries) {
        const results = await searchBing(query);
        allResults.push(...results);
        await delay(Math.random() * 1000 + 1000);
    }
    
    // PHASE 4: Franchises & Commercial
    logger.info(`  Phase 4: Franchises & Commercial...`);
    const topFranchises = [
        'Chick-fil-A', 'Dutch Bros', "Raising Cane's", 'Starbucks', 
        'Chipotle', 'Five Guys', 'Wingstop', 'Crumbl Cookies'
    ];
    for (const franchise of topFranchises) {
        const query = `${franchise} opening OR expansion OR remodel "2025" OR "2026" OR "2027" OR "now" OR "recent" ${state}`;
        const results = await searchBing(query);
        allResults.push(...results);
        await delay(Math.random() * 1000 + 1000);
    }
    const commercialQueries = [
        `strip mall leasing OR construction OR renovation "2025" OR "2026" OR "now" ${state}`,
        `shopping center development OR tenants OR remodel "future" OR "current" OR "recent" ${state}`,
        `commercial retail leasing OR new plaza OR expansion "2025" OR "2026" OR "now" ${state}`
    ];
    for (const query of commercialQueries) {
        const results = await searchBing(query);
        allResults.push(...results);
        await delay(Math.random() * 1000 + 1000);
    }
    
    // PHASE 5: General Openings (broadened)
    logger.info(`  Phase 5: General Openings...`);
    const phase5Queries = [
        `restaurant OR retail opening OR expansion "2025" OR "2026" OR "2027" OR "coming 2026" OR "now open" OR "recent opening" ${state}`,
        `new store OR franchise OR business "expansion 2025" OR "opening soon 2026" OR "recent remodel" OR "new project" ${state}`
    ];
    for (const query of phase5Queries) {
        const results = await searchBing(query);
        allResults.push(...results);
        await delay(Math.random() * 1000 + 1000);
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
        .filter(lead => lead !== null);
    
    logger.success(`${state}: ${leads.length} qualified leads (from ${unique.length} results)`);
    return leads;
}

// ==================== HTML GENERATOR ====================
function generateHTML(state, leads) {
    const stateCode = CONFIG.STATE_CODES[state];
    const ts        = new Date().toISOString().split('T')[0];

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
        .search-bar{flex:1;min-width:200px;padding:12px;background:rgba(255,255,255,.1);border:1px solid rgba(0,212,255,.3);border-radius:8px;color:white;font-size:1em}
        .search-bar::placeholder{color:rgba(255,255,255,.5)}
        .leads-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(350px,1fr));gap:20px;margin:20px 0}
        .lead-card{background:linear-gradient(135deg,rgba(255,255,255,.08),rgba(255,255,255,.05));padding:20px;border-radius:12px;border:1px solid rgba(0,212,255,.2);position:relative;transition:all .3s}
        .lead-card:hover{transform:translateY(-5px);box-shadow:0 10px 30px rgba(0,212,255,.3);border-color:rgba(0,212,255,.5)}
        .lead-title{font-size:1.4em;font-weight:800;color:#00d4ff;margin:10px 0}
        .lead-detail{color:#94a3b8;margin:8px 0;font-size:.95em}
        .signage-box{background:rgba(0,212,255,.05);padding:12px;border-radius:8px;margin:12px 0;border-left:3px solid #00d4ff}
        .signage-box h4{color:#00d4ff;font-size:.9em;margin-bottom:8px;text-transform:uppercase}
        .signage-box li{color:#cbd5e1;font-size:.9em;padding:2px 0;list-style:none}
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
        <div class="stat-item"><span class="stat-number">${stateCode}</span><span>State</span></div>
        <div class="stat-item"><span class="stat-number">${ts}</span><span>Last Updated</span></div>
    </div>
    <div class="filters">
        <input type="text" class="search-bar" id="searchBox" placeholder="Search by name or location..." oninput="filterLeads()">
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
            <div class="lead-card">
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
        const search=document.getElementById('searchBox').value.toLowerCase();
        displayLeads(allLeads.filter(l=>
            search===''||l.name.toLowerCase().includes(search)||l.location.toLowerCase().includes(search)
        ));
    }
    window.onload=()=>displayLeads(allLeads);
</script>
</body>
</html>`;
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
    const statesToScrape = process.env.STATE ? [process.env.STATE] : CONFIG.US_STATES; // e.g., STATE=Florida

    for (const state of statesToScrape) {
        try {
            const leads = await scrapeState(state);
            results[state] = leads;
            total += leads.length;

            // Always write the HTML file - overwrite existing
            const html     = generateHTML(state, leads);
            const filePath = path.join(CONFIG.OUTPUT.directory, `${state.toLowerCase().replace(/ /g,'-')}-sign-leads.html`);
            await fs.writeFile(filePath, html);
            logger.success(`✅ Saved: ${filePath} (${leads.length} leads)`);
            
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
            leadCount: l.length
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
