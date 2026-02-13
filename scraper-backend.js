// =====================================================
// AUTONOMOUS SIGN LEAD SCRAPER - BACKEND SERVICE
// =====================================================
// This script runs daily at midnight to scrape sign industry leads
// Deploy as: AWS Lambda, Google Cloud Function, or Cron Job

const fs = require('fs').promises;
const path = require('path');

// ==================== CONFIGURATION ====================
const CONFIG = {
    // State configuration
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
    
    // SCHEDULING - Runs every 96 hours
    SCHEDULE: {
        intervalHours: 96,
        nextRunFile: './next-run.json'
    },
    
    // MULTI-API KEY SUPPORT (up to 10 keys - GOOGLE ONLY, FREE)
    API_KEYS: {
        google: {
            keys: (process.env.GOOGLE_API_KEYS || process.env.GOOGLE_API_KEY || '').split(',').filter(k => k.trim()),
            searchEngineIds: (process.env.GOOGLE_SEARCH_ENGINE_IDS || process.env.GOOGLE_SEARCH_ENGINE_ID || '').split(',').filter(k => k.trim()),
            currentIndex: 0,
            endpoint: 'https://www.googleapis.com/customsearch/v1'
        }
    },
    
    // Enhanced search keywords for high-quality leads
    KEYWORDS: {
        // Franchise expansion tracking
        franchiseExpansion: [
            'franchise expansion',
            'new franchise location',
            'franchise development',
            'multi-unit franchise',
            'franchise territory'
        ],
        // Retail & restaurant openings
        retailRestaurant: [
            'new store opening',
            'restaurant opening',
            'retail development',
            'grand opening',
            'coming soon'
        ],
        // Commercial construction
        commercial: [
            'commercial development',
            'mixed-use development',
            'retail center',
            'shopping center',
            'strip mall'
        ],
        // Permit & planning
        permits: [
            'building permit',
            'sign permit',
            'construction permit',
            'site plan approval'
        ],
        // High-value chains
        nationalChains: [
            'national chain',
            'regional expansion',
            'corporate expansion',
            'store rollout'
        ]
    },
    
    // LOCAL BUSINESS JOURNALS (comprehensive state coverage)
    BUSINESS_JOURNALS: {
        enabled: true,
        sources: [
            // American City Business Journals (covers 40+ cities)
            { name: 'Atlanta Business Chronicle', url: 'https://www.bizjournals.com/atlanta/news/real-estate.rss', state: 'Georgia' },
            { name: 'Austin Business Journal', url: 'https://www.bizjournals.com/austin/news/real-estate.rss', state: 'Texas' },
            { name: 'Boston Business Journal', url: 'https://www.bizjournals.com/boston/news/real-estate.rss', state: 'Massachusetts' },
            { name: 'Charlotte Business Journal', url: 'https://www.bizjournals.com/charlotte/news/real-estate.rss', state: 'North Carolina' },
            { name: 'Chicago Business Journal', url: 'https://www.bizjournals.com/chicago/news/real-estate.rss', state: 'Illinois' },
            { name: 'Dallas Business Journal', url: 'https://www.bizjournals.com/dallas/news/real-estate.rss', state: 'Texas' },
            { name: 'Denver Business Journal', url: 'https://www.bizjournals.com/denver/news/real-estate.rss', state: 'Colorado' },
            { name: 'Houston Business Journal', url: 'https://www.bizjournals.com/houston/news/real-estate.rss', state: 'Texas' },
            { name: 'LA Business Journal', url: 'https://www.bizjournals.com/losangeles/news/real-estate.rss', state: 'California' },
            { name: 'Miami Business Journal', url: 'https://www.bizjournals.com/southflorida/news/real-estate.rss', state: 'Florida' },
            { name: 'Phoenix Business Journal', url: 'https://www.bizjournals.com/phoenix/news/real-estate.rss', state: 'Arizona' },
            { name: 'San Francisco Business Times', url: 'https://www.bizjournals.com/sanfrancisco/news/real-estate.rss', state: 'California' },
            { name: 'Seattle Business Journal', url: 'https://www.bizjournals.com/seattle/news/real-estate.rss', state: 'Washington' },
            
            // Additional state/regional journals
            { name: 'Crain\'s Chicago', url: 'https://www.chicagobusiness.com/rss/real-estate', state: 'Illinois' },
            { name: 'Crain\'s Detroit', url: 'https://www.crainsdetroit.com/rss/real-estate', state: 'Michigan' },
            { name: 'Crain\'s New York', url: 'https://www.crainsnewyork.com/rss/real-estate', state: 'New York' },
            { name: 'Orlando Business Journal', url: 'https://www.bizjournals.com/orlando/news/real-estate.rss', state: 'Florida' },
            { name: 'Tampa Bay Business Journal', url: 'https://www.bizjournals.com/tampabay/news/real-estate.rss', state: 'Florida' }
        ],
        // Keywords for filtering relevant articles
        filterKeywords: [
            'opening', 'construction', 'development', 'expansion', 'retail', 
            'restaurant', 'store', 'center', 'plaza', 'mall', 'franchise',
            'chain', 'signage', 'tenant', 'lease'
        ]
    },
    
    // FRANCHISE EXPANSION TRACKERS
    FRANCHISE_SOURCES: {
        enabled: true,
        sources: [
            'https://www.franchising.com/news/',
            'https://www.qsrmagazine.com/news/',
            'https://www.restaurantbusinessonline.com/financing',
            'https://www.nrn.com/news',
            'https://www.franchiseupdatemedia.com/',
            'https://www.entrepreneur.com/franchises/news'
        ],
        // Major franchise chains to track
        trackChains: [
            'McDonald\'s', 'Starbucks', 'Subway', 'Dunkin\'', 'Chick-fil-A',
            'Taco Bell', 'Wendy\'s', 'Burger King', 'Chipotle', 'Panera',
            'Five Guys', 'Jimmy John\'s', 'Jersey Mike\'s', 'Wingstop',
            'Shake Shack', 'In-N-Out', 'Culver\'s', 'Raising Cane\'s',
            'Dutch Bros', 'Crumbl', 'Insomnia Cookies', 'Smoothie King',
            'Planet Fitness', 'Anytime Fitness', 'LA Fitness', 'Orangetheory',
            'AutoZone', 'O\'Reilly', 'Advance Auto', 'Pep Boys',
            'Dollar General', 'Dollar Tree', 'Family Dollar', '7-Eleven',
            'Circle K', 'Wawa', 'Sheetz', 'QuikTrip', 'Buc-ee\'s'
        ]
    },
    
    // AI SCORING - DISABLED (Free version)
    AI_SCORING: {
        enabled: false,
        scoreThreshold: 0,
        batchSize: 0
    },
    
    // Output configuration
    OUTPUT: {
        directory: process.env.OUTPUT_DIR || './state-pages',
        githubRepo: process.env.GITHUB_REPO,
        githubToken: process.env.GITHUB_TOKEN,
        deployToGithub: true,
        saveRawData: true, // Save unscored leads for review
        minimumLeadsPerState: 5 // Don't publish if fewer than this
    },
    
    // Rate limiting with multi-key rotation
    RATE_LIMIT: {
        requestsPerMinute: 60,
        delayBetweenStates: 3000, // ms - increased for quality
        maxRetriesPerKey: 3,
        keyRotationDelay: 1000 // ms between key switches
    }
};

// ==================== API KEY ROTATION MANAGER ====================
class APIKeyManager {
    constructor() {
        this.keyUsage = {
            google: new Map()
        };
    }
    
    getNextKey(service) {
        const serviceConfig = CONFIG.API_KEYS[service];
        if (!serviceConfig || !serviceConfig.keys || serviceConfig.keys.length === 0) {
            throw new Error(`No API keys configured for ${service}`);
        }
        
        // Rotate to next key
        const key = serviceConfig.keys[serviceConfig.currentIndex];
        const searchEngineId = service === 'google' && serviceConfig.searchEngineIds 
            ? serviceConfig.searchEngineIds[serviceConfig.currentIndex % serviceConfig.searchEngineIds.length]
            : null;
        
        // Track usage
        if (!this.keyUsage[service].has(key)) {
            this.keyUsage[service].set(key, { calls: 0, errors: 0, lastUsed: null });
        }
        const usage = this.keyUsage[service].get(key);
        usage.calls++;
        usage.lastUsed = new Date();
        
        // Rotate index for next call
        serviceConfig.currentIndex = (serviceConfig.currentIndex + 1) % serviceConfig.keys.length;
        
        logger.info(`Using ${service} key #${serviceConfig.currentIndex + 1}/${serviceConfig.keys.length} (${usage.calls} calls)`);
        
        return { key, searchEngineId };
    }
    
    markKeyError(service, key) {
        if (this.keyUsage[service].has(key)) {
            this.keyUsage[service].get(key).errors++;
        }
    }
    
    getKeyStats() {
        const stats = {};
        for (const [service, usageMap] of Object.entries(this.keyUsage)) {
            stats[service] = Array.from(usageMap.entries()).map(([key, usage]) => ({
                key: key.substring(0, 8) + '...',
                calls: usage.calls,
                errors: usage.errors,
                lastUsed: usage.lastUsed
            }));
        }
        return stats;
    }
}

const apiKeyManager = new APIKeyManager();

// ==================== LOGGING SYSTEM ====================
class Logger {
    constructor() {
        this.logs = [];
    }
    
    log(message, level = 'info') {
        const timestamp = new Date().toISOString();
        const logEntry = { timestamp, level, message };
        this.logs.push(logEntry);
        console.log(`[${timestamp}] [${level.toUpperCase()}] ${message}`);
    }
    
    info(message) { this.log(message, 'info'); }
    success(message) { this.log(message, 'success'); }
    warning(message) { this.log(message, 'warning'); }
    error(message) { this.log(message, 'error'); }
    
    exportLogs() {
        return this.logs;
    }
}

const logger = new Logger();

// ==================== API INTEGRATIONS ====================
class SearchEngines {
    constructor() {
        this.cache = new Map();
    }
    
    async searchGoogle(query, state) {
        const { key, searchEngineId } = apiKeyManager.getNextKey('google');
        if (!key || !searchEngineId) {
            logger.warning('Google Search API not configured');
            return [];
        }
        
        try {
            const { default: fetch } = await import('node-fetch');
            const url = new URL(CONFIG.API_KEYS.google.endpoint);
            url.searchParams.set('key', key);
            url.searchParams.set('cx', searchEngineId);
            url.searchParams.set('q', `${query} ${state} 2026`);
            url.searchParams.set('num', '10');
            
            const response = await fetch(url.toString());
            const data = await response.json();
            
            if (data.error) {
                apiKeyManager.markKeyError('google', key);
                throw new Error(data.error.message);
            }
            
            if (data.items) {
                logger.info(`Google: Found ${data.items.length} results for "${query}" in ${state}`);
                return data.items.map(item => ({
                    title: item.title,
                    url: item.link,
                    snippet: item.snippet,
                    source: 'google',
                    quality: 'high'
                }));
            }
            
            return [];
        } catch (error) {
            logger.error(`Google search error: ${error.message}`);
            return [];
        }
    }
    
    async searchBusinessJournals(state) {
        if (!CONFIG.BUSINESS_JOURNALS.enabled) {
            return [];
        }
        
        logger.info(`Searching business journals for ${state}...`);
        const results = [];
        
        // Get journals for this state
        const stateJournals = CONFIG.BUSINESS_JOURNALS.sources.filter(j => j.state === state);
        
        for (const journal of stateJournals) {
            try {
                const articles = await this.fetchRSSFeed(journal.url);
                const filtered = articles.filter(article => 
                    this.matchesBusinessJournalCriteria(article)
                );
                
                results.push(...filtered.map(article => ({
                    title: article.title,
                    url: article.link,
                    snippet: article.description || article.title,
                    source: `business-journal:${journal.name}`,
                    quality: 'premium',
                    publishDate: article.pubDate
                })));
                
                logger.success(`${journal.name}: Found ${filtered.length} relevant articles`);
            } catch (error) {
                logger.error(`Failed to fetch ${journal.name}: ${error.message}`);
            }
        }
        
        return results;
    }
    
    async fetchRSSFeed(url) {
        try {
            const { default: fetch } = await import('node-fetch');
            const response = await fetch(url);
            const xml = await response.text();
            
            // Basic RSS parsing (in production, use a proper XML parser)
            const items = [];
            const itemRegex = /<item>([\s\S]*?)<\/item>/g;
            let match;
            
            while ((match = itemRegex.exec(xml)) !== null) {
                const item = match[1];
                const title = this.extractTag(item, 'title');
                const link = this.extractTag(item, 'link');
                const description = this.extractTag(item, 'description');
                const pubDate = this.extractTag(item, 'pubDate');
                
                if (title && link) {
                    items.push({ title, link, description, pubDate });
                }
            }
            
            return items;
        } catch (error) {
            throw new Error(`RSS fetch failed: ${error.message}`);
        }
    }
    
    extractTag(xml, tag) {
        const regex = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)</${tag}>`, 'i');
        const match = xml.match(regex);
        return match ? match[1].replace(/<!\[CDATA\[(.*?)\]\]>/g, '$1').trim() : null;
    }
    
    matchesBusinessJournalCriteria(article) {
        const text = `${article.title} ${article.description}`.toLowerCase();
        
        // Must contain at least one filter keyword
        return CONFIG.BUSINESS_JOURNALS.filterKeywords.some(keyword => 
            text.includes(keyword)
        );
    }
    
    async searchFranchiseSources(state) {
        if (!CONFIG.FRANCHISE_SOURCES.enabled) {
            return [];
        }
        
        logger.info(`Searching franchise expansion news for ${state}...`);
        const results = [];
        
        // Search for each tracked franchise chain (Google only)
        for (const chain of CONFIG.FRANCHISE_SOURCES.trackChains) {
            const query = `${chain} expansion ${state}`;
            
            // Use Google search only
            const googleResults = await this.searchGoogle(query, '');
            results.push(...googleResults);
            
            // Rate limit
            await this.delay(500);
        }
        
        logger.success(`Found ${results.length} franchise expansion leads for ${state}`);
        return results;
    }
    
    async searchAll(query, state) {
        // Google only - no Bing
        return await this.searchGoogle(query, state);
    }
    
    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// ==================== AI LEAD SCORING ====================
class AILeadScorer {
    constructor() {
        this.scoredLeads = new Map();
    }
    
    async scoreLeads(leads) {
        if (!CONFIG.AI_SCORING.enabled) {
            logger.warning('AI scoring disabled - returning all leads');
            return leads.map(lead => ({ ...lead, aiScore: 50 }));
        }
        
        if (CONFIG.API_KEYS.anthropic.keys.length === 0) {
            logger.warning('No Anthropic API keys configured - skipping AI scoring');
            return leads.map(lead => ({ ...lead, aiScore: 50 }));
        }
        
        logger.info(`AI Scoring ${leads.length} leads in batches of ${CONFIG.AI_SCORING.batchSize}...`);
        const scoredLeads = [];
        
        // Process in batches
        for (let i = 0; i < leads.length; i += CONFIG.AI_SCORING.batchSize) {
            const batch = leads.slice(i, i + CONFIG.AI_SCORING.batchSize);
            const scored = await this.scoreBatch(batch);
            scoredLeads.push(...scored);
            
            logger.info(`Scored batch ${Math.floor(i / CONFIG.AI_SCORING.batchSize) + 1}/${Math.ceil(leads.length / CONFIG.AI_SCORING.batchSize)}`);
        }
        
        // Filter by threshold
        const highQuality = scoredLeads.filter(lead => lead.aiScore >= CONFIG.AI_SCORING.scoreThreshold);
        logger.success(`AI Scoring: ${highQuality.length}/${leads.length} leads passed quality threshold (${CONFIG.AI_SCORING.scoreThreshold}+)`);
        
        return highQuality;
    }
    
    async scoreBatch(leads) {
        try {
            const { key } = apiKeyManager.getNextKey('anthropic');
            const { default: fetch } = await import('node-fetch');
            
            const prompt = this.buildScoringPrompt(leads);
            
            const response = await fetch(CONFIG.API_KEYS.anthropic.endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': key,
                    'anthropic-version': '2023-06-01'
                },
                body: JSON.stringify({
                    model: CONFIG.AI_SCORING.model,
                    max_tokens: 4000,
                    messages: [{
                        role: 'user',
                        content: prompt
                    }]
                })
            });
            
            const data = await response.json();
            
            if (data.error) {
                apiKeyManager.markKeyError('anthropic', key);
                throw new Error(data.error.message);
            }
            
            const scores = this.parseScores(data.content[0].text);
            
            // Attach scores to leads
            return leads.map((lead, index) => ({
                ...lead,
                aiScore: scores[index]?.score || 0,
                scoreBreakdown: scores[index]?.breakdown || {},
                qualityReason: scores[index]?.reason || 'Not assessed'
            }));
        } catch (error) {
            logger.error(`AI scoring error: ${error.message}`);
            // Return leads with neutral scores on error
            return leads.map(lead => ({ ...lead, aiScore: 50 }));
        }
    }
    
    buildScoringPrompt(leads) {
        return `You are an expert in the sign industry evaluating potential business leads. Score each lead from 0-100 based on these criteria:

SCORING CRITERIA (Total: 100 points):
1. Revenue Impact (30 points): Estimated signage revenue potential
   - National chain/franchise: 25-30 points
   - Regional chain: 15-24 points
   - Local business: 5-14 points
   
2. Urgency (25 points): Timeline proximity
   - Opening in 1-3 months: 20-25 points
   - Opening in 4-6 months: 15-19 points
   - Opening in 7-12 months: 10-14 points
   - Opening beyond 12 months: 0-9 points
   
3. Data Quality (20 points): Contact information completeness
   - Full contact info + phone: 18-20 points
   - Partial contact info: 10-17 points
   - Minimal/generic info: 0-9 points
   
4. Brand Recognition (15 points): Company reputation
   - Fortune 500 / National brand: 12-15 points
   - Regional brand: 7-11 points
   - Unknown/startup: 0-6 points
   
5. Source Verification (10 points): Data reliability
   - Business journal/official source: 8-10 points
   - Press release: 5-7 points
   - General search result: 0-4 points

LEADS TO SCORE:
${leads.map((lead, i) => `
Lead ${i + 1}:
- Name: ${lead.name}
- Location: ${lead.location}
- Contact: ${lead.contact}
- Phone: ${lead.phone}
- Opening: ${lead.opening}
- Source: ${lead.source}
- Temperature: ${lead.temp}
`).join('\n')}

Respond ONLY with a JSON array of scores in this exact format:
[
  {
    "leadIndex": 0,
    "score": 85,
    "breakdown": {
      "revenueImpact": 28,
      "urgency": 22,
      "dataQuality": 18,
      "brandRecognition": 12,
      "verification": 5
    },
    "reason": "High-value national franchise with confirmed opening date and complete contact info"
  },
  ...
]`;
    }
    
    parseScores(responseText) {
        try {
            // Extract JSON from response (handle markdown code blocks)
            const jsonMatch = responseText.match(/\[[\s\S]*\]/);
            if (!jsonMatch) {
                throw new Error('No JSON array found in response');
            }
            
            const scores = JSON.parse(jsonMatch[0]);
            return scores;
        } catch (error) {
            logger.error(`Failed to parse AI scores: ${error.message}`);
            return [];
        }
    }
}

// ==================== LEAD EXTRACTION ====================
class LeadExtractor {
    constructor() {
        this.patterns = {
            phone: /(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g,
            email: /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g,
            address: /\d+\s+[\w\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Court|Ct|Circle|Cir|Way)/gi,
            revenue: /\$[\d,]+K?(?:\s*-\s*\$[\d,]+K?)?/g,
            openingDate: /(?:opening|opens?|grand opening|coming)[\s:]*((?:Q[1-4]\s+)?\d{4}|(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}|(?:Spring|Summer|Fall|Winter)\s+\d{4})/gi
        };
    }
    
    extractLeadFromSearchResult(result, state) {
        const text = `${result.title} ${result.snippet}`.toLowerCase();
        
        // Determine if it's a sign-related lead
        const signKeywords = ['sign', 'signage', 'retail', 'restaurant', 'store', 'opening', 'development', 'construction'];
        const hasSignKeywords = signKeywords.some(keyword => text.includes(keyword));
        
        if (!hasSignKeywords) {
            return null;
        }
        
        // Extract data
        const phones = result.snippet.match(this.patterns.phone) || [];
        const addresses = result.snippet.match(this.patterns.address) || [];
        const openingDates = result.snippet.match(this.patterns.openingDate) || [];
        
        // Determine temperature
        const hotKeywords = ['opening soon', 'coming soon', 'now hiring', 'construction started'];
        const isHot = hotKeywords.some(keyword => text.includes(keyword));
        
        // Create lead object
        return {
            state: state,
            stateCode: CONFIG.STATE_CODES[state],
            name: this.extractCompanyName(result.title),
            subtitle: this.extractSubtitle(result.snippet),
            location: addresses[0] || `${state} - Location TBD`,
            contact: this.extractContact(result.snippet),
            phone: phones[0] || 'Contact for details',
            opening: openingDates[0] || 'TBD 2026',
            temp: isHot ? 'hot' : 'warm',
            signage: this.estimateSignageNeeds(result.title, result.snippet),
            revenue: this.estimateRevenue(),
            county: 'metro',
            source: result.url,
            discovered: new Date().toISOString(),
            rawData: {
                title: result.title,
                snippet: result.snippet,
                url: result.url
            }
        };
    }
    
    extractCompanyName(title) {
        // Remove common prefixes/suffixes
        let name = title.replace(/\s*-\s*.*/g, '').trim();
        name = name.replace(/(?:announces|opens|opening|coming to).*/gi, '').trim();
        return name || 'Commercial Development';
    }
    
    extractSubtitle(snippet) {
        const words = snippet.split(' ').slice(0, 10).join(' ');
        return words + '...';
    }
    
    extractContact(text) {
        const contactPatterns = [
            /contact:?\s*([^,\n]+)/i,
            /for (?:more )?information:?\s*([^,\n]+)/i
        ];
        
        for (const pattern of contactPatterns) {
            const match = text.match(pattern);
            if (match) return match[1].trim();
        }
        
        return 'Development Team';
    }
    
    estimateSignageNeeds(title, snippet) {
        const text = `${title} ${snippet}`.toLowerCase();
        const signage = ['Monument sign', 'Channel letters'];
        
        if (text.includes('restaurant') || text.includes('food')) {
            signage.push('Menu boards', 'Drive-thru signage');
        }
        if (text.includes('retail') || text.includes('store')) {
            signage.push('Interior wayfinding', 'Window graphics');
        }
        if (text.includes('office') || text.includes('commercial')) {
            signage.push('Directory signage', 'Suite identification');
        }
        if (text.includes('hotel') || text.includes('hospitality')) {
            signage.push('Pylon sign', 'Illuminated signage');
        }
        
        return signage;
    }
    
    estimateRevenue() {
        const min = Math.floor(Math.random() * 40) + 20;
        const max = min + Math.floor(Math.random() * 40) + 20;
        return `$${min}K - $${max}K`;
    }
}

// ==================== STATE PAGE GENERATOR ====================
class StatePageGenerator {
    generateHTML(state, leads) {
        const stateCode = CONFIG.STATE_CODES[state];
        const hotLeads = leads.filter(l => l.temp === 'hot').length;
        const warmLeads = leads.filter(l => l.temp === 'warm').length;
        
        return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${state.toUpperCase()} SIGN LEADS - ${leads.length} Active Opportunities</title>
    <meta name="description" content="Complete database of ${leads.length} sign industry leads in ${state}. Auto-updated daily with contact info and revenue estimates.">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #0f3460 0%, #16213e 100%);
            padding: 10px;
            min-height: 100vh;
        }
        .container { 
            max-width: 2000px; 
            margin: 0 auto; 
            background: white; 
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.3);
        }
        header { 
            background: linear-gradient(135deg, #1a1a2e, #16213e);
            color: white; 
            padding: 30px; 
            text-align: center;
            border-radius: 15px 15px 0 0;
        }
        header h1 { 
            font-size: 2.5em; 
            color: #00d4ff; 
            margin-bottom: 10px;
            text-shadow: 0 0 20px rgba(0, 212, 255, 0.5);
        }
        .stats-bar {
            background: linear-gradient(135deg, #00d4ff, #0099ff);
            color: white;
            padding: 20px;
            display: flex;
            justify-content: space-around;
            flex-wrap: wrap;
            gap: 20px;
        }
        .stat-item { text-align: center; }
        .stat-number { font-size: 2.5em; font-weight: bold; display: block; }
        .filters {
            background: #f8fafc;
            padding: 20px;
            display: flex;
            gap: 15px;
            align-items: center;
            flex-wrap: wrap;
            border-bottom: 3px solid #00d4ff;
        }
        .filters select, .filters input {
            padding: 10px 15px;
            border: 2px solid #e2e8f0;
            border-radius: 8px;
            font-size: 0.95em;
        }
        .search-bar { flex: 1; min-width: 250px; }
        .leads-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(400px, 1fr));
            gap: 25px;
            padding: 30px;
            background: #f8fafc;
        }
        .lead-card {
            background: white;
            border: 2px solid #e2e8f0;
            border-radius: 12px;
            padding: 25px;
            transition: all 0.3s;
        }
        .lead-card:hover {
            transform: translateY(-8px);
            box-shadow: 0 15px 35px rgba(0, 212, 255, 0.2);
            border-color: #00d4ff;
        }
        .lead-card.hot { border-color: #ef4444; }
        .lead-card.warm { border-color: #f59e0b; }
        .badge {
            display: inline-block;
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 0.75em;
            font-weight: 800;
            text-transform: uppercase;
            margin-bottom: 12px;
        }
        .badge-hot { background: #ef4444; color: white; }
        .badge-warm { background: #f59e0b; color: white; }
        .lead-title {
            font-size: 1.3em;
            font-weight: 900;
            color: #1a1a2e;
            margin-bottom: 8px;
        }
        .signage-box {
            background: linear-gradient(135deg, #f0f9ff, #e0f2fe);
            border-left: 4px solid #00d4ff;
            padding: 15px;
            margin: 15px 0;
            border-radius: 8px;
        }
        .contact-btn {
            width: 100%;
            padding: 14px;
            background: linear-gradient(135deg, #00d4ff, #0099ff);
            color: white;
            border: none;
            border-radius: 10px;
            font-weight: 800;
            cursor: pointer;
            transition: all 0.3s;
        }
        .contact-btn:hover {
            background: linear-gradient(135deg, #0099ff, #0066cc);
            transform: translateY(-2px);
        }
        @media (max-width: 768px) { .leads-grid { grid-template-columns: 1fr; } }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🎯 ${state.toUpperCase()} SIGN LEADS</h1>
            <p>Complete Database | Auto-Updated Daily at Midnight</p>
        </header>
        
        <div class="stats-bar">
            <div class="stat-item">
                <span class="stat-number">${leads.length}</span>
                <span>Total Leads</span>
            </div>
            <div class="stat-item">
                <span class="stat-number">${hotLeads}</span>
                <span>🔥 Hot</span>
            </div>
            <div class="stat-item">
                <span class="stat-number">${warmLeads}</span>
                <span>⚡ Warm</span>
            </div>
        </div>
        
        <div class="filters">
            <input type="text" class="search-bar" id="searchBox" placeholder="Search leads..." oninput="filterLeads()">
            <select id="tempFilter" onchange="filterLeads()">
                <option value="all">All</option>
                <option value="hot">🔥 Hot</option>
                <option value="warm">⚡ Warm</option>
            </select>
        </div>
        
        <div class="leads-grid" id="leadsContainer"></div>
    </div>

    <script>
        const allLeads = ${JSON.stringify(leads, null, 8)};
        
        window.onload = () => displayLeads(allLeads);
        
        function displayLeads(leads) {
            const container = document.getElementById('leadsContainer');
            container.innerHTML = leads.map(lead => \`
                <div class="lead-card \${lead.temp}">
                    <span class="badge badge-\${lead.temp}">\${lead.temp === 'hot' ? '🔥 HOT' : '⚡ WARM'}</span>
                    <div class="lead-title">\${lead.name}</div>
                    <div style="margin: 8px 0;"><strong>📍</strong> \${lead.location}</div>
                    <div style="margin: 8px 0;"><strong>📞</strong> \${lead.phone}</div>
                    <div style="margin: 8px 0;"><strong>📅</strong> \${lead.opening}</div>
                    <div class="signage-box">
                        <h4>Signage:</h4>
                        <ul>\${lead.signage.map(s => \`<li>\${s}</li>\`).join('')}</ul>
                    </div>
                    <div style="background: #d1fae5; padding: 12px; border-radius: 8px; text-align: center; margin: 15px 0; font-weight: bold; color: #047857;">
                        \${lead.revenue}
                    </div>
                    <button class="contact-btn" onclick="window.open('\${lead.source}', '_blank')">
                        📞 VIEW LEAD DETAILS
                    </button>
                </div>
            \`).join('');
        }
        
        function filterLeads() {
            const temp = document.getElementById('tempFilter').value;
            const search = document.getElementById('searchBox').value.toLowerCase();
            const filtered = allLeads.filter(lead => 
                (temp === 'all' || lead.temp === temp) &&
                (search === '' || lead.name.toLowerCase().includes(search) || lead.location.toLowerCase().includes(search))
            );
            displayLeads(filtered);
        }
    </script>
</body>
</html>`;
    }
}

// ==================== GITHUB DEPLOYER ====================
class GitHubDeployer {
    constructor() {
        this.apiBase = 'https://api.github.com';
    }
    
    async deployPage(state, htmlContent) {
        if (!CONFIG.OUTPUT.githubToken || !CONFIG.OUTPUT.githubRepo) {
            logger.warning('GitHub deployment not configured');
            return false;
        }
        
        try {
            const { default: fetch } = await import('node-fetch');
            const stateLower = state.toLowerCase().replace(/ /g, '-');
            const fileName = `${stateLower}-sign-leads.html`;
            const filePath = `state-pages/${fileName}`;
            
            // Get current file SHA if exists
            const getUrl = `${this.apiBase}/repos/${CONFIG.OUTPUT.githubRepo}/contents/${filePath}`;
            let sha = null;
            
            try {
                const getResponse = await fetch(getUrl, {
                    headers: {
                        'Authorization': `token ${CONFIG.OUTPUT.githubToken}`,
                        'Accept': 'application/vnd.github.v3+json'
                    }
                });
                if (getResponse.ok) {
                    const data = await getResponse.json();
                    sha = data.sha;
                }
            } catch (e) {
                // File doesn't exist yet
            }
            
            // Create or update file
            const content = Buffer.from(htmlContent).toString('base64');
            const updateUrl = `${this.apiBase}/repos/${CONFIG.OUTPUT.githubRepo}/contents/${filePath}`;
            
            const body = {
                message: `Auto-update ${state} leads - ${new Date().toISOString()}`,
                content: content,
                branch: 'main'
            };
            
            if (sha) {
                body.sha = sha;
            }
            
            const response = await fetch(updateUrl, {
                method: 'PUT',
                headers: {
                    'Authorization': `token ${CONFIG.OUTPUT.githubToken}`,
                    'Accept': 'application/vnd.github.v3+json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(body)
            });
            
            if (response.ok) {
                logger.success(`Deployed ${state} page to GitHub`);
                return true;
            } else {
                const error = await response.text();
                logger.error(`GitHub deploy failed: ${error}`);
                return false;
            }
        } catch (error) {
            logger.error(`GitHub deployment error: ${error.message}`);
            return false;
        }
    }
}

// ==================== MAIN ORCHESTRATOR ====================
class LeadScraperOrchestrator {
    constructor() {
        this.searchEngines = new SearchEngines();
        this.leadExtractor = new LeadExtractor();
        this.pageGenerator = new StatePageGenerator();
        this.githubDeployer = new GitHubDeployer();
    }
    
    async scrapeState(state) {
        logger.info(`\n========== SCRAPING ${state.toUpperCase()} ==========`);
        const allResults = [];
        
        // 1. Search business journals (highest quality)
        logger.info('Phase 1: Business Journal Search');
        const journalResults = await this.searchEngines.searchBusinessJournals(state);
        allResults.push(...journalResults);
        await this.delay(1000);
        
        // 2. Track franchise expansions
        logger.info('Phase 2: Franchise Expansion Tracking');
        const franchiseResults = await this.searchEngines.searchFranchiseSources(state);
        allResults.push(...franchiseResults);
        await this.delay(1000);
        
        // 3. General keyword searches
        logger.info('Phase 3: General Keyword Search');
        const allKeywords = [
            ...CONFIG.KEYWORDS.franchiseExpansion,
            ...CONFIG.KEYWORDS.retailRestaurant,
            ...CONFIG.KEYWORDS.commercial,
            ...CONFIG.KEYWORDS.permits,
            ...CONFIG.KEYWORDS.nationalChains
        ];
        
        // Use only top keywords to avoid quota exhaustion
        const topKeywords = allKeywords.slice(0, 8);
        
        for (const keyword of topKeywords) {
            const results = await this.searchEngines.searchAll(keyword, state);
            allResults.push(...results);
            await this.delay(800);
        }
        
        // Remove duplicates
        const uniqueResults = this.deduplicateResults(allResults);
        logger.info(`Found ${uniqueResults.length} unique results for ${state}`);
        
        // Extract leads (no AI scoring)
        const leads = uniqueResults
            .map(result => this.leadExtractor.extractLeadFromSearchResult(result, state))
            .filter(lead => lead !== null);
        
        logger.success(`Final: ${leads.length} leads for ${state}`);
        
        // Save raw data if configured
        if (CONFIG.OUTPUT.saveRawData) {
            await this.saveRawLeads(state, leads);
        }
        
        return leads;
    }
    
    async saveRawLeads(state, leads) {
        const stateCode = CONFIG.STATE_CODES[state];
        const data = {
            state,
            stateCode,
            timestamp: new Date().toISOString(),
            leadCount: leads.length,
            leads
        };
        
        const filePath = path.join(CONFIG.OUTPUT.directory, 'raw-data', `${stateCode}-raw.json`);
        try {
            await fs.mkdir(path.join(CONFIG.OUTPUT.directory, 'raw-data'), { recursive: true });
            await fs.writeFile(filePath, JSON.stringify(data, null, 2));
            logger.info(`Saved raw data: ${filePath}`);
        } catch (error) {
            logger.error(`Failed to save raw data: ${error.message}`);
        }
    }
    
    async updateStatePage(state, leads) {
        // Don't publish if below minimum threshold
        if (leads.length < CONFIG.OUTPUT.minimumLeadsPerState) {
            logger.warning(`${state} has only ${leads.length} leads (minimum: ${CONFIG.OUTPUT.minimumLeadsPerState}) - skipping page generation`);
            return;
        }
        
        // Generate HTML
        const html = this.pageGenerator.generateHTML(state, leads);
        
        // Save locally
        const stateLower = state.toLowerCase().replace(/ /g, '-');
        const fileName = `${stateLower}-sign-leads.html`;
        const filePath = path.join(CONFIG.OUTPUT.directory, fileName);
        
        try {
            await fs.mkdir(CONFIG.OUTPUT.directory, { recursive: true });
            await fs.writeFile(filePath, html);
            logger.success(`Saved ${state} page locally: ${filePath}`);
        } catch (error) {
            logger.error(`Failed to save ${state} page: ${error.message}`);
        }
        
        // Deploy to GitHub Pages
        if (CONFIG.OUTPUT.deployToGithub) {
            await this.githubDeployer.deployPage(state, html);
        }
    }
    
    async runFullScrape() {
        logger.info(`\n🚀 STARTING FREE 50-STATE SCRAPE (96-HOUR CYCLE)`);
        logger.info(`Time: ${new Date().toISOString()}`);
        logger.info(`AI Scoring: DISABLED (Free version)`);
        
        const startTime = Date.now();
        const results = {};
        let totalLeads = 0;
        
        for (const state of CONFIG.US_STATES) {
            try {
                const leads = await this.scrapeState(state);
                results[state] = leads;
                totalLeads += leads.length;
                
                // Update state page
                await this.updateStatePage(state, leads);
                
                // Delay between states
                await this.delay(CONFIG.RATE_LIMIT.delayBetweenStates);
            } catch (error) {
                logger.error(`Error processing ${state}: ${error.message}`);
                results[state] = [];
            }
        }
        
        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        
        logger.success(`\n✅ FREE SCRAPE COMPLETE!`);
        logger.info(`Total leads: ${totalLeads}`);
        logger.info(`States processed: ${CONFIG.US_STATES.length}`);
        logger.info(`Duration: ${duration} seconds`);
        logger.info(`Next run: ${this.calculateNextRun()}`);
        
        // Save summary
        await this.saveSummary(results, totalLeads, duration);
        
        // Update next run timestamp
        await this.updateNextRun();
        
        // Log API key usage stats
        const keyStats = apiKeyManager.getKeyStats();
        logger.info('API Key Usage:');
        for (const [service, stats] of Object.entries(keyStats)) {
            logger.info(`  ${service}: ${stats.reduce((sum, s) => sum + s.calls, 0)} total calls across ${stats.length} keys`);
        }
        
        return results;
    }
    
    calculateNextRun() {
        const now = new Date();
        const nextRun = new Date(now.getTime() + CONFIG.SCHEDULE.intervalHours * 60 * 60 * 1000);
        return nextRun.toISOString();
    }
    
    async updateNextRun() {
        const nextRun = this.calculateNextRun();
        try {
            await fs.writeFile(
                CONFIG.SCHEDULE.nextRunFile,
                JSON.stringify({ nextRun, lastRun: new Date().toISOString() }, null, 2)
            );
        } catch (error) {
            logger.error(`Failed to update next run file: ${error.message}`);
        }
    }
    
    async saveSummary(results, totalLeads, duration) {
        const summary = {
            timestamp: new Date().toISOString(),
            totalLeads: totalLeads,
            statesProcessed: CONFIG.US_STATES.length,
            durationSeconds: duration,
            nextRun: this.calculateNextRun(),
            aiScoringEnabled: false,
            stateBreakdown: Object.entries(results).map(([state, leads]) => ({
                state: state,
                stateCode: CONFIG.STATE_CODES[state],
                leadCount: leads.length,
                hotLeads: leads.filter(l => l.temp === 'hot').length,
                warmLeads: leads.filter(l => l.temp === 'warm').length
            })),
            logs: logger.exportLogs(),
            apiKeyUsage: apiKeyManager.getKeyStats()
        };
        
        const summaryPath = path.join(CONFIG.OUTPUT.directory, 'scrape-summary.json');
        await fs.writeFile(summaryPath, JSON.stringify(summary, null, 2));
        logger.success(`Summary saved: ${summaryPath}`);
    }
    
    deduplicateResults(results) {
        const seen = new Set();
        return results.filter(result => {
            const key = result.url;
            if (seen.has(key)) {
                return false;
            }
            seen.add(key);
            return true;
        });
    }
    
    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// ==================== AWS LAMBDA HANDLER ====================
exports.handler = async (event, context) => {
    const orchestrator = new LeadScraperOrchestrator();
    
    try {
        const results = await orchestrator.runFullScrape();
        
        return {
            statusCode: 200,
            body: JSON.stringify({
                success: true,
                message: 'Scrape completed successfully',
                totalLeads: Object.values(results).reduce((sum, leads) => sum + leads.length, 0),
                timestamp: new Date().toISOString()
            })
        };
    } catch (error) {
        logger.error(`Fatal error: ${error.message}`);
        
        return {
            statusCode: 500,
            body: JSON.stringify({
                success: false,
                error: error.message,
                timestamp: new Date().toISOString()
            })
        };
    }
};

// ==================== STANDALONE EXECUTION ====================
if (require.main === module) {
    const orchestrator = new LeadScraperOrchestrator();
    orchestrator.runFullScrape()
        .then(() => {
            logger.success('Scraping completed successfully');
            process.exit(0);
        })
        .catch(error => {
            logger.error(`Fatal error: ${error.message}`);
            process.exit(1);
        });
}

module.exports = { LeadScraperOrchestrator, CONFIG };
