// ============================================================
// SIGN LEADS SCRAPER — FULL DIAGNOSTIC
// Run this in GitHub Actions to find EXACTLY what's failing
//
// Add a temporary workflow step:
//   - name: Run diagnostic
//     run: node diagnose.js
//     env:
//       GOOGLE_API_KEYS: ${{ secrets.GOOGLE_API_KEYS }}
//       GOOGLE_SEARCH_ENGINE_IDS: ${{ secrets.GOOGLE_SEARCH_ENGINE_IDS }}
// ============================================================

const ENDPOINT = 'https://www.googleapis.com/customsearch/v1';

async function run() {
  const { default: fetch } = await import('node-fetch');

  const keys = (process.env.GOOGLE_API_KEYS || process.env.GOOGLE_API_KEY || '')
    .split(',').map(k => k.trim()).filter(Boolean);
  const cxs  = (process.env.GOOGLE_SEARCH_ENGINE_IDS || process.env.GOOGLE_SEARCH_ENGINE_ID || '')
    .split(',').map(k => k.trim()).filter(Boolean);

  console.log('\n============================================================');
  console.log('SIGN LEADS SCRAPER — DIAGNOSTIC REPORT');
  console.log('============================================================');
  console.log(`Time:        ${new Date().toISOString()}`);
  console.log(`Keys loaded: ${keys.length}`);
  console.log(`CX IDs loaded: ${cxs.length}`);

  // ── STEP 1: ENV VARS ──────────────────────────────────────
  console.log('\n─── STEP 1: Environment Variables ───');
  if (keys.length === 0) {
    console.log('❌ FAIL: GOOGLE_API_KEYS is empty or not set');
    console.log('   Fix: Add GOOGLE_API_KEYS to GitHub repository secrets');
    process.exit(1);
  }
  if (cxs.length === 0) {
    console.log('❌ FAIL: GOOGLE_SEARCH_ENGINE_IDS is empty or not set');
    console.log('   Fix: Add GOOGLE_SEARCH_ENGINE_IDS to GitHub repository secrets');
    process.exit(1);
  }
  console.log(`✅ Keys: ${keys.length} found`);
  console.log(`✅ CX IDs: ${cxs.length} found`);
  keys.forEach((k, i) => console.log(`   Key ${i+1}: ${k.substring(0,14)}...`));
  cxs.forEach((c, i) => console.log(`   CX  ${i+1}: ${c}`));

  // ── STEP 2: NETWORK ──────────────────────────────────────
  console.log('\n─── STEP 2: Network Connectivity ───');
  try {
    const r = await fetch('https://www.googleapis.com/', { timeout: 8000 });
    console.log(`✅ Google reachable — HTTP ${r.status}`);
  } catch (err) {
    console.log(`❌ FAIL: Cannot reach Google — ${err.message}`);
    console.log('   Fix: Check GitHub Actions network/firewall settings');
    process.exit(1);
  }

  // ── STEP 3: TEST EACH KEY ─────────────────────────────────
  console.log('\n─── STEP 3: API Key Validation (testing all keys) ───');
  const workingKeys = [];

  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    const cx  = cxs[i % cxs.length];
    const url = new URL(ENDPOINT);
    url.searchParams.set('key', key);
    url.searchParams.set('cx', cx);
    url.searchParams.set('q', 'new store opening');
    url.searchParams.set('num', '1');

    try {
      const res  = await fetch(url.toString(), { timeout: 10000 });
      const data = await res.json();

      if (data.error) {
        const code = data.error.code;
        const msg  = data.error.message;
        if (code === 400) {
          console.log(`❌ Key ${i+1}: INVALID KEY OR CX ID [400] — ${msg}`);
          console.log(`   Key: ${key.substring(0,20)}...`);
          console.log(`   CX:  ${cx}`);
          console.log(`   Fix: Check this key is enabled for Custom Search API in Google Cloud Console`);
          console.log(`        AND check the CX ID matches a real CSE`);
        } else if (code === 403) {
          console.log(`❌ Key ${i+1}: API NOT ENABLED [403] — ${msg}`);
          console.log(`   Fix: Go to console.cloud.google.com → APIs → Enable "Custom Search API"`);
        } else if (code === 429) {
          console.log(`⚠️  Key ${i+1}: QUOTA EXCEEDED [429] — daily limit hit`);
        } else {
          console.log(`❌ Key ${i+1}: ERROR [${code}] — ${msg}`);
        }
      } else {
        const count = (data.items || []).length;
        const total = data.searchInformation?.totalResults || '0';
        if (count > 0) {
          console.log(`✅ Key ${i+1}: WORKING — ${count} results (${total} total on Google)`);
          console.log(`   First result: "${data.items[0].title?.substring(0, 60)}"`);
          workingKeys.push({ key, cx, index: i+1 });
        } else {
          console.log(`⚠️  Key ${i+1}: API OK but 0 results returned`);
          console.log(`   Total results on Google: ${total}`);
          if (total === '0') {
            console.log(`   ❌ CSE IS NOT FINDING ANYTHING`);
            console.log(`   Possible causes:`);
            console.log(`   1. Sites not yet indexed — wait 24hrs after adding them`);
            console.log(`   2. Sites added in wrong format (must be *.domain.com)`);
            console.log(`   3. CX ID ${cx} doesn't match the CSE you added sites to`);
          }
          workingKeys.push({ key, cx, index: i+1, zeroResults: true });
        }
      }
    } catch (err) {
      console.log(`❌ Key ${i+1}: NETWORK ERROR — ${err.message}`);
    }

    await new Promise(r => setTimeout(r, 400));
  }

  // ── STEP 4: CSE CONTENT TEST ──────────────────────────────
  console.log('\n─── STEP 4: CSE Content Test (what sites are being searched) ───');
  if (workingKeys.length === 0) {
    console.log('❌ No working keys — skipping content test');
  } else {
    const { key, cx } = workingKeys[0];

    // Test with a query that SHOULD return results from major news sites
    const testQueries = [
      { q: 'restaurant opening', expect: 'local news' },
      { q: 'store opening',      expect: 'retail news' },
      { q: 'grand opening',      expect: 'press releases' },
    ];

    for (const { q, expect } of testQueries) {
      const url = new URL(ENDPOINT);
      url.searchParams.set('key', key);
      url.searchParams.set('cx', cx);
      url.searchParams.set('q', q);
      url.searchParams.set('num', '5');

      const res  = await fetch(url.toString(), { timeout: 10000 });
      const data = await res.json();

      if (data.error) {
        console.log(`❌ "${q}" — API error: ${data.error.message}`);
        continue;
      }

      const items = data.items || [];
      console.log(`\n  Query: "${q}" (expecting ${expect})`);
      console.log(`  Results: ${items.length} items`);

      if (items.length === 0) {
        console.log(`  ❌ ZERO RESULTS — your CSE sites aren't indexed yet`);
        console.log(`     OR the sites you added don't contain "${q}" content`);
      } else {
        items.forEach((item, i) => {
          const domain = new URL(item.link).hostname;
          console.log(`  ${i+1}. [${domain}] ${item.title?.substring(0, 55)}`);
        });
      }

      await new Promise(r => setTimeout(r, 400));
    }

    // ── STEP 5: EXTRACT LEAD TEST ─────────────────────────
    console.log('\n─── STEP 5: Lead Extraction Test ───');
    const url = new URL(ENDPOINT);
    url.searchParams.set('key', key);
    url.searchParams.set('cx', cx);
    url.searchParams.set('q', 'new restaurant opening Texas');
    url.searchParams.set('num', '5');

    const res  = await fetch(url.toString(), { timeout: 10000 });
    const data = await res.json();
    const items = data.items || [];

    if (items.length === 0) {
      console.log('❌ No results to test extraction on');
    } else {
      let accepted = 0, rejected = 0;
      const relevantTerms = [
        'opening','opens','open soon','grand opening','coming soon',
        'new location','new store','new restaurant','new franchise',
        'construction','development','retail','restaurant','store',
        'franchise','expansion','permit','plaza','shopping center',
        'strip mall','commercial','build','tenant','lease','relocat'
      ];
      items.forEach(item => {
        const combined = `${item.title} ${item.snippet}`.toLowerCase();
        const isRelevant = relevantTerms.some(t => combined.includes(t));
        if (isRelevant) {
          console.log(`  ✅ ACCEPTED: "${item.title?.substring(0, 60)}"`);
          accepted++;
        } else {
          console.log(`  ❌ REJECTED: "${item.title?.substring(0, 60)}"`);
          console.log(`     (no relevant terms found in title+snippet)`);
          rejected++;
        }
      });
      console.log(`\n  Accepted: ${accepted} | Rejected: ${rejected}`);
    }
  }

  // ── SUMMARY ──────────────────────────────────────────────
  console.log('\n============================================================');
  console.log('DIAGNOSTIC SUMMARY');
  console.log('============================================================');
  const working = workingKeys.filter(k => !k.zeroResults).length;
  const zeroRes = workingKeys.filter(k => k.zeroResults).length;

  if (working > 0) {
    console.log(`✅ ${working} key(s) working and returning results`);
    console.log('   → Scraper should be finding leads');
    console.log('   → Check extractLead filter is not rejecting everything');
  } else if (zeroRes > 0) {
    console.log(`⚠️  ${zeroRes} key(s) valid but returning 0 results from CSE`);
    console.log('   → MOST LIKELY CAUSE: Sites not yet indexed by Google');
    console.log('   → ACTIONS NEEDED:');
    console.log('     1. Verify sites were added to the CORRECT CSE (matching CX ID)');
    console.log('     2. Wait 24 hours after adding sites — indexing takes time');
    console.log('     3. Check sites were added as *.domain.com format');
    console.log('     4. Go to CSE → Test tab and search manually to verify');
  } else {
    console.log('❌ No working keys found');
    console.log('   → Check API key is enabled in Google Cloud Console');
    console.log('   → Check Custom Search API is enabled for the project');
    console.log('   → Verify CX IDs match your actual CSE IDs');
  }
  console.log('============================================================\n');
}

run().catch(err => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
