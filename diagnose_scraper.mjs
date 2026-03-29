/**
 * diagnose_scraper.mjs — quick test of each scraper source
 * Usage: node diagnose_scraper.mjs
 */
import 'dotenv/config';
import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import { writeFileSync } from 'fs';

puppeteer.use(StealthPlugin());

const CITY     = 'Atlanta, GA';
const INDUSTRY = 'Dental Offices';

const sleep = ms => new Promise(r => setTimeout(r, ms));

const browser = await puppeteer.launch({
  headless: 'new',
  args: [
    '--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
    '--disable-gpu', '--disable-blink-features=AutomationControlled',
    '--window-size=1366,768',
  ],
  ignoreDefaultArgs: ['--enable-automation'],
});

async function shot(page, name) {
  await page.screenshot({ path: `diag_${name}.png`, fullPage: false });
  console.log(`  📸 screenshot saved: diag_${name}.png`);
}

async function testYelp() {
  console.log('\n── YELP ─────────────────────────────────────────────');
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
  const url = `https://www.yelp.com/search?find_desc=dentists&find_loc=${encodeURIComponent(CITY)}`;
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 20_000 });
    await sleep(3000);
    const title = await page.title();
    console.log(`  Title: ${title}`);
    await shot(page, 'yelp');
    const count = await page.evaluate(() =>
      document.querySelectorAll('[data-testid="serp-ia-card"], li[class*="result"]').length
    );
    console.log(`  Cards found with current selector: ${count}`);
    // Try alternate selectors
    const alt = await page.evaluate(() => ({
      cards: document.querySelectorAll('[class*="businessName"]').length,
      h3:    document.querySelectorAll('h3 a[href*="/biz/"]').length,
      total: document.querySelectorAll('a[href*="/biz/"]').length,
    }));
    console.log(`  Alt selectors — businessName: ${alt.cards}  h3>a[biz]: ${alt.h3}  any[biz]: ${alt.total}`);
  } catch (e) { console.log(`  ERROR: ${e.message}`); }
  await page.close();
}

async function testBBB() {
  console.log('\n── BBB ──────────────────────────────────────────────');
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
  const url = `https://www.bbb.org/search?find_text=dental+office&find_loc=${encodeURIComponent(CITY)}`;
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 20_000 });
    await sleep(3000);
    const title = await page.title();
    console.log(`  Title: ${title}`);
    await shot(page, 'bbb');
    const count = await page.evaluate(() =>
      document.querySelectorAll('[class*="search-result"], .result-block').length
    );
    console.log(`  Cards with current selector: ${count}`);
    const alt = await page.evaluate(() => ({
      cards:   document.querySelectorAll('[class*="SearchResult"]').length,
      names:   document.querySelectorAll('[class*="businessName"], [class*="business-name"]').length,
      links:   document.querySelectorAll('a[href*="/profile/"]').length,
      headers: document.querySelectorAll('h2, h3').length,
    }));
    console.log(`  Alt — SearchResult: ${alt.cards}  businessName: ${alt.names}  /profile/ links: ${alt.links}  h2/h3: ${alt.headers}`);
  } catch (e) { console.log(`  ERROR: ${e.message}`); }
  await page.close();
}

async function testGoogleOrganic() {
  console.log('\n── GOOGLE ORGANIC ───────────────────────────────────');
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
  const query = `dental offices in ${CITY}`;
  const url = `https://www.google.com/search?q=${encodeURIComponent(query)}&num=20&gl=us&hl=en`;
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 20_000 });
    await sleep(3000);
    const title = await page.title();
    console.log(`  Title: ${title}`);
    await shot(page, 'google');
    const count = await page.evaluate(() =>
      document.querySelectorAll('#search a[href], #rso a[href], .yuRUbf a').length
    );
    console.log(`  Links with current selector: ${count}`);
    const alt = await page.evaluate(() => ({
      rso:     document.querySelectorAll('#rso a[href^="http"]').length,
      h3:      document.querySelectorAll('h3').length,
      captcha: document.querySelector('#captcha, form[action*="CaptchaRedirect"]') ? 'YES' : 'NO',
      consent: document.querySelector('[id*="consent"], form[action*="consent"]') ? 'YES' : 'NO',
    }));
    console.log(`  Alt — #rso http links: ${alt.rso}  h3s: ${alt.h3}  CAPTCHA: ${alt.captcha}  Consent: ${alt.consent}`);
  } catch (e) { console.log(`  ERROR: ${e.message}`); }
  await page.close();
}

async function testGoogleMaps() {
  console.log('\n── GOOGLE MAPS ──────────────────────────────────────');
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
  const query = `Dental Offices near ${CITY}`;
  const url   = `https://www.google.com/maps/search/${encodeURIComponent(query)}`;
  try {
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 30_000 });
    await sleep(4000);
    const title = await page.title();
    console.log(`  Title: ${title}`);
    await shot(page, 'maps');
    const count = await page.evaluate(() =>
      document.querySelectorAll('[role="feed"] a[href*="/maps/place/"], div.Nv2PK a').length
    );
    console.log(`  Listing cards with current selector: ${count}`);
    const alt = await page.evaluate(() => ({
      feed:    document.querySelectorAll('[role="feed"] a').length,
      place:   document.querySelectorAll('a[href*="/maps/place/"]').length,
      Nv2PK:   document.querySelectorAll('div.Nv2PK').length,
      cards:   document.querySelectorAll('[role="article"]').length,
    }));
    console.log(`  Alt — feed links: ${alt.feed}  place hrefs: ${alt.place}  Nv2PK divs: ${alt.Nv2PK}  articles: ${alt.cards}`);
  } catch (e) { console.log(`  ERROR: ${e.message}`); }
  await page.close();
}

async function testDirectSite() {
  console.log('\n── DIRECT SITE SCRAPE (atlantasmiles.com) ───────────');
  const page = await browser.newPage();
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36');
  try {
    await page.goto('https://www.atlantasmiles.com', { waitUntil: 'domcontentloaded', timeout: 15_000 });
    await sleep(2000);
    const data = await page.evaluate(() => {
      const re = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;
      const emails = [...new Set(document.body?.innerText?.match(re) ?? [])];
      const phones = document.body?.innerText?.match(/\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}/g) ?? [];
      return { emails, phone: phones[0] ?? null, title: document.title };
    });
    console.log(`  Title: ${data.title}`);
    console.log(`  Emails found: ${data.emails.join(', ') || '(none)'}`);
    console.log(`  Phone: ${data.phone ?? '(none)'}`);
    await shot(page, 'direct_site');
  } catch (e) { console.log(`  ERROR: ${e.message}`); }
  await page.close();
}

console.log(`\n🔍 Scraper Diagnostics — ${CITY} / ${INDUSTRY}`);
console.log('Screenshots will be saved as diag_*.png\n');

await testYelp();
await testBBB();
await testGoogleOrganic();
await testGoogleMaps();
await testDirectSite();

await browser.close();
console.log('\n✅ Done. Check the diag_*.png screenshots to see what the browser saw.');
