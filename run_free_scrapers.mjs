/**
 * run_free_scrapers.mjs
 *
 * Launches N parallel workers that divide ALL US cities evenly between them.
 * Every worker covers its slice of the country — no cities are skipped.
 *
 * Usage:
 *   node run_free_scrapers.mjs              # 2 workers (safe default for home PC)
 *   node run_free_scrapers.mjs --workers 4  # 4 workers, all 160+ cities split 4 ways
 *   node run_free_scrapers.mjs --workers 8  # 8 workers (needs 16GB+ RAM)
 *   node run_free_scrapers.mjs texas        # single-region filter (debug/test)
 *
 * Resource guide:
 *   1 worker  → ~400MB RAM   (safest on home network)
 *   2 workers → ~800MB RAM   ← DEFAULT
 *   4 workers → ~1.5GB RAM   (good overnight run, needs 8GB+ free RAM)
 *   8 workers → ~3GB RAM     (max throughput, needs 16GB+ RAM)
 */

import { config } from 'dotenv';
config();

import { readFileSync }    from 'fs';
import { FreeScraperNode } from './nodes/free_scraper_node.mjs';
import { log }             from './core/logger.mjs';

// ── Load profile ──────────────────────────────────────────────────────────────
const profile    = JSON.parse(readFileSync('./profile.json', 'utf8'));
const industries = profile.icp.industries;

// ── All US cities in geographic order (east → west) ──────────────────────────
const ALL_CITIES = [
  // Texas
  'San Antonio, TX', 'Houston, TX', 'Dallas, TX', 'Fort Worth, TX', 'Austin, TX',
  'Corpus Christi, TX', 'Lubbock, TX', 'El Paso, TX', 'Arlington, TX', 'Plano, TX',
  'Laredo, TX', 'Irving, TX', 'Amarillo, TX', 'Garland, TX', 'Frisco, TX',
  'McKinney, TX', 'Waco, TX', 'Killeen, TX', 'Beaumont, TX', 'Midland, TX',
  // Florida
  'Jacksonville, FL', 'Tampa, FL', 'Orlando, FL', 'Miami, FL', 'Fort Lauderdale, FL',
  'St. Petersburg, FL', 'Cape Coral, FL', 'Clearwater, FL', 'Lakeland, FL',
  'Port St. Lucie, FL', 'Sarasota, FL', 'Pensacola, FL', 'Tallahassee, FL',
  'Gainesville, FL', 'Daytona Beach, FL', 'Boca Raton, FL', 'West Palm Beach, FL',
  'Pompano Beach, FL', 'Hialeah, FL', 'Miramar, FL',
  // Southeast
  'Nashville, TN', 'Memphis, TN', 'Knoxville, TN', 'Chattanooga, TN',
  'Birmingham, AL', 'Huntsville, AL', 'Montgomery, AL', 'Mobile, AL',
  'Columbia, SC', 'Charleston, SC', 'Greenville, SC',
  'Atlanta, GA', 'Savannah, GA', 'Augusta, GA', 'Columbus, GA', 'Macon, GA',
  'Jackson, MS', 'Gulfport, MS', 'Baton Rouge, LA', 'New Orleans, LA',
  'Shreveport, LA', 'Lafayette, LA', 'Little Rock, AR', 'Fayetteville, AR',
  // Northeast
  'New York, NY', 'Buffalo, NY', 'Rochester, NY', 'Yonkers, NY',
  'Philadelphia, PA', 'Pittsburgh, PA', 'Allentown, PA',
  'Newark, NJ', 'Jersey City, NJ', 'Trenton, NJ',
  'Boston, MA', 'Worcester, MA', 'Springfield, MA',
  'Providence, RI', 'Hartford, CT', 'Bridgeport, CT', 'New Haven, CT',
  'Baltimore, MD', 'Frederick, MD',
  'Wilmington, DE', 'Manchester, NH', 'Portland, ME', 'Burlington, VT',
  // Mid-Atlantic & Midwest
  'Charlotte, NC', 'Raleigh, NC', 'Durham, NC', 'Greensboro, NC', 'Winston-Salem, NC',
  'Virginia Beach, VA', 'Norfolk, VA', 'Chesapeake, VA', 'Richmond, VA', 'Arlington, VA',
  'Louisville, KY', 'Lexington, KY',
  'Indianapolis, IN', 'Fort Wayne, IN', 'Evansville, IN',
  'Columbus, OH', 'Cincinnati, OH', 'Cleveland, OH', 'Toledo, OH', 'Akron, OH',
  'Dayton, OH', 'Canton, OH',
  // Great Lakes
  'Chicago, IL', 'Aurora, IL', 'Naperville, IL', 'Rockford, IL', 'Joliet, IL',
  'Detroit, MI', 'Grand Rapids, MI', 'Warren, MI', 'Sterling Heights, MI', 'Ann Arbor, MI',
  'Milwaukee, WI', 'Madison, WI', 'Green Bay, WI',
  'Minneapolis, MN', 'Saint Paul, MN', 'Rochester, MN',
  'Kansas City, MO', 'St. Louis, MO', 'Springfield, MO',
  'Omaha, NE', 'Lincoln, NE',
  'Des Moines, IA', 'Cedar Rapids, IA',
  'Sioux Falls, SD', 'Fargo, ND',
  // Mountain & Southwest
  'Phoenix, AZ', 'Tucson, AZ', 'Mesa, AZ', 'Scottsdale, AZ', 'Chandler, AZ',
  'Gilbert, AZ', 'Tempe, AZ', 'Peoria, AZ',
  'Denver, CO', 'Colorado Springs, CO', 'Aurora, CO', 'Fort Collins, CO', 'Boulder, CO',
  'Las Vegas, NV', 'Henderson, NV', 'Reno, NV', 'North Las Vegas, NV',
  'Salt Lake City, UT', 'West Valley City, UT', 'Provo, UT', 'Ogden, UT',
  'Albuquerque, NM', 'Santa Fe, NM', 'Las Cruces, NM',
  'Boise, ID', 'Nampa, ID',
  // West Coast
  'Los Angeles, CA', 'San Diego, CA', 'San Jose, CA', 'San Francisco, CA',
  'Fresno, CA', 'Sacramento, CA', 'Long Beach, CA', 'Oakland, CA',
  'Bakersfield, CA', 'Riverside, CA', 'Anaheim, CA', 'Stockton, CA',
  'Chula Vista, CA', 'Irvine, CA', 'Santa Ana, CA',
  'Seattle, WA', 'Spokane, WA', 'Tacoma, WA', 'Bellevue, WA', 'Vancouver, WA',
  'Portland, OR', 'Eugene, OR', 'Salem, OR',
  'Anchorage, AK', 'Honolulu, HI',
];

// ── Parse CLI args ────────────────────────────────────────────────────────────
const args       = process.argv.slice(2);
const workerFlag = args.indexOf('--workers');
const numWorkers = workerFlag !== -1 ? parseInt(args[workerFlag + 1], 10) : 1;

// Optional single-region debug filter (e.g. "texas" filters to TX cities only)
const STATE_FILTERS = {
  texas:             c => c.endsWith(', TX'),
  florida:           c => c.endsWith(', FL'),
  southeast:         c => [', TN', ', AL', ', SC', ', GA', ', MS', ', LA', ', AR'].some(s => c.endsWith(s)),
  northeast:         c => [', NY', ', PA', ', NJ', ', MA', ', RI', ', CT', ', MD', ', DE', ', NH', ', ME', ', VT'].some(s => c.endsWith(s)),
  mid_atlantic:      c => [', NC', ', VA', ', KY', ', IN', ', OH'].some(s => c.endsWith(s)),
  great_lakes:       c => [', IL', ', MI', ', WI', ', MN', ', MO', ', NE', ', IA', ', SD', ', ND'].some(s => c.endsWith(s)),
  mountain_southwest:c => [', AZ', ', CO', ', NV', ', UT', ', NM', ', ID'].some(s => c.endsWith(s)),
  west_coast:        c => [', CA', ', WA', ', OR', ', AK', ', HI'].some(s => c.endsWith(s)),
};

const filterKey  = args.find(a => STATE_FILTERS[a] || a === 'all') ?? null;
const citiesToRun = filterKey && filterKey !== 'all'
  ? ALL_CITIES.filter(STATE_FILTERS[filterKey])
  : ALL_CITIES;

// ── Divide cities evenly across workers ──────────────────────────────────────
const chunkSize = Math.ceil(citiesToRun.length / numWorkers);
const workers   = Array.from({ length: numWorkers }, (_, i) => ({
  id:     i + 1,
  cities: citiesToRun.slice(i * chunkSize, (i + 1) * chunkSize),
})).filter(w => w.cities.length > 0);

log.info({
  event:      'scraper_launch',
  workers:    workers.length,
  total_cities: citiesToRun.length,
  cities_per_worker: chunkSize,
  industries: industries.length,
});

console.log(`
╔══════════════════════════════════════════════════════╗
║          FREE SCRAPER — DIVIDE & CONQUER MODE        ║
╠══════════════════════════════════════════════════════╣
║  Workers       : ${String(workers.length).padEnd(33)} ║
║  Total cities  : ${String(citiesToRun.length).padEnd(33)} ║
║  Cities/worker : ${String(chunkSize).padEnd(33)} ║
║  Industries    : ${String(industries.length).padEnd(33)} ║
║                                                      ║
║  Email sending : DISABLED (scrape only)              ║
║  Stop with     : Ctrl+C                              ║
╚══════════════════════════════════════════════════════╝
`);

workers.forEach(w =>
  console.log(`  Worker ${w.id}: ${w.cities[0]} → ${w.cities[w.cities.length - 1]} (${w.cities.length} cities)`)
);
console.log('');

// ── Launch all workers concurrently ──────────────────────────────────────────
const nodes = workers.map(({ id, cities }) => {
  const node = new FreeScraperNode(
    {
      node_id:             `free-scraper-${id}`,
      region_label:        `Worker ${id}`,
      target_industries:   industries,
      target_locations:    cities,
      sources:             ['bing', 'bbb', 'google_maps'],
      daily_cap_per_city:  500,
      cycle_sleep:         30,
      allow_self_destruct: false,
      queue_url:           process.env.SWARM_SCRAPER_QUEUE_URL ?? '',
    },
    process.env.AWS_REGION ?? 'us-east-1',
  );
  return { id, node };
});

await Promise.allSettled(
  nodes.map(({ id, node }) =>
    node.start().catch(err => {
      log.error({ event: 'worker_crashed', worker: id, error: err.message });
    }),
  ),
);
