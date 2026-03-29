/**
 * run_free_scrapers.mjs
 *
 * Launches parallel FreeScraperNode instances — one per US region.
 * Each instance owns a slice of cities so they don't overlap.
 *
 * Usage:
 *   node run_free_scrapers.mjs            # 2 workers (safe default for home PC)
 *   node run_free_scrapers.mjs texas      # one region only
 *   node run_free_scrapers.mjs --workers 4  # more workers if you have 16GB+ RAM
 *
 * Resource guide:
 *   1 worker  → ~400MB RAM, ~2-3Mbps     (safest on home network)
 *   2 workers → ~800MB RAM, ~5Mbps       ← DEFAULT (home PC safe)
 *   4 workers → ~1.5GB RAM, ~10Mbps      (needs 8GB+ free RAM)
 *   8 workers → ~3GB RAM,   ~20Mbps      (needs 16GB+ RAM, will OOM on most home PCs)
 */

import { config } from 'dotenv';
config();

import { readFileSync }     from 'fs';
import { FreeScraperNode }  from './nodes/free_scraper_node.mjs';
import { log }              from './core/logger.mjs';

// ── Load profile ─────────────────────────────────────────────────────────────
const profile    = JSON.parse(readFileSync('./profile.json', 'utf8'));
const industries = profile.icp.industries;
const allCities  = profile.icp.locations;

// ── Split cities into 8 regional buckets (one worker each) ──────────────────
const REGIONS = {
  texas: [
    'San Antonio, TX', 'Houston, TX', 'Dallas, TX', 'Fort Worth, TX', 'Austin, TX',
    'Corpus Christi, TX', 'Lubbock, TX', 'El Paso, TX', 'Arlington, TX', 'Plano, TX',
    'Laredo, TX', 'Irving, TX', 'Amarillo, TX', 'Garland, TX', 'Frisco, TX',
    'McKinney, TX', 'Waco, TX', 'Killeen, TX', 'Beaumont, TX', 'Midland, TX',
  ],
  florida_south: [
    'Jacksonville, FL', 'Tampa, FL', 'Orlando, FL', 'Miami, FL', 'Fort Lauderdale, FL',
    'St. Petersburg, FL', 'Cape Coral, FL', 'Clearwater, FL', 'Lakeland, FL',
    'Port St. Lucie, FL', 'Sarasota, FL', 'Pensacola, FL', 'Tallahassee, FL',
    'Gainesville, FL', 'Daytona Beach, FL', 'Boca Raton, FL', 'West Palm Beach, FL',
    'Pompano Beach, FL', 'Hialeah, FL', 'Miramar, FL',
  ],
  southeast: [
    'Nashville, TN', 'Memphis, TN', 'Knoxville, TN', 'Chattanooga, TN',
    'Birmingham, AL', 'Huntsville, AL', 'Montgomery, AL', 'Mobile, AL',
    'Columbia, SC', 'Charleston, SC', 'Greenville, SC',
    'Atlanta, GA', 'Savannah, GA', 'Augusta, GA', 'Columbus, GA', 'Macon, GA',
    'Jackson, MS', 'Gulfport, MS', 'Baton Rouge, LA', 'New Orleans, LA',
    'Shreveport, LA', 'Lafayette, LA', 'Little Rock, AR', 'Fayetteville, AR',
  ],
  northeast: [
    'New York, NY', 'Buffalo, NY', 'Rochester, NY', 'Yonkers, NY',
    'Philadelphia, PA', 'Pittsburgh, PA', 'Allentown, PA',
    'Newark, NJ', 'Jersey City, NJ', 'Trenton, NJ',
    'Boston, MA', 'Worcester, MA', 'Springfield, MA',
    'Providence, RI', 'Hartford, CT', 'Bridgeport, CT', 'New Haven, CT',
    'Baltimore, MD', 'Frederick, MD',
    'Wilmington, DE', 'Manchester, NH', 'Portland, ME', 'Burlington, VT',
  ],
  mid_atlantic: [
    'Charlotte, NC', 'Raleigh, NC', 'Durham, NC', 'Greensboro, NC', 'Winston-Salem, NC',
    'Virginia Beach, VA', 'Norfolk, VA', 'Chesapeake, VA', 'Richmond, VA', 'Arlington, VA',
    'Louisville, KY', 'Lexington, KY',
    'Indianapolis, IN', 'Fort Wayne, IN', 'Evansville, IN',
    'Columbus, OH', 'Cincinnati, OH', 'Cleveland, OH', 'Toledo, OH', 'Akron, OH',
    'Dayton, OH', 'Canton, OH',
  ],
  great_lakes: [
    'Chicago, IL', 'Aurora, IL', 'Naperville, IL', 'Rockford, IL', 'Joliet, IL',
    'Detroit, MI', 'Grand Rapids, MI', 'Warren, MI', 'Sterling Heights, MI', 'Ann Arbor, MI',
    'Milwaukee, WI', 'Madison, WI', 'Green Bay, WI',
    'Minneapolis, MN', 'Saint Paul, MN', 'Rochester, MN',
    'Kansas City, MO', 'St. Louis, MO', 'Springfield, MO',
    'Omaha, NE', 'Lincoln, NE',
    'Des Moines, IA', 'Cedar Rapids, IA',
    'Sioux Falls, SD', 'Fargo, ND',
  ],
  mountain_southwest: [
    'Phoenix, AZ', 'Tucson, AZ', 'Mesa, AZ', 'Scottsdale, AZ', 'Chandler, AZ',
    'Gilbert, AZ', 'Tempe, AZ', 'Peoria, AZ',
    'Denver, CO', 'Colorado Springs, CO', 'Aurora, CO', 'Fort Collins, CO', 'Boulder, CO',
    'Las Vegas, NV', 'Henderson, NV', 'Reno, NV', 'North Las Vegas, NV',
    'Salt Lake City, UT', 'West Valley City, UT', 'Provo, UT', 'Ogden, UT',
    'Albuquerque, NM', 'Santa Fe, NM', 'Las Cruces, NM',
    'Boise, ID', 'Nampa, ID',
  ],
  west_coast: [
    'Los Angeles, CA', 'San Diego, CA', 'San Jose, CA', 'San Francisco, CA',
    'Fresno, CA', 'Sacramento, CA', 'Long Beach, CA', 'Oakland, CA',
    'Bakersfield, CA', 'Riverside, CA', 'Anaheim, CA', 'Stockton, CA',
    'Chula Vista, CA', 'Irvine, CA', 'Santa Ana, CA',
    'Seattle, WA', 'Spokane, WA', 'Tacoma, WA', 'Bellevue, WA', 'Vancouver, WA',
    'Portland, OR', 'Eugene, OR', 'Salem, OR',
    'Anchorage, AK', 'Honolulu, HI',
  ],
};

// ── Parse CLI args ────────────────────────────────────────────────────────────
const args         = process.argv.slice(2);
const workerFlag   = args.indexOf('--workers');
const maxWorkers   = workerFlag !== -1 ? parseInt(args[workerFlag + 1], 10) : 2;
const regionFilter = args.find(a => REGIONS[a]) ?? null;

const regionsToRun = regionFilter
  ? { [regionFilter]: REGIONS[regionFilter] }
  : REGIONS;

const workerEntries = Object.entries(regionsToRun).slice(0, maxWorkers);

const fmt = key => key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());

log.info({
  event:    'scraper_launch',
  workers:  workerEntries.length,
  regions:  workerEntries.map(([r]) => fmt(r)),
  cities:   workerEntries.reduce((sum, [, cities]) => sum + cities.length, 0),
  industries: industries.length,
});

console.log(`
╔══════════════════════════════════════════════════════╗
║          FREE SCRAPER — STATE-BY-STATE MODE          ║
╠══════════════════════════════════════════════════════╣
║  Workers  : ${String(workerEntries.length).padEnd(38)} ║
║  Regions  : ${workerEntries.map(([r]) => fmt(r)).join(', ').padEnd(38)} ║
║  Cities   : ${String(workerEntries.reduce((s, [, c]) => s + c.length, 0)).padEnd(38)} ║
║  Industries: ${String(industries.length).padEnd(37)} ║
║                                                      ║
║  Email sending: DISABLED (scrape only)               ║
║  Stop with: Ctrl+C                                   ║
╚══════════════════════════════════════════════════════╝
`);

// ── Launch one node per region ────────────────────────────────────────────────
const nodes = workerEntries.map(([regionName, cities]) => {
  const node = new FreeScraperNode(
    {
      node_id:            `free-scraper-${regionName}`,
      region_label:       fmt(regionName),
      target_industries:  industries,
      target_locations:   cities,
      sources:            ['bing', 'bbb', 'google_maps'],
      daily_cap_per_city: 500,   // high cap — let it run
      cycle_sleep:        30,    // 30s between cycles (moves to next city each cycle)
      allow_self_destruct: false, // never kill these
      queue_url:          process.env.SWARM_SCRAPER_QUEUE_URL ?? '',
    },
    process.env.AWS_REGION ?? 'us-east-1',
  );
  return { regionName, node };
});

// ── Start all workers concurrently ────────────────────────────────────────────
await Promise.allSettled(
  nodes.map(({ regionName, node }) =>
    node.start().catch(err => {
      log.error({ event: 'worker_crashed', region: regionName, error: err.message });
    }),
  ),
);
