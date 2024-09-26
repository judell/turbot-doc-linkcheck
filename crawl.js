import { PuppeteerCrawler, RequestQueue, log } from 'crawlee';
import { URL } from 'url';

/*
// guardrails
const BASE_URL = 'https://turbot.com/guardrails/docs';
const TOC_SELECTOR = 'div.hidden a'
const TOC_EXCLUDER = 'a[href]:not(div.hidden a)'
*/

/*
// flowpipe
const BASE_URL = 'https://flowpipe.io/docs';
const TOC_SELECTOR = 'div.mt-2 a'
const TOC_EXCLUDER = 'a[href]:not(div.mt-2 a)'
*/

/*
// steampipe
const BASE_URL = 'https://steampipe.io/docs';
const TOC_SELECTOR = 'div.mt-2 a'
const TOC_EXCLUDER = 'a[href]:not(div.mt-2 a)'
*/

// powerpipe
const BASE_URL = 'https://powerpipe.io/docs';
const TOC_SELECTOR = 'div.mt-2 a'
const TOC_EXCLUDER = 'a[href]:not(div.mt-2 a)'

log.setLevel(log.LEVELS.INFO)

// Create a request queue
const requestQueue = await RequestQueue.open();

const notFoundUrls = [];
const caseInsensitiveMatches = [];

// Create Sets to keep track of processed and enqueued URLs, and toc links
const processedUrls = new Set();
const enqueuedUrls = new Set();
const tocLinks = new Set();

// Function to normalize URLs (removes trailing slashes, sorts query params, ensures uniformity)
const normalizeUrl = (url) => {
  try {
    const normalized = new URL(url, BASE_URL);
    normalized.hash = ''; // Ignore URL fragments
    normalized.searchParams.sort(); // Sort query parameters
    let path = normalized.pathname;
    if (path.endsWith('/')) {
      path = path.slice(0, -1); // Remove trailing slash
    }
    return `${normalized.origin}${path}${normalized.search}`;
  } catch (err) {
    log.warn(`Failed to normalize URL: ${url}, error: ${err.message}`);
    return null;
  }
};

// Function to check if a URL belongs to the site we're crawling
const isInternalUrl = (url) => {
  return url.toLowerCase().startsWith(BASE_URL.toLowerCase());
};

// Function to compare URLs case-insensitively
const areUrlsEqual = (url1, url2) => {
  return url1.toLowerCase() === url2.toLowerCase();
};

let counter = 1;
let tocLinksSize = 0;

const crawler = new PuppeteerCrawler({
  async requestHandler({ page, request }) {
    const response = await page.goto(request.url, { waitUntil: 'networkidle0' });
    const finalUrl = normalizeUrl(page.url());

    if (!finalUrl || !isInternalUrl(finalUrl)) {
      log.debug(`Skipping non-internal URL: ${finalUrl}`);
      return;
    }

    log.info(`Processing URL ${counter++}: ${finalUrl}`);

    if (processedUrls.has(finalUrl)) {
      log.debug(`Already processed URL: ${finalUrl}. Skipping.`);
      return;
    }
    processedUrls.add(finalUrl);

    // Capture TOC links only on the first page
    if (tocLinks.size === 0) {
      const pageTocLinks = await page.evaluate((TOC_SELECTOR) => {
        const links = document.querySelectorAll(TOC_SELECTOR);
        return Array.from(links).map(link => link.href);
      }, TOC_SELECTOR);

      log.info(`Found ${pageTocLinks.length} TOC links.`);

      pageTocLinks.forEach(link => tocLinks.add(normalizeUrl(link)));

      // Ensure all TOC links are enqueued
      tocLinks.forEach(async (tocLink) => {
        if (!enqueuedUrls.has(tocLink)) {
          log.debug(`Enqueuing TOC link: ${tocLink}`);
          await requestQueue.addRequest({
            url: tocLink,
            userData: { referrer: 'Initial TOC page' }
          });
          enqueuedUrls.add(tocLink);
        } else {
          log.debug(`TOC link already enqueued: ${tocLink}`);
        }
      });
    }

    const statusCode = response.status();
    
    if (statusCode === 404) {

      const normalizedFinalUrl = normalizeUrl(finalUrl);
      if (!tocLinks.has(normalizedFinalUrl)) {
        log.error(`404 Not Found: ${finalUrl}, Referrer: ${request.userData.referrer || 'Unknown'}`);
        notFoundUrls.push({
          url: finalUrl,
          referrer: request.userData.referrer || 'Unknown'
        });
      } else {
        log.debug(`Skipped logging 404 for TOC link: ${finalUrl}`);
      }

      return;
    }

    // Extract internal links
    const linkDetails = await page.$$eval(TOC_EXCLUDER, (links) => {
      return links.map(link => ({
        href: link.href,
        text: link.textContent.trim()
      })).filter(link => link.href);
    });

    log.debug(`Found ${linkDetails.length} internal links on ${finalUrl}`);

    // Enqueue internal links for further crawling
    for (const link of linkDetails) {
      const fullUrl = normalizeUrl(link.href);
      if (!fullUrl || enqueuedUrls.has(fullUrl)) continue;

      log.debug(`Enqueuing internal link: ${fullUrl}`);
      await requestQueue.addRequest({
        url: fullUrl,
        userData: { referrer: finalUrl }
      });
      enqueuedUrls.add(fullUrl);
    }
  },
  requestQueue,
});

// Run the crawler
await crawler.run([{ url: BASE_URL, userData: { referrer: 'Initial URL' } }]);

// After the crawl, log the list of 404 URLs with their referrers
const filteredNotFoundUrls = notFoundUrls.filter(({ url }) => !tocLinks.has(normalizeUrl(url)));
if (filteredNotFoundUrls.length > 0) {
  log.info('List of 404 URLs and their referrers:');
  filteredNotFoundUrls.forEach(({ url, referrer }) => {
    log.info(`404 URL: ${url}`);
    log.info(`Referrer: ${referrer}`);
    log.info('---');
  });
} else {
  log.info('No 404 errors found.');
}

// Log statistics
log.info(`Total processed URLs: ${processedUrls.size}`);
log.info(`Total enqueued URLs: ${enqueuedUrls.size}`);
log.info(`Total 404 errors: ${notFoundUrls.length}`);
