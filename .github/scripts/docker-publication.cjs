'use strict';

const { appendFileSync } = require('node:fs');
const { createHash } = require('node:crypto');
const { validateRelease } = require('./release.cjs');

const ACCEPT_MANIFEST = [
  'application/vnd.oci.image.index.v1+json',
  'application/vnd.docker.distribution.manifest.list.v2+json',
  'application/vnd.oci.image.manifest.v1+json',
  'application/vnd.docker.distribution.manifest.v2+json',
].join(', ');

function stableVersion(version) {
  try {
    const canonical = validateRelease(version, false);
    return canonical === version ? canonical.split('.').map(BigInt) : null;
  } catch {
    return null;
  }
}

function compareStableVersions(left, right) {
  const a = stableVersion(left);
  const b = stableVersion(right);
  if (!a || !b) throw new Error('Docker latest comparisons require canonical stable versions.');
  for (let index = 0; index < 3; index += 1) {
    if (a[index] < b[index]) return -1;
    if (a[index] > b[index]) return 1;
  }
  return 0;
}

function nextPageUrl(link, currentUrl, tagsUrl) {
  if (!link) return null;
  const next = link.split(',').filter((part) => /;\s*rel\s*=\s*"?next"?(?:\s*;|\s*$)/i.test(part));
  if (next.length === 0) throw new Error('Unrecognized GHCR pagination link.');
  if (next.length !== 1) throw new Error('Ambiguous GHCR pagination links.');
  const match = /^\s*<([^>]+)>\s*;/.exec(next[0]);
  if (!match) throw new Error('Malformed GHCR pagination link.');
  const url = new URL(match[1], currentUrl);
  const allowed = new URL(tagsUrl);
  if (url.origin !== allowed.origin || url.pathname !== allowed.pathname || url.username || url.password || url.hash) {
    throw new Error('GHCR pagination must stay on the package tags endpoint.');
  }
  for (const key of url.searchParams.keys()) {
    if (key !== 'n' && key !== 'last') throw new Error('Unexpected GHCR pagination parameter.');
  }
  return url.href;
}

async function shouldPromotePublishedLatest({ image, version, actor, token, fetchImpl = fetch }) {
  if (typeof image !== 'string' || !/^ghcr\.io\/[a-z0-9]+(?:[._-][a-z0-9]+)*(?:\/[a-z0-9]+(?:[._-][a-z0-9]+)*)+$/.test(image)) {
    throw new Error('IMAGE must name a GHCR package in lowercase.');
  }
  if (!stableVersion(version)) throw new Error('VERSION must be a canonical stable release version.');
  if (!actor || !token) throw new Error('GitHub actor and token are required to read GHCR.');
  const repository = image.slice('ghcr.io/'.length);

  async function request(url, headers, allowMissing = false) {
    let response;
    try {
      response = await fetchImpl(url, {
        method: 'GET', headers, redirect: 'error', signal: AbortSignal.timeout(30_000),
      });
    } catch {
      throw new Error('GHCR request failed; latest remains unchanged.');
    }
    if (allowMissing && response.status === 404) return null;
    if (!response.ok) throw new Error(`GHCR returned HTTP ${response.status}; latest remains unchanged.`);
    return response;
  }

  async function readJson(response) {
    try {
      return JSON.parse(await response.text());
    } catch {
      throw new Error('GHCR returned invalid JSON; latest remains unchanged.');
    }
  }

  const authUrl = new URL('https://ghcr.io/token');
  authUrl.searchParams.set('service', 'ghcr.io');
  authUrl.searchParams.set('scope', `repository:${repository}:pull`);
  const credentials = Buffer.from(`${actor}:${token}`).toString('base64');
  const auth = await readJson(await request(authUrl.href, { Authorization: `Basic ${credentials}` }));
  if (!auth || typeof auth.token !== 'string' || !auth.token) throw new Error('GHCR did not return a pull token.');
  const headers = { Authorization: `Bearer ${auth.token}`, Accept: ACCEPT_MANIFEST };
  const tagsUrl = `https://ghcr.io/v2/${repository}/tags/list`;
  const tags = new Set();
  const visited = new Set();
  let page = `${tagsUrl}?n=100`;
  while (page) {
    if (visited.has(page)) throw new Error('GHCR pagination repeated a page.');
    visited.add(page);
    const response = await request(page, headers);
    const body = await readJson(response);
    if (!body || body.name !== repository || (body.tags !== null && !Array.isArray(body.tags))) {
      throw new Error('GHCR returned an invalid package tag listing.');
    }
    for (const tag of body.tags || []) {
      if (typeof tag !== 'string') throw new Error('GHCR returned an invalid package tag.');
      tags.add(tag);
    }
    page = nextPageUrl(response.headers.get('link'), page, tagsUrl);
  }
  if (!tags.has(version)) throw new Error('The current release version is not yet visible in GHCR.');
  const stableTags = [...tags].filter((tag) => stableVersion(tag));
  if (stableTags.some((tag) => compareStableVersions(tag, version) > 0)) return false;

  async function manifest(tag, allowMissing = false) {
    const response = await request(`https://ghcr.io/v2/${repository}/manifests/${encodeURIComponent(tag)}`, headers, allowMissing);
    if (!response) return null;
    const raw = await response.text();
    let document;
    try { document = JSON.parse(raw); } catch { throw new Error('GHCR returned an invalid image manifest.'); }
    if (!document || document.schemaVersion !== 2) throw new Error('GHCR returned an unsupported image manifest.');
    const digest = `sha256:${createHash('sha256').update(raw).digest('hex')}`;
    const reported = response.headers.get('docker-content-digest');
    if (reported && reported !== digest) throw new Error('GHCR manifest digest does not match its content.');
    return { document, digest };
  }

  const latest = await manifest('latest', !tags.has('latest'));
  if (!latest) return true;
  const annotated = latest.document.annotations?.['org.opencontainers.image.version'];
  if (annotated !== undefined) {
    if (!stableVersion(annotated)) throw new Error('The current latest image has invalid version metadata.');
    return compareStableVersions(version, annotated) >= 0;
  }

  // Older releases have no index annotation. Find their immutable version tag
  // by manifest digest rather than guessing a version from GitHub releases.
  for (const tag of stableTags) {
    const published = await manifest(tag);
    if (published.digest === latest.digest) return compareStableVersions(version, tag) >= 0;
  }
  throw new Error('The current latest image cannot be matched to a stable published version.');
}

module.exports = { compareStableVersions, nextPageUrl, shouldPromotePublishedLatest };

if (require.main === module) {
  shouldPromotePublishedLatest({
    image: process.env.IMAGE,
    version: process.env.VERSION,
    actor: process.env.GITHUB_ACTOR,
    token: process.env.GITHUB_TOKEN,
  }).then((promote) => {
    if (!process.env.GITHUB_OUTPUT) throw new Error('GITHUB_OUTPUT is required.');
    appendFileSync(process.env.GITHUB_OUTPUT, `promote=${promote}\n`);
    console.log(promote ? 'Promoting the highest successfully published stable image.' : 'A newer published stable image exists; latest remains unchanged.');
  }).catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}
