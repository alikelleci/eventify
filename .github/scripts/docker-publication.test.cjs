'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { createHash } = require('node:crypto');
const { compareStableVersions, nextPageUrl, shouldPromotePublishedLatest } = require('./docker-publication.cjs');

const IMAGE = 'ghcr.io/example/console';
const REPOSITORY = 'example/console';
const TAGS_URL = `https://ghcr.io/v2/${REPOSITORY}/tags/list`;
const manifest = (version, annotate = true) => ({
  schemaVersion: 2,
  mediaType: 'application/vnd.oci.image.index.v1+json',
  manifests: [],
  ...(annotate ? { annotations: { 'org.opencontainers.image.version': version } } : {}),
  // Keep legacy fixture contents distinct without relying on version metadata.
  fixture: version,
});

function registry({ tags = ['1.2.3'], latest = null, manifests = {}, pages = {}, responses = {} } = {}) {
  const calls = [];
  const fetchImpl = async (url, options) => {
    const parsed = new URL(url);
    calls.push({ url: parsed, options });
    assert.equal(options.redirect, 'error');
    if (responses[parsed.pathname]) return responses[parsed.pathname]();
    if (parsed.pathname === '/token') {
      assert.equal(parsed.searchParams.get('scope'), `repository:${REPOSITORY}:pull`);
      assert.equal(options.headers.Authorization, `Basic ${Buffer.from('actor:github-token').toString('base64')}`);
      return Response.json({ token: 'registry-token' });
    }
    assert.equal(options.headers.Authorization, 'Bearer registry-token');
    if (parsed.pathname.endsWith('/tags/list')) {
      const page = pages[parsed.searchParams.get('last') || 'first'];
      return Response.json({ name: REPOSITORY, tags: page?.tags || tags }, {
        headers: page?.link ? { link: page.link } : {},
      });
    }
    const tag = decodeURIComponent(parsed.pathname.split('/').at(-1));
    const document = tag === 'latest' ? latest : manifests[tag] || manifest(tag);
    if (document === null) return new Response('', { status: 404 });
    const body = JSON.stringify(document);
    return new Response(body, { headers: { 'docker-content-digest': `sha256:${createHash('sha256').update(body).digest('hex')}` } });
  };
  return {
    calls,
    check: (version = '1.2.3') => shouldPromotePublishedLatest({
      image: IMAGE, version, actor: 'actor', token: 'github-token', fetchImpl,
    }),
  };
}

test('the first stable image can create latest after a successful registry listing', async () => {
  const ghcr = registry();
  assert.equal(await ghcr.check(), true);
  assert.ok(ghcr.calls.every(({ url }) => url.origin === 'https://ghcr.io'));
});

test('only actual published stable versions can block promotion', async () => {
  const ghcr = registry({
    tags: ['1.2.2', '1.2.3', '9.0.0-rc.1', 'build-100-1-amd64', 'v10.0.0', 'latest'],
    latest: manifest('1.2.2'),
  });
  assert.equal(await ghcr.check(), true);
  assert.equal(await registry({ tags: ['1.2.3', '1.2.4'] }).check(), false);
});

test('numeric stable version comparisons preserve precision', () => {
  for (const [older, newer] of [
    ['9.99.99', '10.0.0'], ['1.9.99', '1.10.0'], ['1.2.9', '1.2.10'],
    ['9007199254740992.0.0', '9007199254740993.0.0'],
  ]) {
    assert.equal(compareStableVersions(older, newer), -1);
    assert.equal(compareStableVersions(newer, older), 1);
    assert.equal(compareStableVersions(newer, newer), 0);
  }
  for (const invalid of ['v1.2.3', '1.2.3-rc.1', '1.2.3+build', '01.2.3', null]) {
    assert.throws(() => compareStableVersions(invalid, '1.2.3'), /canonical stable/);
  }
});

test('rerunning the same stable release is idempotent', async () => {
  assert.equal(await registry({ tags: ['1.2.3', 'latest'], latest: manifest('1.2.3') }).check(), true);
});

test('current latest guards against downgrade even with an outdated tag listing', async () => {
  for (const tags of [['1.2.3'], ['1.2.3', 'latest']]) {
    assert.equal(await registry({ tags, latest: manifest('2.0.0') }).check(), false);
  }
});

test('legacy latest can be identified by a stable manifest digest', async () => {
  const legacy = manifest('1.2.2', false);
  assert.equal(await registry({
    tags: ['1.2.2', '1.2.3', 'latest'], latest: legacy, manifests: { '1.2.2': legacy },
  }).check(), true);
});

test('unknown or malformed latest metadata cannot be overwritten', async () => {
  await assert.rejects(registry({ latest: manifest('unknown', false) }).check(), /cannot be matched/);
  for (const version of ['v1.2.3', '1.2.3-rc.1', '', null]) {
    await assert.rejects(registry({ latest: manifest(version) }).check(), /invalid version metadata/);
  }
});

test('latest listed by the registry must be readable', async () => {
  await assert.rejects(registry({ tags: ['1.2.3', 'latest'] }).check(), /HTTP 404/);
});

test('the candidate must have actually been published', async () => {
  await assert.rejects(registry({ tags: ['1.2.2'] }).check(), /not yet visible/);
});

test('all pages of published version tags participate in the decision', async () => {
  assert.equal(await registry({ pages: {
    first: { tags: ['1.2.3'], link: `</v2/${REPOSITORY}/tags/list?n=100&last=1.2.3>; rel="next"` },
    '1.2.3': { tags: ['2.0.0'] },
  } }).check(), false);
});

test('pagination cannot send credentials outside the package tags endpoint', () => {
  const base = `${TAGS_URL}?n=100`;
  assert.equal(nextPageUrl(`<${TAGS_URL}?n=100&last=1.2.3>; rel="next"`, base, TAGS_URL), `${TAGS_URL}?n=100&last=1.2.3`);
  for (const destination of [
    'https://evil.example/tags', 'http://ghcr.io/v2/example/console/tags/list',
    'https://ghcr.io/v2/other/console/tags/list', `${TAGS_URL}?token=secret`,
    `https://actor:secret@ghcr.io/v2/${REPOSITORY}/tags/list`,
  ]) {
    assert.throws(() => nextPageUrl(`<${destination}>; rel="next"`, base, TAGS_URL), /pagination/);
  }
  assert.throws(() => nextPageUrl('invalid', base, TAGS_URL), /pagination/);
});

test('repeated pagination cannot loop indefinitely', async () => {
  await assert.rejects(registry({ pages: {
    first: { tags: ['1.2.3'], link: `<${TAGS_URL}?n=100>; rel="next"` },
  } }).check(), /repeated a page/);
});

test('HTTP failures and network failures leave latest untouched', async () => {
  for (const pathname of ['/token', `/v2/${REPOSITORY}/tags/list`, `/v2/${REPOSITORY}/manifests/latest`]) {
    for (const status of [401, 403, 404, 429, 500]) {
      // Only a genuinely missing, unlisted latest permits bootstrapping.
      const tags = pathname.endsWith('/latest') ? ['1.2.3', 'latest'] : ['1.2.3'];
      await assert.rejects(registry({ tags, responses: {
        [pathname]: () => new Response('', { status }),
      } }).check(), /HTTP/);
    }
    await assert.rejects(registry({ responses: {
      [pathname]: () => { throw new Error('Network unavailable'); },
    } }).check(), /request failed/);
  }
});

test('malformed registry responses and digest mismatches fail closed', async () => {
  const cases = [
    ['/token', () => Response.json({ token: '' })],
    [`/v2/${REPOSITORY}/tags/list`, () => Response.json({ name: 'other/package', tags: ['1.2.3'] })],
    [`/v2/${REPOSITORY}/tags/list`, () => Response.json({ name: REPOSITORY, tags: [42] })],
    [`/v2/${REPOSITORY}/tags/list`, () => new Response('invalid')],
    [`/v2/${REPOSITORY}/manifests/latest`, () => Response.json({ schemaVersion: 1 })],
    [`/v2/${REPOSITORY}/manifests/latest`, () => new Response('invalid')],
    [`/v2/${REPOSITORY}/manifests/latest`, () => Response.json(manifest('1.2.2'), { headers: { 'docker-content-digest': 'sha256:wrong' } })],
  ];
  for (const [pathname, response] of cases) {
    await assert.rejects(registry({ responses: { [pathname]: response } }).check());
  }
});
