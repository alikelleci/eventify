'use strict';

const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const test = require('node:test');
const { publicationUrls, inspectPublication, checkPublication, waitForPublication } = require('./maven-publication.cjs');

const version = '1.2.3-rc.1';
const revision = 'a'.repeat(40);
const urls = publicationUrls(version);
const response = (status) => ({ status, text: async () => `<project><properties><eventify.release.commit>${revision}</eventify.release.commit></properties></project>` });
const fetchStatus = (status) => async () => response(status);

test('the publication includes the parent, every deployable library, all classifiers and signatures', () => {
  const rootPom = readFileSync(join(__dirname, '../../pom.xml'), 'utf8');
  const modules = [...rootPom.matchAll(/<module>([^<]+)<\/module>/g)].map((match) => match[1]);
  const publishedModules = modules.filter((module) => {
    const pom = readFileSync(join(__dirname, '../../', module, 'pom.xml'), 'utf8');
    return !pom.includes('<maven.deploy.skip>true</maven.deploy.skip>');
  });
  assert.equal(urls.length, 2 + publishedModules.length * 8);
  assert.equal(new Set(urls).size, urls.length);
  assert.ok(urls.includes(`https://repo.maven.apache.org/maven2/io/github/alikelleci/eventify/${version}/eventify-${version}.pom.asc`));
  for (const module of publishedModules) {
    for (const suffix of ['.pom', '.jar', '-sources.jar', '-javadoc.jar']) {
      assert.ok(urls.some((url) => url.endsWith(`/${module}-${version}${suffix}`)));
      assert.ok(urls.some((url) => url.endsWith(`/${module}-${version}${suffix}.asc`)));
    }
  }
  assert.ok(urls.every((url) => !url.includes('eventify-console-server')));
});

test('only canonical release versions can be interpolated into repository paths', () => {
  assert.equal(publicationUrls('1.2.3').length, 34);
  for (const invalid of ['v1.2.3', '../1.2.3', '1.2.3-SNAPSHOT', '1.2.3/other', '1.2.3\n', undefined]) {
    assert.throws(() => publicationUrls(invalid));
  }
});

test('complete publication is accepted only when every artifact and signature is present', async () => {
  const calls = [];
  const result = await inspectPublication(version, { revision, fetchImpl: async (url, options) => {
    calls.push(url);
    assert.equal(options.method, url.endsWith(`/eventify-${version}.pom`) ? 'GET' : 'HEAD');
    assert.equal(options.redirect, 'error');
    assert.ok(options.signal instanceof AbortSignal);
    return response(200);
  } });
  assert.equal(result.state, 'complete');
  assert.equal(result.missing.length, 0);
  assert.deepEqual(calls.sort(), [...urls].sort());
  assert.equal(await checkPublication(version, { revision, fetchImpl: fetchStatus(200) }), true);
});

test('deployment is allowed only when all expected files explicitly return 404', async () => {
  const result = await inspectPublication(version, { revision, fetchImpl: fetchStatus(404) });
  assert.equal(result.state, 'absent');
  assert.equal(result.present.length, 0);
  assert.equal(result.missing.length, 34);
  assert.equal(await checkPublication(version, { revision, fetchImpl: fetchStatus(404) }), false);
});

test('a missing javadoc signature prevents a publication being treated as complete', async () => {
  const missing = urls.find((url) => url.endsWith('eventify-spring-boot-starter-1.2.3-rc.1-javadoc.jar.asc'));
  const result = await inspectPublication(version, { revision, fetchImpl: async (url) => response(url === missing ? 404 : 200) });
  assert.equal(result.state, 'partial');
  assert.deepEqual(result.missing, [missing]);
  await assert.rejects(checkPublication(version, {
    revision,
    fetchImpl: async (url) => response(url === missing ? 404 : 200),
    attempts: 1,
  }), /partial; 1 expected files remain missing/);
});

test('network failures and ambiguous HTTP results fail closed instead of triggering deployment', async () => {
  for (const status of [204, 301, 401, 403, 429, 500, 503]) {
    await assert.rejects(checkPublication(version, { revision, fetchImpl: fetchStatus(status) }), new RegExp(`HTTP ${status}`));
  }
  await assert.rejects(checkPublication(version, { revision, fetchImpl: async () => { throw new Error('connection timed out'); } }), /connection timed out/);
});

test('a fully published Maven version must originate from this release commit', async () => {
  for (const pom of [
    '<project/>',
    '<project><eventify.release.commit>development</eventify.release.commit></project>',
    `<project><eventify.release.commit>${'b'.repeat(40)}</eventify.release.commit></project>`,
    `<project><!-- <eventify.release.commit>${revision}</eventify.release.commit> --></project>`,
    `<project><eventify.release.commit>${revision}</eventify.release.commit><eventify.release.commit>${revision}</eventify.release.commit></project>`,
  ]) {
    await assert.rejects(checkPublication(version, {
      revision,
      fetchImpl: async () => ({ status: 200, text: async () => pom }),
    }), /different or missing eventify.release.commit/);
  }
  await assert.rejects(checkPublication(version, { fetchImpl: fetchStatus(200) }), /full Git commit SHA/);
});

test('failed deployment can recover after all expected files propagate', async () => {
  let calls = 0;
  const delays = [];
  const result = await waitForPublication(version, {
    revision,
    fetchImpl: async () => response(calls++ < urls.length ? 404 : 200),
    attempts: 3,
    delayMs: 7,
    sleepImpl: async (delay) => delays.push(delay),
  });
  assert.equal(result.state, 'complete');
  assert.equal(calls, urls.length * 2);
  assert.deepEqual(delays, [7]);
});

test('partial publication is polled before a rerun can skip deployment', async () => {
  let calls = 0;
  let waits = 0;
  const published = await checkPublication(version, {
    revision,
    fetchImpl: async () => response(calls++ === 0 ? 404 : 200),
    attempts: 2,
    sleepImpl: async () => waits++,
  });
  assert.equal(published, true);
  assert.equal(calls, urls.length * 2);
  assert.equal(waits, 0);
});

test('recovery is bounded and never treats an absent version as successful', async () => {
  const delays = [];
  await assert.rejects(waitForPublication(version, {
    revision,
    fetchImpl: fetchStatus(404),
    attempts: 3,
    delayMs: 20,
    sleepImpl: async (delay) => delays.push(delay),
  }), /absent; 34 expected files remain missing/);
  assert.deepEqual(delays, [20, 20]);
  await assert.rejects(waitForPublication(version, { attempts: 0 }), /attempts must be positive/);
  await assert.rejects(waitForPublication(version, { delayMs: -1 }), /delay must not be negative/);
});
