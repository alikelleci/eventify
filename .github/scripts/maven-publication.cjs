'use strict';

const { appendFileSync } = require('node:fs');
const { setTimeout: sleep } = require('node:timers/promises');
const { validateRelease } = require('./release.cjs');

const REPOSITORY = 'https://repo.maven.apache.org/maven2/io/github/alikelleci';
// Keep aligned with the release profile: the console server is a Docker-only application.
const LIBRARIES = [
  'eventify-core',
  'eventify-console-protocol',
  'eventify-console-client',
  'eventify-spring-boot-starter',
];

function publicationUrls(version) {
  const canonical = validateRelease(version, typeof version === 'string' && version.includes('-'));
  if (canonical !== version) throw new Error('Use the canonical Maven version without a v prefix.');

  const artifacts = [
    ['eventify', ['.pom']],
    ...LIBRARIES.map((artifact) => [artifact, ['.pom', '.jar', '-sources.jar', '-javadoc.jar']]),
  ];
  return artifacts.flatMap(([artifact, extensions]) => extensions.flatMap((extension) => {
    const url = `${REPOSITORY}/${artifact}/${version}/${artifact}-${version}${extension}`;
    return [url, `${url}.asc`];
  }));
}

function verifyRevision(pom, revision) {
  // Only the release property's literal value counts; never accept a legacy or different build.
  const properties = [...pom.replace(/<!--[\s\S]*?-->/g, '').matchAll(/<eventify\.release\.commit>\s*([^<]*?)\s*<\/eventify\.release\.commit>/g)];
  if (properties.length !== 1 || properties[0][1] !== revision) {
    throw new Error('The published parent POM has a different or missing eventify.release.commit; refusing to reuse this Maven version.');
  }
}

async function inspectPublication(version, { revision, fetchImpl = fetch, timeoutMs = 10000 } = {}) {
  if (typeof revision !== 'string' || !/^[a-f0-9]{40}$/.test(revision)) {
    throw new Error('A full Git commit SHA is required to verify Maven publication provenance.');
  }
  const urls = publicationUrls(version);
  const parentPom = `${REPOSITORY}/eventify/${version}/eventify-${version}.pom`;
  const present = [];
  const missing = [];
  let next = 0;
  let failed = false;

  // Bound concurrent probes to avoid overloading Central or its rate limiter.
  await Promise.all(Array.from({ length: 4 }, async () => {
    while (!failed && next < urls.length) {
      const url = urls[next++];
      try {
        const response = await fetchImpl(url, {
          method: url === parentPom ? 'GET' : 'HEAD',
          redirect: 'error',
          signal: AbortSignal.timeout(timeoutMs),
        });
        if (response.status === 200) {
          if (url === parentPom) verifyRevision(await response.text(), revision);
          present.push(url);
        }
        else if (response.status === 404) missing.push(url);
        else throw new Error(`Unexpected HTTP ${response.status}`);
      } catch (error) {
        failed = true;
        throw new Error(`Cannot determine Maven Central publication state for ${url}: ${error.message}`, { cause: error });
      }
    }
  }));

  return {
    state: missing.length === 0 ? 'complete' : present.length === 0 ? 'absent' : 'partial',
    present,
    missing,
  };
}

async function waitForPublication(version, {
  attempts = 21,
  delayMs = 30000,
  sleepImpl = sleep,
  ...options
} = {}) {
  if (!Number.isSafeInteger(attempts) || attempts < 1) throw new Error('Publication polling attempts must be positive.');
  if (!Number.isFinite(delayMs) || delayMs < 0) throw new Error('Publication polling delay must not be negative.');

  let result;
  for (let attempt = 0; attempt < attempts; attempt++) {
    if (attempt > 0) await sleepImpl(delayMs);
    result = await inspectPublication(version, options);
    if (result.state === 'complete') return result;
  }
  throw new Error(
    `Maven Central publication ${version} is ${result.state}; ${result.missing.length} expected files remain missing. ` +
    'No automatic redeployment was attempted. Check the Central deployment before retrying.',
  );
}

async function checkPublication(version, options = {}) {
  const result = await inspectPublication(version, options);
  if (result.state === 'complete') return true;
  if (result.state === 'absent') return false;
  // A partly visible version may be propagating. Never try to overwrite it.
  await waitForPublication(version, options);
  return true;
}

async function main() {
  const [command, version] = process.argv.slice(2);
  const options = { revision: process.env.GITHUB_SHA };
  if (command === 'check') {
    const published = await checkPublication(version, options);
    if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `published=${published}\n`);
    console.log(published
      ? `Maven Central ${version} is complete; skip deployment.`
      : `Maven Central ${version} is absent; deployment can proceed.`);
  } else if (command === 'wait') {
    await waitForPublication(version, options);
    console.log(`Maven Central ${version} is complete; publication verified.`);
  } else {
    throw new Error('Usage: node maven-publication.cjs <check|wait> VERSION');
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}

module.exports = { publicationUrls, inspectPublication, checkPublication, waitForPublication };
