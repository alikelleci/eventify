'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { validateRelease } = require('./release.cjs');

test('release tags produce the same canonical Maven version and Docker tag', () => {
  assert.equal(validateRelease('v1.2.3', false), '1.2.3');
  assert.equal(validateRelease('1.2.3', false), '1.2.3');
  assert.equal(validateRelease('v0.0.0', false), '0.0.0');
});

test('prerelease suffixes require GitHub prerelease status', () => {
  for (const version of ['1.2.3-rc1', '1.2.3-rc.1', '1.2.3-beta', '1.2.3-0', '1.2.3-001beta', '1.2.3-alpha-beta.2']) {
    assert.equal(validateRelease(`v${version}`, true), version);
    assert.throws(() => validateRelease(`v${version}`, false), /marked as a GitHub prerelease/);
  }
});

test('GitHub prereleases must not consume a stable Maven version', () => {
  for (const tag of ['v1.2.3', '1.2.3', 'v0.0.0']) {
    assert.throws(() => validateRelease(tag, true), /prerelease must have a version suffix/);
  }
});

test('malformed, snapshot and Docker-incompatible release tags are rejected', () => {
  const invalidTags = [
    '', 'v', 'V1.2.3', 'vv1.2.3', 'v.1.2.3-rc1', 'v.1.2.3-rc.1', 'v.1.2.3-snapshot',
    '1.2', '1.2.3.4', '1.2.3\n', ' 1.2.3',
    '01.2.3', '1.02.3', '1.2.03', '1.2.3-', '1.2.3-01', '1.2.3-beta..1',
    '1.2.3-beta_1', '1.2.3+build.1', '1.2.3-rc.1+build.1',
    '1.2.3-SNAPSHOT', '1.2.3-snapshot', '1.2.3-rc.SNAPSHOT', '1.2.3-rc-SNAPSHOT', '1.2.3-rcSNAPSHOT',
  ];
  for (const tag of invalidTags) {
    assert.throws(() => validateRelease(tag, true), undefined, tag);
  }
  for (const tag of [null, undefined, 123, {}]) {
    assert.throws(() => validateRelease(tag, true), /SemVer string/);
  }
});

test('the canonical Docker version respects the 128 character tag limit', () => {
  const valid = `1.2.3-${'a'.repeat(122)}`;
  assert.equal(valid.length, 128);
  assert.equal(validateRelease(`v${valid}`, true), valid);
  assert.throws(() => validateRelease(`${valid}a`, true), /128 characters/);
});
