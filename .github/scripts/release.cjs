'use strict';

// SemVer prerelease identifiers may contain letters, digits and hyphens;
// identifiers consisting only of digits must not contain leading zeroes.
const NUMBER = '(?:0|[1-9][0-9]*)';
const PRERELEASE_IDENTIFIER = '(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)';
const VERSION = new RegExp(
  `^(${NUMBER})\\.(${NUMBER})\\.(${NUMBER})(?:-(${PRERELEASE_IDENTIFIER}(?:\\.${PRERELEASE_IDENTIFIER})*))?$`,
);

function validateRelease(tag, prereleaseFlag) {
  if (typeof tag !== 'string') {
    throw new Error('The release tag must be a SemVer string, for example v1.2.3.');
  }

  const version = tag.startsWith('v') ? tag.slice(1) : tag;
  const match = VERSION.exec(version);
  if (!match) {
    throw new Error('Use a SemVer release tag such as v1.2.3 or v1.2.3-rc.1; build metadata is not supported.');
  }
  if (version.length > 128) {
    throw new Error('The release version exceeds the Docker tag limit of 128 characters.');
  }
  if (match[4] && /snapshot/i.test(match[4])) {
    throw new Error('SNAPSHOT versions cannot be published as releases.');
  }
  if (match[4] && prereleaseFlag !== true) {
    throw new Error('A version with a prerelease suffix must be marked as a GitHub prerelease.');
  }
  if (!match[4] && prereleaseFlag === true) {
    throw new Error('A GitHub prerelease must have a version suffix such as -rc.1.');
  }
  return version;
}

module.exports = { validateRelease };
