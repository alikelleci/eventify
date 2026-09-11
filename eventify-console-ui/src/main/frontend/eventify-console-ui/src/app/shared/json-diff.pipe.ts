import { Pipe, PipeTransform } from '@angular/core';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import * as jsondiffpatch from 'jsondiffpatch';
import { format } from 'jsondiffpatch/formatters/html';

const differ = jsondiffpatch.create();

@Pipe({ name: 'jsonDiff', standalone: true, pure: true })
export class JsonDiffPipe implements PipeTransform {
  constructor(private sanitizer: DomSanitizer) {}

  transform(current: Record<string, unknown>, previous: Record<string, unknown>): SafeHtml {
    const delta = differ.diff(previous, current);
    if (!delta) return this.sanitizer.bypassSecurityTrustHtml('<span class="text-surface-400 text-xs">No changes</span>');
    return this.sanitizer.bypassSecurityTrustHtml(format(delta, previous) ?? '');
  }
}
