import { Pipe, PipeTransform } from '@angular/core';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';

@Pipe({ name: 'jsonHighlight', standalone: true, pure: true })
export class JsonHighlightPipe implements PipeTransform {
  constructor(private sanitizer: DomSanitizer) {}

  transform(json: string): SafeHtml {
    // The JSON holds application data, e.g. what a customer typed: escaped first, so it shows as text and never runs as
    // HTML. Quotes stay, so the pattern below still finds the strings.
    const highlighted = escapeHtml(json).replace(
      /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,
      (match) => {
        if (/^"/.test(match)) {
          if (/:$/.test(match)) {
            return `<span class="text-slate-700 dark:text-slate-300">${match}</span>`;
          }
          return `<span class="text-emerald-600 dark:text-emerald-400">${match}</span>`;
        }
        if (/true|false/.test(match)) {
          return `<span class="text-amber-600 dark:text-amber-400">${match}</span>`;
        }
        if (/null/.test(match)) {
          return `<span class="text-slate-400 dark:text-slate-500 italic">${match}</span>`;
        }
        return `<span class="text-emerald-600 dark:text-emerald-400">${match}</span>`;
      }
    );
    return this.sanitizer.bypassSecurityTrustHtml(highlighted);
  }
}

function escapeHtml(text: string): string {
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
