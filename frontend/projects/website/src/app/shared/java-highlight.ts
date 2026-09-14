/**
 * Minimal Java highlighting for the website's static code samples: comments, strings, annotations, keywords and types.
 * Returns HTML with Tailwind classes; the input is escaped first, so it is safe to bind with [innerHTML].
 */
const KEYWORDS = new Set([
  'public', 'private', 'class', 'interface', 'return', 'new', 'if', 'else', 'throw', 'null', 'void', 'final', 'import', 'static',
]);

const TOKEN = /(\/\/[^\n]*)|("(?:[^"\\]|\\.)*")|(@\w+)|\b([A-Za-z_]\w*)\b/g;

export function highlightJava(code: string): string {
  const escaped = code.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  return escaped.replace(TOKEN, (match, comment, string, annotation, word) => {
    if (comment) return `<span class="text-slate-500 italic">${comment}</span>`;
    if (string) return `<span class="text-emerald-300">${string}</span>`;
    if (annotation) return `<span class="text-amber-300">${annotation}</span>`;
    if (KEYWORDS.has(word)) return `<span class="text-sky-300">${word}</span>`;
    if (/^[A-Z]/.test(word)) return `<span class="text-primary-300">${word}</span>`;
    return match;
  });
}
