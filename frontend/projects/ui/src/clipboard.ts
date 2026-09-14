/**
 * Copies text to the clipboard. Resolves to false if the browser refused.
 *
 * The Clipboard API only exists on HTTPS and localhost, but the console server itself serves plain HTTP,
 * so opening it from another machine needs the older select-and-copy fallback, which works over HTTP too.
 */
export async function copyToClipboard(text: string): Promise<boolean> {
  if (navigator.clipboard && window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      // e.g. the document isn't focused; try the fallback
    }
  }

  const previousFocus = document.activeElement as HTMLElement | null;
  const textarea = document.createElement('textarea');
  textarea.value = text;
  textarea.setAttribute('readonly', '');
  textarea.style.cssText = 'position:fixed;top:0;left:0;opacity:0;pointer-events:none';
  // Next to the clicked button rather than on <body>, so a drawer's focus trap doesn't steal the selection.
  (previousFocus?.parentElement ?? document.body).appendChild(textarea);
  textarea.select();
  try {
    return document.execCommand('copy');
  } catch {
    return false;
  } finally {
    textarea.remove();
    previousFocus?.focus(); // back to the copy button, so keyboard users don't lose their place
  }
}
