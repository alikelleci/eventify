import { Injectable, effect, inject, signal, untracked } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { NavigationEnd, Router } from '@angular/router';
import { Subject, filter, map } from 'rxjs';

const RECENT_KEY = 'eventify.recentSearches';
const MAX_RECENT = 8;

/** Aggregate search state shared by the header search and the aggregate page. */
@Injectable({ providedIn: 'root' })
export class SearchService {
  private readonly router = inject(Router);

  /** The aggregate ID in the URL (/aggregates/:id), or '' on the landing page. */
  readonly currentId = toSignal(
    this.router.events.pipe(filter(e => e instanceof NavigationEnd), map(() => this.idFromUrl())),
    { initialValue: '' },
  );
  readonly recent = signal<string[]>(this.loadRecent());
  /** True while the open aggregate is loading; drives the header progress bar. */
  readonly loading = signal(false);
  /** Emits when the already-open aggregate is searched again, so the page can reload it. */
  readonly reload$ = new Subject<string>();
  /** Emits when something outside the header (e.g. the landing page) wants the search box focused. */
  readonly focus$ = new Subject<void>();

  constructor() {
    // Remember every aggregate that gets opened, also via a link, a bookmark or the back button.
    effect(() => {
      const id = this.currentId();
      // untracked: removing the open aggregate from the list must not re-add it.
      if (id) untracked(() => this.saveRecent(id));
    });
  }

  open(rawId: string) {
    const id = rawId.trim();
    if (!id) return;
    const currentId = this.idFromUrl();
    if (id === currentId) {
      this.reload$.next(id);
    } else {
      // From the landing page push a history entry, so the back button returns to it.
      this.router.navigate(['/aggregates', id], { replaceUrl: !!currentId });
    }
  }

  focusSearch() {
    this.focus$.next();
  }

  removeRecent(id: string) {
    this.storeRecent(this.recent().filter(r => r !== id));
  }

  private idFromUrl(): string {
    let route = this.router.routerState.snapshot.root;
    while (route.firstChild) route = route.firstChild;
    return (route.paramMap.get('id') ?? '').trim();
  }

  private saveRecent(id: string) {
    this.storeRecent([id, ...this.recent().filter(r => r !== id)].slice(0, MAX_RECENT));
  }

  private storeRecent(ids: string[]) {
    this.recent.set(ids);
    localStorage.setItem(RECENT_KEY, JSON.stringify(ids));
  }

  private loadRecent(): string[] {
    try { return JSON.parse(localStorage.getItem(RECENT_KEY) ?? '[]'); }
    catch { return []; }
  }
}
