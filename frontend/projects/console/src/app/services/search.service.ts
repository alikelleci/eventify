import { Injectable, inject, signal } from '@angular/core';
import { toSignal } from '@angular/core/rxjs-interop';
import { NavigationEnd, Router } from '@angular/router';
import { Subject, filter, map } from 'rxjs';

const RECENT_KEY = 'eventify.recentSearches';
const MAX_RECENT = 8;

/** An aggregate's address: its application, its type and its identifier. One application can hold several aggregates. */
export interface AggregateRef {
  app: string;
  type: string;
  id: string;
}

/** Aggregate search state shared by the header search and the aggregate page. */
@Injectable({ providedIn: 'root' })
export class SearchService {
  private readonly router = inject(Router);

  /** The aggregate in the URL (/aggregates/:app/:type/:id), or null on the landing page. */
  readonly current = toSignal(
    this.router.events.pipe(filter(e => e instanceof NavigationEnd), map(() => this.fromUrl())),
    { initialValue: null as AggregateRef | null },
  );
  readonly recent = signal<AggregateRef[]>(this.loadRecent());
  /** True while the open aggregate is loading; drives the header progress bar. */
  readonly loading = signal(false);
  /** Emits when the already-open aggregate is searched again, so the page can reload it. */
  readonly reload$ = new Subject<AggregateRef>();
  /** Emits when something outside the header (e.g. the landing page) wants the search box focused. */
  readonly focus$ = new Subject<void>();

  open(app: string, type: string, rawId: string) {
    const id = rawId.trim();
    if (!id || !type || !app) return;
    const current = this.fromUrl();
    if (current && same(current, { app, type, id })) {
      this.reload$.next({ app, type, id });
    } else {
      // From the landing page push a history entry, so the back button returns to it.
      this.router.navigate(['/aggregates', app, type, id], { replaceUrl: !!current });
    }
  }

  /** Another application picked while an aggregate is open: the same type and ID in that application. */
  switchApp(app: string) {
    const current = this.fromUrl();
    if (current && current.app !== app) this.open(app, current.type, current.id);
  }

  focusSearch() {
    this.focus$.next();
  }

  removeRecent(aggregate: AggregateRef) {
    this.storeRecent(this.recent().filter(r => !same(r, aggregate)));
  }

  private fromUrl(): AggregateRef | null {
    let route = this.router.routerState.snapshot.root;
    while (route.firstChild) route = route.firstChild;
    const app = (route.paramMap.get('app') ?? '').trim();
    const type = (route.paramMap.get('type') ?? '').trim();
    const id = (route.paramMap.get('id') ?? '').trim();
    return app && type && id ? { app, type, id } : null;
  }

  /** Puts an aggregate on top of the recent ones. Only once something was found at its address: a mistyped ID isn't kept. */
  remember(aggregate: AggregateRef) {
    this.storeRecent([aggregate, ...this.recent().filter(r => !same(r, aggregate))].slice(0, MAX_RECENT));
  }

  private storeRecent(aggregates: AggregateRef[]) {
    this.recent.set(aggregates);
    localStorage.setItem(RECENT_KEY, JSON.stringify(aggregates));
  }

  private loadRecent(): AggregateRef[] {
    try {
      const stored: unknown = JSON.parse(localStorage.getItem(RECENT_KEY) ?? '[]');
      // Entries stored without an application or a type are dropped: they don't say what they point at.
      return Array.isArray(stored) ? stored.filter(isAggregate) : [];
    } catch {
      return [];
    }
  }
}

function same(a: AggregateRef, b: AggregateRef): boolean {
  return a.app === b.app && a.type === b.type && a.id === b.id;
}

function isAggregate(value: unknown): value is AggregateRef {
  const aggregate = value as AggregateRef;
  return !!aggregate && typeof aggregate.app === 'string' && typeof aggregate.type === 'string' && typeof aggregate.id === 'string';
}
