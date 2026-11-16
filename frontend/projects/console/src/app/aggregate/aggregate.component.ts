import { Component, inject, signal, computed, effect, untracked, viewChild, HostListener, DestroyRef } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActivatedRoute } from '@angular/router';
import { distinctUntilChanged, map } from 'rxjs';
import { ButtonModule } from 'primeng/button';
import { DrawerModule } from 'primeng/drawer';
import { TabsModule } from 'primeng/tabs';
import { ToastModule } from 'primeng/toast';
import { MessageService } from 'primeng/api';

import { CommandMessage, EventMessage } from '@eventify/ui/models';
import { AggregateRef, SearchService } from '../services/search.service';
import { BackendService } from '@eventify/ui/services/backend.service';
import { EventListComponent } from './event-list/event-list.component';
import { CommandListComponent } from './command-list/command-list.component';
import { EventDetailComponent } from '@eventify/ui/components/event-detail/event-detail.component';
import { CommandDetailComponent } from '@eventify/ui/components/command-detail/command-detail.component';
import { AggregateSkeletonComponent } from '@eventify/ui/components/skeletons/aggregate-skeleton.component';
import { MIN_LOADING_MS } from '@eventify/ui/loading-timing';

type Tab = 'events' | 'commands';

/**
 * The aggregate page. Its lists and detail panes load their own data and show their own skeletons;
 * this component decides the tab, the selection and the full-page skeleton while the aggregate opens.
 */
@Component({
  selector: 'app-aggregate',
  templateUrl: './aggregate.component.html',
  standalone: true,
  imports: [
    ButtonModule, DrawerModule, TabsModule, ToastModule,
    EventListComponent, CommandListComponent, EventDetailComponent, CommandDetailComponent,
    AggregateSkeletonComponent,
  ],
  // Shared with the lists, so their load errors show in this page's toast.
  providers: [MessageService],
})
export class AggregateComponent {
  private readonly destroyRef = inject(DestroyRef);
  private readonly route = inject(ActivatedRoute);
  private readonly searchSvc = inject(SearchService);

  /**
   * The aggregate being shown. A new object on every search, which recreates the lists and
   * detail panes in the template, so each search starts from scratch.
   */
  load = signal<AggregateRef>({ app: '', type: '', id: '' });
  /** A new object on every refresh, which recreates both lists so they load again. */
  lists = signal({});
  /** The tab that was open when refresh was clicked; the spinner follows its list, so switching tabs doesn't change it. */
  private refreshedTab = signal<Tab | null>(null);
  private eventList = viewChild(EventListComponent);
  private commandList = viewChild(CommandListComponent);
  /** How far back the commands were read, for the not-found text; null when the application didn't say. */
  lookbackDays = computed(() => this.commandList()?.lookbackDays() ?? null);
  tab = signal<Tab>('events');
  selected = signal<EventMessage | CommandMessage | null>(null);

  // Item counts reported by the lists once loaded; null while still loading.
  private eventCount = signal<number | null>(null);
  private commandCount = signal<number | null>(null);
  /** A list couldn't be loaded: then nothing can be said about whether the aggregate exists. */
  private loadFailed = signal(false);
  /** Checking again from the not-found page: the page stays until the answer is in, with the button spinning. */
  checkingAgain = signal(false);
  /**
   * Nothing at this address: no events, and no commands either (a command that failed creates no events). Only once both
   * lists are in, so the page goes from the skeleton straight to the answer.
   */
  notFound = computed(() => {
    if (this.checkingAgain()) return true;
    return !this.loadFailed() && this.eventCount() === 0 && this.commandCount() === 0;
  });
  // Reads the list's own loading signal (the one that shows its skeleton), so the spinner stops as the list appears.
  refreshing = computed(() => {
    const tab = this.refreshedTab();
    return tab !== null && ((tab === 'events' ? this.eventList() : this.commandList())?.loading() ?? false);
  });
  activeListHasItems = computed(() => ((this.tab() === 'events' ? this.eventCount() : this.commandCount()) ?? 0) > 0);

  // Opening an aggregate shows the full-page skeleton (and the header loading bar) until the events
  // and the auto-selected newest event's detail are both ready, so everything appears at once.
  // Commands load in the background; they're slow (Kafka poll) and have their own list skeleton.
  opening = signal(false);
  openingMinElapsed = signal(false);
  private awaitingDetail = signal(false);

  drawerVisible = signal(false);
  isMobile = signal(window.innerWidth < 1024);
  selectedEvent = computed(() => this.isEvent(this.selected()) ? this.selected() as EventMessage : null);
  selectedCommand = computed(() => this.isCommand(this.selected()) ? this.selected() as CommandMessage : null);

  constructor() {
    // Angular reuses this component when navigating from one aggregate to another, so reload on every address change.
    // The address names the application, so a link or a recent search opens in the right one.
    const backend = inject(BackendService);
    this.route.paramMap.pipe(
      map(p => ({ app: (p.get('app') ?? '').trim(), type: (p.get('type') ?? '').trim(), id: (p.get('id') ?? '').trim() })),
      distinctUntilChanged((a, b) => a.app === b.app && a.type === b.type && a.id === b.id),
      takeUntilDestroyed(this.destroyRef),
    ).subscribe(aggregate => {
      backend.setActiveApp(aggregate.app);
      this.reload(aggregate);
    });
    this.searchSvc.reload$.pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(aggregate => this.reload(aggregate));

    effect(() => {
      // Without events the commands decide what shows (their tab, or that nothing is here), so wait for them too.
      const events = this.eventCount();
      if (this.opening() && this.openingMinElapsed() && events !== null && (events > 0 || this.commandCount() !== null) && !this.awaitingDetail())
        this.opening.set(false);
    });
    effect(() => this.searchSvc.loading.set(this.opening()));
    // An address with nothing at it doesn't stay among the recent ones (it may be there from when it had something).
    effect(() => {
      if (this.notFound() && !this.checkingAgain()) untracked(() => this.searchSvc.removeRecent(this.load()));
    });
    this.destroyRef.onDestroy(() => this.searchSvc.loading.set(false));
  }

  isEvent(item: EventMessage | CommandMessage | null): item is EventMessage {
    return item != null && 'revision' in item;
  }

  isCommand(item: EventMessage | CommandMessage | null): item is CommandMessage {
    return item != null && !('revision' in item);
  }

  @HostListener('window:resize')
  onResize() { this.isMobile.set(window.innerWidth < 1024); }

  // Arrow keys are handled by the list of the open tab.
  @HostListener('window:keydown', ['$event'])
  onKeydown(e: KeyboardEvent) {
    const tag = (e.target as HTMLElement).tagName;
    if (e.key === 'Escape' && tag !== 'INPUT' && tag !== 'TEXTAREA') {
      this.selected.set(null);
      this.drawerVisible.set(false);
    }
  }

  selectItem(item: EventMessage | CommandMessage) {
    this.selected.set(item);
    if (this.isMobile()) this.drawerVisible.set(true);
  }

  onEventsLoaded(loaded: EventMessage[] | null) {
    const events = loaded ?? [];
    if (!loaded) this.loadFailed.set(true);
    if (events.length) this.searchSvc.remember(this.load());
    this.eventCount.set(events.length);
    if (this.commandCount() !== null) this.checkingAgain.set(false);
    if (this.isEvent(this.selected())) this.reselect(events);
    this.autoSelect('events', events);
  }

  onCommandsLoaded(loaded: CommandMessage[] | null) {
    const commands = loaded ?? [];
    if (!loaded) this.loadFailed.set(true);
    if (commands.length) this.searchSvc.remember(this.load());
    this.commandCount.set(commands.length);
    if (this.eventCount() !== null) this.checkingAgain.set(false);
    // Only commands, all failed: show those instead of an empty event list.
    if (this.eventCount() === 0 && commands.length) this.tab.set('commands');
    if (this.isCommand(this.selected())) this.reselect(commands);
    this.autoSelect('commands', commands);
  }

  /**
   * Loads both lists again, keeping the tab and the selected item. Like opening an aggregate, the other tab loads in the background.
   * Recreating the lists cancels any request still in flight, so an older response can't overwrite the new one.
   */
  refresh() {
    this.eventCount.set(null);
    this.commandCount.set(null);
    this.loadFailed.set(false);
    this.refreshedTab.set(this.tab());
    this.lists.set({});
  }

  /** From the not-found page: look again, staying on that page until the answer is in. */
  checkAgain() {
    this.checkingAgain.set(true);
    this.refresh();
  }

  /** From the not-found page: back to the ID in the address. */
  changeId() {
    this.searchSvc.focusSearch();
  }

  onDetailLoaded() {
    this.awaitingDetail.set(false);
  }

  /** Every search starts from scratch: new lists and details, nothing selected, back on the Events tab. */
  private reload(aggregate: AggregateRef) {
    const load = { ...aggregate };
    this.load.set(load);
    this.tab.set('events');
    this.refreshedTab.set(null);
    this.selected.set(null);
    this.drawerVisible.set(false);
    this.eventCount.set(null);
    this.commandCount.set(null);
    this.loadFailed.set(false);
    this.checkingAgain.set(false);
    this.awaitingDetail.set(false);
    this.openingMinElapsed.set(false);
    this.opening.set(true);
    setTimeout(() => { if (this.load() === load) this.openingMinElapsed.set(true); }, MIN_LOADING_MS);
  }

  /**
   * After a refresh the list holds new objects: select the new copy of the selected item, so it stays highlighted
   * and its detail shows the latest fields (the detail only reloads when the id changes). Gone: select nothing.
   */
  private reselect(items: (EventMessage | CommandMessage)[]) {
    const id = this.selected()!.id;
    this.selected.set(items.find(item => item.id === id) ?? null);
    if (!this.selected()) this.drawerVisible.set(false);
  }

  /** On desktop, open the newest item of the active tab once it loads, so the detail pane isn't empty. */
  private autoSelect(tab: Tab, items: (EventMessage | CommandMessage)[]) {
    if (!this.isMobile() && this.tab() === tab && !this.selected() && items.length > 0) {
      this.selected.set(items[0]);
      this.awaitingDetail.set(true);
    }
  }
}
