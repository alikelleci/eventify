import { Component, inject, signal, computed, effect, viewChild, HostListener, DestroyRef } from '@angular/core';
import { takeUntilDestroyed, toObservable } from '@angular/core/rxjs-interop';
import { ActivatedRoute } from '@angular/router';
import { distinctUntilChanged, map, skip } from 'rxjs';
import { ButtonModule } from 'primeng/button';
import { DrawerModule } from 'primeng/drawer';
import { TabsModule } from 'primeng/tabs';
import { ToastModule } from 'primeng/toast';
import { MessageService } from 'primeng/api';

import { CommandMessage, EventMessage } from '@eventify/ui/models';
import { SearchService } from '../services/search.service';
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
  load = signal({ type: '', id: '' });
  /** A new object on every refresh, which recreates both lists so they load again. */
  lists = signal({});
  /** The tab that was open when refresh was clicked; the spinner follows its list, so switching tabs doesn't change it. */
  private refreshedTab = signal<Tab | null>(null);
  private eventList = viewChild(EventListComponent);
  private commandList = viewChild(CommandListComponent);
  tab = signal<Tab>('events');
  selected = signal<EventMessage | CommandMessage | null>(null);

  // Item counts reported by the lists once loaded; null while still loading.
  private eventCount = signal<number | null>(null);
  private commandCount = signal<number | null>(null);
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
  private openingMinElapsed = signal(false);
  private awaitingDetail = signal(false);

  drawerVisible = signal(false);
  isMobile = signal(window.innerWidth < 1024);
  selectedEvent = computed(() => this.isEvent(this.selected()) ? this.selected() as EventMessage : null);
  selectedCommand = computed(() => this.isCommand(this.selected()) ? this.selected() as CommandMessage : null);

  constructor() {
    // Angular reuses this component when navigating from one aggregate to another, so reload on every ID change.
    this.route.paramMap.pipe(
      map(p => ({ type: (p.get('type') ?? '').trim(), id: (p.get('id') ?? '').trim() })),
      distinctUntilChanged((a, b) => a.type === b.type && a.id === b.id),
      takeUntilDestroyed(this.destroyRef),
    ).subscribe(aggregate => this.reload(aggregate.type, aggregate.id));
    this.searchSvc.reload$.pipe(takeUntilDestroyed(this.destroyRef))
      .subscribe(aggregate => this.reload(aggregate.type, aggregate.id));
    // Picking another app in the header shows the same aggregate from that app.
    toObservable(inject(BackendService).activeApp).pipe(skip(1), takeUntilDestroyed(this.destroyRef))
      .subscribe(() => this.reload(this.load().type, this.load().id));

    effect(() => {
      if (this.opening() && this.openingMinElapsed() && this.eventCount() !== null && !this.awaitingDetail())
        this.opening.set(false);
    });
    effect(() => this.searchSvc.loading.set(this.opening()));
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

  onEventsLoaded(events: EventMessage[]) {
    this.eventCount.set(events.length);
    if (this.isEvent(this.selected())) this.reselect(events);
    this.autoSelect('events', events);
  }

  onCommandsLoaded(commands: CommandMessage[]) {
    this.commandCount.set(commands.length);
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
    this.refreshedTab.set(this.tab());
    this.lists.set({});
  }

  onDetailLoaded() {
    this.awaitingDetail.set(false);
  }

  /** Every search starts from scratch: new lists and details, nothing selected, back on the Events tab. */
  private reload(type: string, id: string) {
    const load = { type, id };
    this.load.set(load);
    this.tab.set('events');
    this.refreshedTab.set(null);
    this.selected.set(null);
    this.drawerVisible.set(false);
    this.eventCount.set(null);
    this.commandCount.set(null);
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
