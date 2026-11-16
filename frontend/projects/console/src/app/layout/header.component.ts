import { Component, computed, inject } from '@angular/core';
import { RouterLink } from '@angular/router';
import { BackendService } from '@eventify/ui/services/backend.service';
import { SearchService } from '../services/search.service';
import { SessionService } from '../services/session.service';
import { AppSwitcherComponent } from './app-switcher.component';
import { HeaderSearchComponent } from './header-search.component';

@Component({
  selector: 'app-header',
  standalone: true,
  imports: [RouterLink, AppSwitcherComponent, HeaderSearchComponent],
  host: { class: 'relative block' },
  template: `
    <header class="relative flex items-center gap-4 px-5 h-14 border-b border-slate-800 bg-slate-950">
      <a routerLink="/" class="flex items-center gap-3 rounded outline-none focus-visible:ring-2 focus-visible:ring-primary-400">
        <i class="pi pi-bolt inline-block w-5 shrink-0 text-center text-xl text-primary-400"></i>
        <span class="text-lg tracking-tight"><span class="font-semibold text-white">Eventify</span><span class="font-light text-slate-300 ml-1.5">Console</span></span>
      </a>

      <!-- The open aggregate's address: application / aggregate type / ID. Each part can be changed in place. The start page
           has its own large one, so the header carries none there. -->
      @if (backend.apps().length > 0 && !onStartPage()) {
        <div class="relative hidden sm:flex items-stretch h-9 flex-1 max-w-2xl mx-auto rounded-lg border border-slate-800 bg-slate-900 font-mono text-[13px] transition-colors focus-within:border-slate-600 hover:border-slate-700">
          <app-app-switcher look="dark" class="shrink-0 max-w-48 [&>button]:rounded-l-[7px]" />
          <span class="flex items-center select-none text-slate-600" aria-hidden="true">/</span>
          <app-header-search />
        </div>
        <app-app-switcher class="sm:hidden ml-auto w-40 shrink-0" />
      }

      <!-- With login: who is logged in, and log out -->
      @if (session.session().loginEnabled) {
        <button type="button" (click)="session.logout()" title="Log out"
                class="flex shrink-0 items-center gap-2 h-8 px-2.5 rounded text-sm text-slate-400 hover:text-white hover:bg-slate-800 transition-colors cursor-pointer outline-none focus-visible:ring-2 focus-visible:ring-primary-400"
                [class.ml-auto]="backend.apps().length === 0 || onStartPage()">
          <span class="hidden md:inline max-w-40 truncate">{{ session.session().user }}</span>
          <i class="pi pi-sign-out text-xs"></i>
        </button>
      }
    </header>

    <!-- On mobile the type and ID get their own white bar below the header -->
    @if (backend.apps().length > 0 && !onStartPage()) {
      <div class="sm:hidden px-4 py-3 border-b border-surface-200 dark:border-surface-800">
        <div class="relative flex items-stretch h-11 rounded-lg border border-surface-200 bg-surface-50 font-mono text-sm focus-within:border-primary-500 dark:border-surface-700 dark:bg-surface-800">
          <app-header-search variant="light" />
        </div>
      </div>
    }

    @if (search.loading()) {
      <div class="absolute inset-x-0 -bottom-px h-0.5 overflow-hidden">
        <div class="header-progress h-full w-1/3 bg-primary-400"></div>
      </div>
    }
  `,
})
export class HeaderComponent {
  readonly backend = inject(BackendService);
  readonly search = inject(SearchService);
  readonly session = inject(SessionService);
  /** The start page opens aggregates with its own large address. */
  readonly onStartPage = computed(() => !this.search.current());
}
