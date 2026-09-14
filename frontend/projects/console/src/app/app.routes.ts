import { Routes } from '@angular/router';
import { ConsoleLayoutComponent } from './layout/console-layout.component';
import { HomeComponent } from './home/home.component';

/** The console: the header with search and app switcher around its pages. */
export const routes: Routes = [
  {
    path: '',
    component: ConsoleLayoutComponent,
    children: [
      { path: '', component: HomeComponent },
      // A separate chunk, so the home page doesn't wait for the aggregate views (and jsondiffpatch).
      { path: 'aggregates/:id', loadComponent: () => import('./aggregate/aggregate.component').then(m => m.AggregateComponent) },
    ],
  },
  { path: '**', redirectTo: '' },
];
