import { Routes } from '@angular/router';
import { WebsiteLayoutComponent } from './layout/website-layout.component';

/** The public website: the framework's home page and the Eventify Console page, with a shared header. */
export const routes: Routes = [
  {
    path: '',
    component: WebsiteLayoutComponent,
    children: [
      { path: '', loadComponent: () => import('./home/home.component').then(m => m.HomeComponent), title: 'Eventify | Event sourcing for Java' },
      { path: 'console', loadComponent: () => import('./console/console.component').then(m => m.ConsoleComponent), title: 'Eventify Console | Inspect aggregate history' },
    ],
  },
  { path: '**', redirectTo: '' },
];
