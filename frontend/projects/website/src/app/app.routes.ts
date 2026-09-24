import { Routes } from '@angular/router';
import { WebsiteLayoutComponent } from './layout/website-layout.component';
import { HomeComponent } from './home/home.component';
import { ConsoleComponent } from './console/console.component';

/** The public website: the framework's home page and the Eventify Console page, with a shared header. */
export const routes: Routes = [
  {
    path: '',
    component: WebsiteLayoutComponent,
    children: [
      { path: '', component: HomeComponent, title: 'Eventify | Event sourcing for Java' },
      { path: 'console', component: ConsoleComponent, title: 'Eventify Console | Inspect aggregate history' },
    ],
  },
  { path: '**', redirectTo: '' },
];
