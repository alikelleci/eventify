import { Routes } from '@angular/router';
import { WebsiteLayoutComponent } from './layout/website-layout.component';
import { LandingComponent } from './landing/landing.component';

/** The public website: the product page, without the console's search or backend. */
export const routes: Routes = [
  {
    path: '',
    component: WebsiteLayoutComponent,
    children: [
      { path: '', component: LandingComponent },
    ],
  },
  { path: '**', redirectTo: '' },
];
