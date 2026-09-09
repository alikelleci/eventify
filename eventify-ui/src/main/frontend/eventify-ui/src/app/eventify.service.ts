import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { AggregateState, EventsPage } from './models';

@Injectable({ providedIn: 'root' })
export class EventifyService {
  private readonly http = inject(HttpClient);

  getEvents(aggregateId: string, cursor?: string | null, limit = 50): Observable<EventsPage> {
    let params = new HttpParams().set('limit', limit);
    if (cursor) params = params.set('cursor', cursor);
    return this.http.get<EventsPage>(`/_eventify/${encodeURIComponent(aggregateId)}/events`, { params });
  }

  getState(aggregateId: string, at?: string): Observable<AggregateState> {
    let params = new HttpParams();
    if (at) params = params.set('at', at);
    return this.http.get<AggregateState>(`/_eventify/${encodeURIComponent(aggregateId)}/state`, { params });
  }
}
