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
    return this.http.get<EventsPage>(`/api/aggregates/${encodeURIComponent(aggregateId)}/events`, { params });
  }

  getState(aggregateId: string, eventId?: string): Observable<AggregateState> {
    let params = new HttpParams();
    if (eventId) params = params.set('eventId', eventId);
    return this.http.get<AggregateState>(`/api/aggregates/${encodeURIComponent(aggregateId)}/state`, { params });
  }
}
