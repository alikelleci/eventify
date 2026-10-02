import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { CommandEventsPage, CommandsPage, EventDetail, EventsPage, CommandMessage } from '../models';
import { BackendService } from './backend.service';

@Injectable({ providedIn: 'root' })
export class EventifyService {
  private readonly http = inject(HttpClient);
  private readonly backend = inject(BackendService);

  getEvents(aggregateType: string, aggregateId: string, cursor?: number | null, limit = 50): Observable<EventsPage> {
    let params = new HttpParams().set('limit', limit);
    if (cursor != null) params = params.set('cursor', cursor);
    return this.http.get<EventsPage>(`${this.aggregateUrl(aggregateType, aggregateId)}/events`, { params });
  }

  /** An event is found by its aggregate and its sequence in it. */
  getEventDetail(aggregateType: string, aggregateId: string, sequence: number): Observable<EventDetail> {
    return this.http.get<EventDetail>(`${this.aggregateUrl(aggregateType, aggregateId)}/events/${sequence}`);
  }

  /** The events that name this command as their cause. A command does not say which aggregate it is for. */
  getEventsOfCommand(aggregateType: string, command: CommandMessage): Observable<CommandEventsPage> {
    return this.http.get<CommandEventsPage>(
      `${this.aggregateUrl(aggregateType, command.aggregateId)}/commands/${encodeURIComponent(command.id)}/events`);
  }

  getCommands(aggregateType: string, aggregateId: string, limit = 500): Observable<CommandsPage> {
    const params = new HttpParams().set('limit', limit);
    return this.http.get<CommandsPage>(`${this.aggregateUrl(aggregateType, aggregateId)}/commands`, { params });
  }

  /** One application can hold several aggregates, so both parts address it. */
  private aggregateUrl(aggregateType: string, aggregateId: string): string {
    return `${this.backend.baseUrl()}/aggregates/${encodeURIComponent(aggregateType)}/${encodeURIComponent(aggregateId)}`;
  }

  retryCommand(command: CommandMessage): Observable<void> {
    return this.http.post<void>(`${this.backend.baseUrl()}/commands/retry`, command);
  }
}
