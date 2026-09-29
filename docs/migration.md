# Migrating from 4.x

Eventify 5 stores events under a different key than Eventify 4, and stores more with each event. An application that
ran on 4.x needs its stores migrated once, with the `eventify-migration` tool, before it starts on 5.

| | Eventify 4 | Eventify 5 |
|---|---|---|
| Event key | `aggregateId@ULID` | aggregate type, id and sequence (`order␀order-1␀0000000000000000042`) |
| Event order | ULID, from the sender's clock | a sequence per aggregate: 1, 2, 3, … |
| Stored event | no aggregate type, no sequence | `aggregateType` and `sequence` added |
| Snapshot key | `aggregateId` | aggregate type and id |

The tool numbers each aggregate's events 1..n in the order Eventify 4 replayed them, and keeps everything else of an
event as it is, including its id and timestamp. It changes only the two changelog topics of the application's stores.
The command, event and result topics are left alone.

!!! warning "Back up first"
    After `migrate`, Eventify 4 can no longer run on the stores: it would find every aggregate empty. The way back is
    the `backup` you make just before, and `restore`.

## Before the day

**Port the application to Eventify 5** and test it; [Upgrading to 5.0](upgrading.md) lists what changes. Two things
must match the stored data:

- the `@AggregateRoot` name: you pass the same name to the tool as `--aggregate-type`;
- the event classes: a stored event names its class (`@class`). Keep the classes in their package, or add an
  [upcaster](advanced.md#event-upcasting) for a moved one.

The tool handles one aggregate type per application: Eventify 4 kept every aggregate in one store, and nothing in it
says which type an event belongs to.

**Build the tool** (Java 17 or later):

```bash
mvn -pl eventify-migration -am package -DskipTests
```

This gives `eventify-migration/target/eventify-migration.jar`.

**Kafka settings.** Pass the client settings of your cluster (security, SASL) in a properties file with `--config`. The
tool reads and writes the changelog topics, creates a backup topic next to each, describes the consumer group of the
application, and writes in transactions with the transactional id `eventify-migration-<application id>`. With ACLs, it
needs:

- Read, Write and DescribeConfigs on `<application id>-event-store-changelog` and
  `<application id>-snapshot-store-changelog`;
- Create, Read and Write on the same names followed by `-v4-backup`;
- Describe on the group `<application id>`;
- Write on the transactional id `eventify-migration-<application id>`.

**Run the check against production.** It only reads, without a consumer group, so the application keeps running:

```bash
java -jar eventify-migration.jar check --config client.properties --bootstrap-servers kafka:9092 --application-id my-app --aggregate-type order
```

Read the report:

- **Event classes.** Every class listed must be an event of the aggregate type you passed.
- **Aggregates that included another aggregate's events.** Eventify 4 read the events of `ada` as everything from
  `ada@` to `ada@~`, so `ada` also replayed the events of `ada@example.com` when they were in the same partition.
  After the migration each aggregate replays only its own events, so the state of the aggregates listed here changes.
  Check what that means for them before you go on.
- **Snapshots.** Eventify 4 snapshots cannot be migrated: their version counted differently. When they are only a
  cache, the tool deletes them with `--drop-snapshots`, and Eventify 5 rebuilds them from the events. Do not drop
  them if `@EnableSnapshotting(deleteEvents = true)` was ever used: then a snapshot is the only record of the events
  it replaced, and this migration is not for that store.
- **Conflicts.** The migration writes nothing while there is one. Solve them first.
- **Read in … s.** The migration itself reads the stores about four times as often, so plan for about four times this,
  plus the application's restart.

**Disk.** The backup is a second copy of both stores. And until the broker compacts them, the changelog topics hold
every event twice (under its old and its new key) plus a tombstone. Make sure the brokers have room for both.

## The migration

**1. Stop sending commands, and let the application finish.** Commands that are sent but not yet handled would be
handled by Eventify 5 later, by which time nobody waits for their result. Stop the traffic that sends commands, then
wait until the application's group has no lag:

```bash
kafka-consumer-groups.sh --bootstrap-server kafka:9092 --command-config client.properties --describe --group my-app
```

**2. Stop every instance** of the application. The tool refuses to write while the group has members.

**3. Back up** both stores:

```bash
java -jar eventify-migration.jar backup --config client.properties --bootstrap-servers kafka:9092 --application-id my-app
```

It copies each changelog to a new topic with the same partitions and settings,
`my-app-event-store-changelog-v4-backup` and `my-app-snapshot-store-changelog-v4-backup`, and compares the copy with
the original. It refuses when a backup topic already exists, and when the store is already (partly) migrated.

**4. Migrate:**

```bash
java -jar eventify-migration.jar migrate --config client.properties --bootstrap-servers kafka:9092 --application-id my-app --aggregate-type order
```

Add `--drop-snapshots` when the check told you to. `migrate` runs the check again, writes, and verifies. It ends with
exit code 0 and "The migration is complete." Anything else: see
[If something goes wrong](#if-something-goes-wrong).

**5. Delete the local state** of every instance: the directory `<state.dir>/<application id>`. Kafka Streams keeps it
in `state.dir`, by default `kafka-streams` under the system's temp directory. On a container without a persistent
volume there is nothing to delete. Eventify 5 then restores its stores from the changelog topics, which the tool just
verified.

**6. Start Eventify 5** with the same `application.id`: the changelog topics are named after it.

**7. Open the traffic** again.

When Eventify 5 runs well, delete the two backup topics.

## If something goes wrong

**The check reports conflicts.** Nothing was written. The report names each record.

**`migrate` stopped halfway** (a crash, a lost connection). Run `migrate` again. An event and the tombstone of its old key
are written in one transaction, so every event is under exactly one of its keys, and a second run migrates the rest.
It recognises the events it already wrote by their id, which is still their Eventify 4 key.

**Is it done?** `verify` reads both stores and ends with exit code 0 only when every event is under its new key and no
Eventify 4 snapshot is left:

```bash
java -jar eventify-migration.jar verify --config client.properties --bootstrap-servers kafka:9092 --application-id my-app --aggregate-type order
```

**Eventify 5 was started before the migration was complete.** It finds unmigrated aggregates empty, and starts them
again at sequence 1. The tool recognises the events Eventify 5 wrote and refuses to go on: stop the application and
solve it before anything else is written.

**Back to Eventify 4.** Stop every instance, then put the backup back:

```bash
java -jar eventify-migration.jar restore --config client.properties --bootstrap-servers kafka:9092 --application-id my-app
```

It writes every record of the backup into the changelog again and a tombstone for every other key, then compares the
changelog with the backup. Delete the local state (step 5) and start Eventify 4.

A restore only returns to the moment of the backup. It refuses when Eventify 5 already recorded events of its own,
because they would be lost: decide what should happen to them first.
