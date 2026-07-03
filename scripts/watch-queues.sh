#!/bin/bash
# Live-refreshing view of all RabbitMQ queues — message counts, consumers,
# and rates — without navigating the management UI. Run from anywhere:
#   ./scripts/watch-queues.sh

watch -n 2 -c '
curl -s -u guest:guest http://localhost:15672/api/queues/%2f | python3 -c "
import json, sys

queues = json.load(sys.stdin)
queues.sort(key=lambda q: q[\"name\"])

print(f\"{\"QUEUE\":30s} {\"READY\":>6s} {\"UNACKED\":>8s} {\"TOTAL\":>6s} {\"CONSUMERS\":>10s} {\"IN/s\":>8s} {\"OUT/s\":>8s}\")
print(\"-\" * 80)
for q in queues:
    name = q[\"name\"]
    ready = q.get(\"messages_ready\", 0)
    unacked = q.get(\"messages_unacknowledged\", 0)
    total = q.get(\"messages\", 0)
    consumers = q.get(\"consumers\", 0)
    in_rate = q.get(\"message_stats\", {}).get(\"publish_details\", {}).get(\"rate\", 0)
    out_rate = q.get(\"message_stats\", {}).get(\"deliver_get_details\", {}).get(\"rate\", 0)
    marker = \"  <-- DLQ\" if \"dlq\" in name else \"\"
    print(f\"{name:30s} {ready:>6d} {unacked:>8d} {total:>6d} {consumers:>10d} {in_rate:>8.1f} {out_rate:>8.1f}{marker}\")
"
'
