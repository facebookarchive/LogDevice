{
  "logs": [
    {
      "name": "a",
      "id": 1,
      "replication_factor": 1,
      "extra_copies": 10,
      "synced_copies": 0,
      "max_writes_in_flight": 10001
    },
    {
      "name": "b",
      "id": 2,
      "replication_factor": 2,
      "extra_copies": 3,
      "synced_copies": 0,
      "max_writes_in_flight": 10,
      "single_writer": false
    },
    {
      "name": "c",
      "id": "3",
      "replication_factor": 3,
      "extra_copies": 3,
      "synced_copies": 0,
      "max_writes_in_flight": 10,
      "single_writer": true
    },
    {
      "name": "foo",
      "id": "8..10",
      "replication_factor": 3,
      "extra_copies": 2,
      "synced_copies": 0,
      "max_writes_in_flight": 10
    }
  ]
}
