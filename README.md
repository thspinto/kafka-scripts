# Simple script to access an manage kafka

## Fetch messages

Gets messages from topic

Params:

|         Name            |       Description               |        Default         |
| ----------------------- | ------------------------------- | ---------------------- |
| --bootstrap_server      | Kafak bootstrap server          | localhost:9092         |
| --date                  | UTC date (2018-11-21-T11:00:00) | None                   |
| --max_messages          | max messages to read            | None                   |
| --use_json_deserializer | uses json as value deserializar | False                  |


Example:

```
python fetch_message_after_date.py --topic TestTopic --bootstrap_server localhost:19092 --max_messages 3 --date 2018-11-28T09:40:00Z
```