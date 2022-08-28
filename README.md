# max-task-idle-ms
Understanding max.task.idle.ms

To run kafka locally:
```shell
docker compose up
```

To run the streams app:
```shell
./gradlew run -PmainClass=com.mokd.MaxTaskIdleStreamsApp
```

To run the message producer
```
./gradlew run -PmainClass=com.mokd.MessageProducer
```
