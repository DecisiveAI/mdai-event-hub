# Current changes
1. Connect to ValKey
2. Connect to RMQ via eventing pkg
3. Listen to channel for MdaiEvents
4. Receive MdaiEvents, find associated workflows, execute handlers
5. Fetch automation configs from however the `mdai-operator` conveys them
   6. Includes watch & refetch on changes
7. Retry logic for connecting to RabbitMQ
8. Mock for eventing pkg
9. Unit tests
10. Increment tag to v0.0.2


## TODOs:
1. ~~Extract handlers to their own lib~~
   2. Hard code set of handlers for updating vars "manually" -- Dmitry
   3. ~~{Stretch?} Determine method that allows users to configure which handler lib 
      to use~~
2. ~~Fetch automation configs from however the `mdai-operator` conveys them~~
   - ~~expose config reload api and implement the logic for reload~~
3. ~~Retry logic for connecting to RabbitMQ~~
4. Refactor/harden existing logic
5. Logs for debugging/audit
3. ~~Tests~~
3. Version?
4. ~~Create a mock for eventing so it can be used in dependent projects for unit 
   testing, for example event-handler-webserive~~