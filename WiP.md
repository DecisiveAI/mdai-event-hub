# Current changes
1. Connect to ValKey
2. Connect to RMQ via eventing pkg
3. Listen to channel for MdaiEvents
4. Receive MdaiEvents, find associated workflows, execute handlers


## TODOs:
1. Extract handlers to their own lib
   2. Hard code set of handlers for updating vars "manually"
   3. {Stretch?} Determine method that allows users to configure which handler lib to use
2. Fetch automation configs from however the `mdai-operator` conveys them
3. Retry logic for connecting to RabbitMQ
4. Refactor/harden existing logic
5. Logs for debugging/audit
3. Tests
3. Version?