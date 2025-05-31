
Command -> 
    store pulsar -> (KafkaCommandGateway)
        consume command -> 
            store events dynamodb (event store) ->
                dynamodb stream to kinesis ->
                    pulsar source to topic ->
                        pulsar consume dynamodb event topic ->
                            publish results to .results topic
                            publish results to reply topic
                            publish events to event topics



- for ease of use, autotopic creation is advised on provided namespace
