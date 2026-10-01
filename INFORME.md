## Sum

Las réplicas consumen de la cola de entrada y acumulan por cliente la suma parcial de cada fruta y la cantidad de registros procesados. El EOF que genera un cliente tiene incluido el total de registros enviados. Entonces, la réplica que lo recibe lo broadcastea por un exchange de control a todas las réplicas. Al recibir un EOF por el exchange de control, la réplica envía un mensaje con la cantidad de mensajes leídos hasta ese momento. Si después sigue recibiendo y procesando mensajes de ese cliente, vuelve a enviar su conteo actualizado por cada uno. Si la suma de los mensajes recibidos hasta el EOF es igual a la cantidad de mensajes enviados, se hace el flush.

Como el contador de cada replica solo se incrementa cuando un mensaje ya fue procesado, la suma de los contadores nunca puede llegar al total real antes de que todos los mensajes hayan sido procesados por alguna replica. Asi se evita hacer flush antes de tiempo, con datos de un cliente que todavia estan en vuelo.

Hay dos hilos: uno para consumir la cola de entrada, y otro para el exchange de control. Se utiliza un lock para sincronizar los hilos y proteger el estado compartido. 

Al hacer el flush, cada fruta se envia a un aggregator, que se elije con un hash deterministico por el nombre de fruta. Asi cada fruta se acumula en un solo lugar. Por ultimo, se envia un EOF del cliente a los Aggregators a traves del output exchange.


## Aggregation

Los aggregators reciben las sumas parciales de las frutas que le corresponden por el hash y las acumula por cliente. Cuenta los EOF que recibe por cliente, y cuando recibio uno de cada replica de Sum, envia los resultados al join.

## Join

El join recibe un top parcial de cada aggregator por cliente. Como las frutas viven cada una en un aggregator, lo que hace es quedarse con las mayores (top_size) y lo envia al cliente a traves del Gateway.

## Escalabilidad

- Clientes:
El estado esta separado por client_id, asi que pueden funcionan concurrentemente. Al terminar, se elimina el estado del cliente

- Grandes Volumenes de Datos:
Sum envia por fruta, no por registro entero ni por cliente. Entonces el trafico hacia el aggregator depende de la cantidad de frutas distintas, no de la cantidad de registros, asi que crece mucho menos que el volumen de datos de entrada.

- Cantidad de Controles: 

Agregar replicas de Sum o de Aggregation no requiere cambios en el codigo, solo en la configuracion (SUM_AMOUNT, AGGREGATION_AMOUNT). Cada fruta se envia siempre al mismo aggregator por hash, y cada cliente queda repartido entre las replicas de Sum que lo procesan, asi que el trabajo se reparte automaticamente al escalar la cantidad de controles. El limite esta en que un cliente grande no se paraleliza mas alla de la cantidad de replicas de Sum que reciben sus datos, y una fruta muy pesada siempre cae en el mismo aggregator.

