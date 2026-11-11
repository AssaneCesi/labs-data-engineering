# Indices du niveau 2

## TODO 3 : la clé
1. Quel ordre veux-tu préserver ? Entre quels messages ?
2. Kafka envoie dans la même partition tous les messages qui ont la même clé. Une clé est une suite d'octets.
3. `cle = m["siret"].encode()`.

## TODO 2 : la configuration du consommateur
1. Trois réglages : l'identifiant du groupe, le point de départ sans offset connu, la validation automatique.
2. Les clés s'appellent `group.id`, `auto.offset.reset`, `enable.auto.commit`.
3. `{"group.id": groupe, "auto.offset.reset": "earliest", "enable.auto.commit": False}`, en plus de `bootstrap.servers`.

## TODO 4 : valider l'offset
1. Valider = dire au broker « j'ai fini jusqu'ici ». Quel est le pire cas si on valide trop tôt ? Trop tard ?
2. `c.commit(message=msg, asynchronous=False)`.
3. Placer cet appel **après** `appliquer_message`. Si le programme plante entre les deux, le message sera relu : c'est ton `appliquer_message` idempotent (niveau 1) qui rend cela sans danger.
